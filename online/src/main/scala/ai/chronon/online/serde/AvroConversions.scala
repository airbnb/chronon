/*
 *    Copyright (C) 2023 The Chronon Authors.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package ai.chronon.online.serde

import ai.chronon.api._
import org.apache.avro.Schema
import org.apache.avro.Schema.Field
import org.apache.avro.generic.{GenericData, GenericRecord}
import org.apache.avro.util.Utf8

import java.nio.ByteBuffer
import java.util
import scala.collection.{AbstractIterator, mutable}
import scala.util.ScalaJavaConversions.{JListOps, ListOps}
import scala.util.Try

object AvroConversions {

  def toAvroValue(value: AnyRef, schema: Schema): Object =
    schema.getType match {
      case Schema.Type.UNION  => toAvroValue(value, schema.getTypes.get(1))
      case Schema.Type.LONG   => value.asInstanceOf[Long].asInstanceOf[Object]
      case Schema.Type.INT    => value.asInstanceOf[Int].asInstanceOf[Object]
      case Schema.Type.FLOAT  => value.asInstanceOf[Float].asInstanceOf[Object]
      case Schema.Type.DOUBLE => value.asInstanceOf[Double].asInstanceOf[Object]
      case _                  => value
    }

  def cleanSuffix(name: String): String = {
    if (name.contains(RepetitionSuffix)) {
      name.substring(0, name.indexOf(RepetitionSuffix))
    } else {
      name
    }
  }

  def toChrononSchema(schema: Schema, cleanName: Boolean = false): DataType = {
    def clean(name: String): String = if (cleanName) cleanSuffix(name) else name
    schema.getType match {
      case Schema.Type.RECORD =>
        StructType(clean(schema.getName),
                   schema.getFields.toScala.toArray.map { field =>
                     StructField(clean(field.name()), toChrononSchema(field.schema(), cleanName))
                   })
      case Schema.Type.ARRAY   => ListType(toChrononSchema(schema.getElementType, cleanName))
      case Schema.Type.MAP     => MapType(StringType, toChrononSchema(schema.getValueType, cleanName))
      case Schema.Type.STRING  => StringType
      case Schema.Type.INT     => IntType
      case Schema.Type.LONG    => LongType
      case Schema.Type.FLOAT   => FloatType
      case Schema.Type.DOUBLE  => DoubleType
      case Schema.Type.BYTES   => BinaryType
      case Schema.Type.BOOLEAN => BooleanType
      case Schema.Type.UNION =>
        toChrononSchema(schema.getTypes.get(1), cleanName) // unions are only used to represent nullability
      case _ => throw new UnsupportedOperationException(s"Cannot convert avro type ${schema.getType.toString}")
    }
  }

  val RepetitionSuffix = "_REPEATED_NAME_"
  def fromChrononSchema(dataType: DataType, nameSet: mutable.Set[String] = new mutable.HashSet[String]): Schema = {
    def addName(name: String): String = {
      val cleanName = name.replaceAll("[^0-9a-zA-Z_]", "_")
      val eligibleName = if (!nameSet.contains(cleanName)) {
        cleanName
      } else {
        var i = 0
        while (nameSet.contains(cleanName + RepetitionSuffix + i.toString)) { i += 1 }
        cleanName + RepetitionSuffix + i.toString
      }
      nameSet.add(eligibleName)
      eligibleName
    }
    dataType match {
      case StructType(name, fields) =>
        assert(name != null)
        Schema.createRecord(
          addName(name),
          "", // doc
          "ai.chronon.data", // namespace
          false, // isError
          fields
            .map { chrononField =>
              val defaultValue: AnyRef = null
              new Field(
                addName(chrononField.name),
                Schema.createUnion(Schema.create(Schema.Type.NULL), fromChrononSchema(chrononField.fieldType, nameSet)),
                "",
                defaultValue)
            }
            .toList
            .toJava
        )
      case ListType(elementType) => Schema.createArray(fromChrononSchema(elementType, nameSet))
      case MapType(keyType, valueType) => {
        assert(keyType == StringType, s"Avro only supports string keys for a map")
        Schema.createMap(fromChrononSchema(valueType, nameSet))
      }
      case StringType  => Schema.create(Schema.Type.STRING)
      case IntType     => Schema.create(Schema.Type.INT)
      case LongType    => Schema.create(Schema.Type.LONG)
      case FloatType   => Schema.create(Schema.Type.FLOAT)
      case DoubleType  => Schema.create(Schema.Type.DOUBLE)
      case BinaryType  => Schema.create(Schema.Type.BYTES)
      case BooleanType => Schema.create(Schema.Type.BOOLEAN)
      case _ =>
        throw new UnsupportedOperationException(
          s"Cannot convert chronon type $dataType to avro type. Cast it to string please")
    }
  }

  def fromChrononRow(value: Any,
                     dataType: DataType,
                     topLevelSchema: Schema,
                     extraneousRecord: Any => Array[Any] = null): Any = {
    // But this also has to happen at the recursive depth - data type and schema inside the compositor need to
    Row.to[GenericRecord, ByteBuffer, util.ArrayList[Any], util.Map[Any, Any], Schema](
      value,
      dataType,
      { (data: Iterator[Any], elemDataType: DataType, providedSchema: Option[Schema]) =>
        val schema = providedSchema.getOrElse(AvroConversions.fromChrononSchema(elemDataType))
        val record = new GenericData.Record(schema)
        data.zipWithIndex.foreach {
          case (value1, idx) => record.put(idx, value1)
        }
        record
      },
      ByteBuffer.wrap,
      { (elems: Iterator[Any], size: Int) =>
        val result = new util.ArrayList[Any](size)
        elems.foreach(result.add)
        result
      },
      { m: util.Map[Any, Any] => m },
      extraneousRecord,
      Some(AvroSchemaTraverser(topLevelSchema))
    )
  }

  def toChrononRow(value: Any, dataType: DataType): Any = {
    Row.from[GenericRecord, ByteBuffer, GenericData.Array[Any], Utf8](
      value,
      dataType,
      { (record: GenericRecord, fields: Seq[StructField]) =>
        new AbstractIterator[Any]() {
          var idx = 0
          override def next(): Any = {
            val res = record.get(idx)
            idx += 1
            res
          }
          override def hasNext: Boolean = idx < fields.size
        }
      },
      { (byteBuffer: ByteBuffer) => byteBuffer.array() },
      { (garr: GenericData.Array[Any]) =>
        val arr = new util.ArrayList[Any](garr.size)
        val it = garr.iterator()
        while (it.hasNext) {
          arr.add(it.next())
        }
        arr
      },
      { (avString: Utf8) => avString.toString }
    )
  }

  def encodeBytes(schema: StructType, extraneousRecord: Any => Array[Any] = null): Any => Array[Byte] = {
    val codec: AvroCodec = new AvroCodec(fromChrononSchema(schema).toString(true));
    { data: Any =>
      val record =
        fromChrononRow(data, codec.chrononSchema, codec.schema, extraneousRecord).asInstanceOf[GenericData.Record]
      val bytes = codec.encodeBinary(record)
      bytes
    }
  }

  def encodeJson(schema: StructType, extraneousRecord: Any => Array[Any] = null): Any => String = {
    val codec: AvroCodec = new AvroCodec(fromChrononSchema(schema).toString(true));
    { data: Any =>
      val record =
        fromChrononRow(data, codec.chrononSchema, codec.schema, extraneousRecord).asInstanceOf[GenericData.Record]
      val json = codec.encodeJson(record)
      json
    }
  }
}

case class AvroSchemaTraverser(currentNode: Schema) extends SchemaTraverser[Schema] {

  private lazy val cleanSchema = currentNode.getFields.toScala.map { f =>
    AvroConversions.cleanSuffix(f.name()) -> f.schema()
  }.toMap

  // We only use union types for nullable fields, and always
  // unbox them when writing the actual schema out.
  private def unboxUnion(maybeUnion: Schema): Schema =
    if (maybeUnion.getType == Schema.Type.UNION) {
      maybeUnion.getTypes.get(1)
    } else {
      maybeUnion
    }

  override def getField(field: StructField): SchemaTraverser[Schema] = {
    copy(
      unboxUnion(
        Try(currentNode.getField(field.name).schema()).getOrElse(cleanSchema(AvroConversions.cleanSuffix(field.name))))
    )
  }

  override def getCollectionType: SchemaTraverser[Schema] =
    copy(
      unboxUnion(currentNode.getElementType)
    )

  // Avro map keys are always strings.
  override def getMapKeyType: SchemaTraverser[Schema] =
    if (currentNode.getType == Schema.Type.MAP) {
      copy(
        Schema.create(Schema.Type.STRING)
      )
    } else {
      throw new UnsupportedOperationException(
        s"Current node ${currentNode.getName} is a ${currentNode.getType}, not a ${Schema.Type.MAP}"
      )
    }

  override def getMapValueType: SchemaTraverser[Schema] =
    copy(
      unboxUnion(currentNode.getValueType)
    )
}
