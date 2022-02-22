package ai.zipline.online

import ai.zipline.api.Row
import ai.zipline.api._
import org.apache.avro.Schema
import org.apache.avro.Schema.Field
import org.apache.avro.generic.{GenericData, GenericRecord}
import org.apache.avro.util.Utf8

import java.nio.ByteBuffer
import java.util
import scala.collection.AbstractIterator
import scala.collection.JavaConverters._

object AvroConversions {
  def toZiplineSchema(schema: Schema): DataType = {
    schema.getType match {
      case Schema.Type.RECORD =>
        StructType(schema.getName,
                   schema.getFields.asScala.toArray.map { field =>
                     StructField(field.name(), toZiplineSchema(field.schema()))
                   })
      case Schema.Type.ARRAY   => ListType(toZiplineSchema(schema.getElementType))
      case Schema.Type.MAP     => MapType(StringType, toZiplineSchema(schema.getValueType))
      case Schema.Type.STRING  => StringType
      case Schema.Type.INT     => IntType
      case Schema.Type.LONG    => LongType
      case Schema.Type.FLOAT   => FloatType
      case Schema.Type.DOUBLE  => DoubleType
      case Schema.Type.BYTES   => BinaryType
      case Schema.Type.BOOLEAN => BooleanType
      case Schema.Type.UNION   => toZiplineSchema(schema.getTypes.get(1)) // unions are only used to represent nullability
      case _                   => throw new UnsupportedOperationException(s"Cannot convert avro type ${schema.getType.toString}")
    }
  }

  def fromZiplineSchema(dataType: DataType): Schema = {
    dataType match {
      case StructType(name, fields) =>
        assert(name != null)
        Schema.createRecord(
          name.replaceAll("[^0-9a-zA-Z_]", "_"),
          "", // doc
          "ai.zipline.data", // namespace
          false, // isError
          fields
            .map { ziplineField =>
              val defaultValue: AnyRef = null
              new Field(ziplineField.name,
                        Schema.createUnion(Schema.create(Schema.Type.NULL), fromZiplineSchema(ziplineField.fieldType)),
                        "",
                        defaultValue)
            }
            .toList
            .asJava
        )
      case ListType(elementType) => Schema.createArray(fromZiplineSchema(elementType))
      case MapType(keyType, valueType) => {
        assert(keyType == StringType, s"Avro only supports string keys for a map")
        Schema.createMap(fromZiplineSchema(valueType))
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
          s"Cannot convert zipline type $dataType to avro type. Cast it to string please")
    }
  }

  def fromZiplineRow(value: Any, dataType: DataType): Any = {
    // But this also has to happen at the recursive depth - data type and schema inside the compositor need to
    Row.to[GenericRecord, ByteBuffer, util.ArrayList[Any], util.Map[Any, Any]](
      value,
      dataType,
      { (data: Iterator[Any], elemDataType: DataType) =>
        val schema = AvroConversions.fromZiplineSchema(elemDataType)
        val record = new GenericData.Record(schema)
        data.zipWithIndex.foreach { case (value, idx) => record.put(idx, value) }
        record
      },
      ByteBuffer.wrap,
      { (elems: Iterator[Any], size: Int) =>
        val result = new util.ArrayList[Any](size)
        elems.foreach(result.add)
        result
      },
      { m: util.Map[Any, Any] => m }
    )
  }

  def toZiplineRow(value: Any, dataType: DataType): Any = {
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
}
