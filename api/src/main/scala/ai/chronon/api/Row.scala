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

package ai.chronon.api

import java.util
import scala.collection.mutable
import scala.util.ScalaJavaConversions.IteratorOps

trait Row {
  def get(index: Int): Any

  def ts: Long

  val length: Int

  def isBefore: Boolean

  def mutationTs: Long

  def getAs[T](index: Int): T = get(index).asInstanceOf[T]

  def values: Array[Any] = (0 until length).map(get).toArray

  override def toString: String = {
    s"""[${values.mkString(", ")}]"""
  }
}

/**
  * SchemaTraverser aids in the traversal of the given SchemaType.
  * In some cases (eg avro), it is more performant to create the
  * top-level schema once and then traverse it top-to-bottom, rather
  * than recreating at each node.
  *
  * This helper trait allows the Row.to function to traverse SchemaType
  * without leaking details of the SchemaType structure.
  */
trait SchemaTraverser[SchemaType] {

  def currentNode: SchemaType

  // Returns the equivalent SchemaType representation of the given field
  def getField(field: StructField): SchemaTraverser[SchemaType]

  // Returns the inner type of the current collection field type.
  // Throws if the current type is not a collection.
  def getCollectionType: SchemaTraverser[SchemaType]

  // Returns the key type of the current map field type.
  // Throws if the current type is not a map.
  def getMapKeyType: SchemaTraverser[SchemaType]

  // Returns the valye type of the current map field type.
  // Throws if the current type is not a map.
  def getMapValueType: SchemaTraverser[SchemaType]

}

object Row {

  /**
    * TODO migrate in the rest of the codebase
    * This is done in the Netflix fork but wanted to break out the PR
    *
    * Build a reusable converter function that transforms values according to their DataType.
    * Unlike `Row.to` which recreates the conversion logic on every call, this method builds
    * the converter once and returns a function that can be applied many times. This allows
    * the JIT compiler to optimize the hot path.
    *
    * @param dataType The Chronon DataType to convert
    * @param composer Function to compose struct fields into target StructType
    * @param binarizer Function to convert byte arrays to target BinaryType
    * @param collector Function to collect array elements into target ListType
    * @param mapper Function to convert maps to target MapType
    * @param extraneousRecord Optional handler for non-standard record types
    * @param schemaTraverser Optional traverser for output schema (used by Avro)
    * @return A converter function that transforms values according to the DataType
    */
  def buildToConverter[StructType, BinaryType, ListType, MapType, OutputSchema](
      dataType: DataType,
      composer: (Array[Any], DataType, Option[OutputSchema]) => StructType,
      binarizer: Array[Byte] => BinaryType,
      collector: (Array[Any], Int) => ListType,
      mapper: util.Map[Any, Any] => MapType,
      extraneousRecord: Any => Array[Any] = null,
      schemaTraverser: Option[SchemaTraverser[OutputSchema]] = None): Any => Any = {

    val unguardedFunc: Any => Any = dataType match {
      case StructType(_, fields) =>
        val fieldConverters = fields.map { field =>
          buildToConverter(
            field.fieldType,
            composer,
            binarizer,
            collector,
            mapper,
            extraneousRecord,
            schemaTraverser.map(_.getField(field))
          )
        }
        val traverser = schemaTraverser

        (value: Any) =>
          value match {
            case arr: Array[Any] =>
              val result = new Array[Any](arr.length)
              var i = 0
              while (i < arr.length) {
                result(i) = fieldConverters(i)(arr(i))
                i += 1
              }
              composer(result, dataType, traverser.map(_.currentNode))
            case list: util.ArrayList[Any] =>
              val result = new Array[Any](list.size())
              var i = 0
              while (i < list.size()) {
                result(i) = fieldConverters(i)(list.get(i))
                i += 1
              }
              composer(result, dataType, traverser.map(_.currentNode))
            case other: Any =>
              assert(extraneousRecord != null, s"No handler for $other of class ${other.getClass}")
              val arr = extraneousRecord(other)
              val result = new Array[Any](arr.length)
              var i = 0
              while (i < arr.length) {
                result(i) = fieldConverters(i)(arr(i))
                i += 1
              }
              composer(result, dataType, traverser.map(_.currentNode))
          }

      case ListType(elemType) =>
        val elemConverter = buildToConverter(
          elemType,
          composer,
          binarizer,
          collector,
          mapper,
          extraneousRecord,
          schemaTraverser.map(_.getCollectionType)
        )

        (value: Any) =>
          value match {
            case list: util.ArrayList[Any] =>
              val result = new Array[Any](list.size())
              val it = list.iterator()
              var i = 0
              while (it.hasNext) {
                result(i) = elemConverter(it.next())
                i += 1
              }
              collector(result, list.size())
            case arr: Array[_] =>
              val result = new Array[Any](arr.length)
              var i = 0
              while (i < arr.length) {
                result(i) = elemConverter(arr(i))
                i += 1
              }
              collector(result, arr.length)
            case arr: mutable.WrappedArray[Any] =>
              val result = new Array[Any](arr.length)
              var i = 0
              while (i < arr.length) {
                result(i) = elemConverter(arr(i))
                i += 1
              }
              collector(result, arr.length)
          }

      case MapType(keyType, valueType) =>
        val keyConverter = buildToConverter(
          keyType,
          composer,
          binarizer,
          collector,
          mapper,
          extraneousRecord,
          schemaTraverser.map(_.getMapKeyType)
        )
        val valConverter = buildToConverter(
          valueType,
          composer,
          binarizer,
          collector,
          mapper,
          extraneousRecord,
          schemaTraverser.map(_.getMapValueType)
        )

        (value: Any) =>
          value match {
            case map: util.Map[Any, Any] =>
              val newMap = new util.HashMap[Any, Any](map.size())
              val iter = map.entrySet().iterator()
              while (iter.hasNext) {
                val entry = iter.next()
                newMap.put(keyConverter(entry.getKey), valConverter(entry.getValue))
              }
              mapper(newMap)
            case map: collection.Map[Any, Any] =>
              val newMap = new util.HashMap[Any, Any](map.size)
              map.foreach { entry =>
                newMap.put(keyConverter(entry._1), valConverter(entry._2))
              }
              mapper(newMap)
          }

      case BinaryType =>
        (value: Any) => binarizer(value.asInstanceOf[Array[Byte]])

      case IntType =>
        (value: Any) => value.asInstanceOf[Number].intValue()

      case LongType =>
        (value: Any) => value.asInstanceOf[Number].longValue()

      case DoubleType =>
        (value: Any) => value.asInstanceOf[Number].doubleValue()

      case FloatType =>
        (value: Any) => value.asInstanceOf[Number].floatValue()

      case ShortType =>
        (value: Any) => value.asInstanceOf[Number].shortValue()

      case ByteType =>
        (value: Any) => value.asInstanceOf[Number].byteValue()

      case BooleanType =>
        (value: Any) => value.asInstanceOf[Boolean]

      case StringType =>
        (value: Any) => value.toString

      case _ =>
        (value: Any) => value
    }

    // Guard function handles nulls
    (value: Any) => if (value == null) null else unguardedFunc(value)
  }

  // recursively traverse a logical struct, and convert it chronon's row type
  def from[CompositeType, BinaryType, ArrayType, StringType](
      value: Any,
      dataType: DataType,
      decomposer: (CompositeType, Seq[StructField]) => Iterator[Any],
      debinarizer: BinaryType => Array[Byte],
      delister: ArrayType => util.ArrayList[Any],
      deStringer: StringType => String): Any = {
    if (value == null) return null
    def edit(value: Any, dataType: DataType): Any =
      from(value, dataType, decomposer, debinarizer, delister, deStringer)
    dataType match {
      case StructType(_, fields) =>
        value match {
          case record: CompositeType =>
            val iter = decomposer(record, fields)
            val newArr = new Array[Any](fields.size)
            var idx = 0
            while (iter.hasNext) {
              val value = iter.next()
              newArr.update(idx, edit(value, fields(idx).fieldType))
              idx += 1
            }
            newArr
        }
      case ListType(elemType) =>
        value match {
          case list: ArrayType =>
            val arr = delister(list)
            var idx = 0
            while (idx < arr.size) {
              arr.set(idx, edit(arr.get(idx), elemType))
              idx += 1
            }
            arr
        }
      case MapType(keyType, valueType) =>
        value match {
          case map: util.Map[Any, Any] =>
            val newMap = new util.HashMap[Any, Any]()
            val iter = map.entrySet().iterator()
            while (iter.hasNext) {
              val entry = iter.next()
              newMap.put(edit(entry.getKey, keyType), edit(entry.getValue, valueType))
            }
            newMap
        }
      case BinaryType => debinarizer(value.asInstanceOf[BinaryType])
      case StringType => deStringer(value.asInstanceOf[StringType])
      case ShortType  => value.asInstanceOf[Number].shortValue()
      case ByteType   => value.asInstanceOf[Number].byteValue()
      case _          => value
    }
  }

  // recursively traverse a chronon dataType value, and convert it to an external type
  def to[StructType, BinaryType, ListType, MapType, OutputSchema](
      value: Any,
      dataType: DataType,
      composer: (Iterator[Any], DataType, Option[OutputSchema]) => StructType,
      binarizer: Array[Byte] => BinaryType,
      collector: (Iterator[Any], Int) => ListType,
      mapper: (util.Map[Any, Any] => MapType),
      extraneousRecord: Any => Array[Any] = null,
      schemaTraverser: Option[SchemaTraverser[OutputSchema]] = None): Any = {

    if (value == null) return null

    def getFieldSchema(f: StructField) = schemaTraverser.map(_.getField(f))

    def edit(value: Any, dataType: DataType, subTreeTraverser: Option[SchemaTraverser[OutputSchema]]): Any =
      to(value, dataType, composer, binarizer, collector, mapper, extraneousRecord, subTreeTraverser)

    dataType match {
      case StructType(_, fields) =>
        value match {
          case arr: Array[Any] =>
            composer(
              arr.iterator.zipWithIndex.map {
                case (value, idx) => edit(value, fields(idx).fieldType, getFieldSchema(fields(idx)))
              },
              dataType,
              schemaTraverser.map(_.currentNode)
            )
          case list: util.ArrayList[Any] =>
            composer(
              list
                .iterator()
                .toScala
                .zipWithIndex
                .map { case (value, idx) => edit(value, fields(idx).fieldType, getFieldSchema(fields(idx))) },
              dataType,
              schemaTraverser.map(_.currentNode)
            )
          case value: Any =>
            assert(extraneousRecord != null, s"No handler for $value of class ${value.getClass}")
            composer(
              extraneousRecord(value).iterator.zipWithIndex.map {
                case (value, idx) => edit(value, fields(idx).fieldType, getFieldSchema(fields(idx)))
              },
              dataType,
              schemaTraverser.map(_.currentNode)
            )
        }
      case ListType(elemType) =>
        value match {
          case list: util.ArrayList[Any] =>
            collector(
              list.iterator().toScala.map(edit(_, elemType, schemaTraverser.map(_.getCollectionType))),
              list.size()
            )
          case arr: Array[_] => // avro only recognizes arrayList for its ArrayType/ListType
            collector(
              arr.iterator.map(edit(_, elemType, schemaTraverser.map(_.getCollectionType))),
              arr.length
            )
          case arr: mutable.WrappedArray[Any] => // handles the wrapped array type from transform function in spark sql
            collector(
              arr.iterator.map(edit(_, elemType, schemaTraverser.map(_.getCollectionType))),
              arr.length
            )
        }
      case MapType(keyType, valueType) =>
        value match {
          case map: util.Map[Any, Any] =>
            val newMap = new util.HashMap[Any, Any](map.size())
            map
              .entrySet()
              .iterator()
              .toScala
              .foreach { entry =>
                newMap.put(
                  edit(
                    entry.getKey,
                    keyType,
                    schemaTraverser.map(_.getMapKeyType)
                  ),
                  edit(
                    entry.getValue,
                    valueType,
                    schemaTraverser.map(_.getMapValueType)
                  )
                )
              }
            mapper(newMap)
          case map: collection.Map[Any, Any] =>
            val newMap = new util.HashMap[Any, Any](map.size)
            map
              .foreach { entry =>
                newMap.put(
                  edit(
                    entry._1,
                    keyType,
                    schemaTraverser.map(_.getMapKeyType)
                  ),
                  edit(
                    entry._2,
                    valueType,
                    schemaTraverser.map(_.getMapValueType)
                  )
                )
              }
            mapper(newMap)
        }
      case BinaryType  => binarizer(value.asInstanceOf[Array[Byte]])
      case IntType     => value.asInstanceOf[Number].intValue()
      case LongType    => value.asInstanceOf[Number].longValue()
      case DoubleType  => value.asInstanceOf[Number].doubleValue()
      case FloatType   => value.asInstanceOf[Number].floatValue()
      case ShortType   => value.asInstanceOf[Number].shortValue()
      case ByteType    => value.asInstanceOf[Number].byteValue()
      case BooleanType => value.asInstanceOf[Boolean]
      case StringType  => value.toString
      case _           => value
    }
  }
}
