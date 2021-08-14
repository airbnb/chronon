package ai.zipline.fetcher

import ai.zipline.api._
import org.apache.avro.generic.{GenericData, GenericRecord}
import org.apache.avro.util.Utf8

import java.nio.ByteBuffer
import java.util
import scala.collection.AbstractIterator
import scala.collection.JavaConverters._

object RowConversions {
  def toAvroRecord(value: Any, dataType: DataType): Any = {
    // TODO: avoid schema generation multiple times
    // But this also has to happen at the recursive depth - data type and schema inside the compositor need to
    recursiveEdit[GenericRecord, ByteBuffer, util.ArrayList[Any], util.Map[Any, Any]](
      value,
      dataType,
      { (data: Iterator[Any], elemDataType: DataType) =>
        val schema = AvroUtils.fromZiplineSchema(elemDataType)
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

  def fromAvroRecord(value: Any, dataType: DataType): Any = {
    inverseEdit[GenericRecord, ByteBuffer, GenericData.Array[Any], Utf8](
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
        val arr = new Array[Any](garr.size)
        var idx = 0
        while (idx < garr.size()) {
          arr.update(idx, garr.get(idx))
          idx += 1
        }
        arr
      },
      { (avString: Utf8) => avString.toString }
    )
  }

  def inverseEdit[CompositeType, BinaryType, ArrayType, StringType](
      value: Any,
      dataType: DataType,
      decomposer: (CompositeType, Seq[StructField]) => Iterator[Any],
      debinarizer: BinaryType => Array[Byte],
      delister: ArrayType => Array[Any],
      deStringer: StringType => String): Any = {
    if (value == null) return null
    def edit(value: Any, dataType: DataType): Any =
      inverseEdit(value, dataType, decomposer, debinarizer, delister, deStringer)
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
              arr.update(idx, edit(arr(idx), elemType))
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
      case _          => value
    }
  }

  // compositor converts a bare array of java types into a format specific row/record type
  // recursively explore the ZDataType to explore where composition/record building happens
  def recursiveEdit[StructType, BinaryType, ListType, MapType](value: Any,
                                                               dataType: DataType,
                                                               composer: (Iterator[Any], DataType) => StructType,
                                                               binarizer: Array[Byte] => BinaryType,
                                                               collector: (Iterator[Any], Int) => ListType,
                                                               mapper: (util.Map[Any, Any] => MapType)): Any = {

    if (value == null) return null
    def edit(value: Any, dataType: DataType): Any =
      recursiveEdit(value, dataType, composer, binarizer, collector, mapper)
    dataType match {
      case StructType(_, fields) =>
        value match {
          case arr: Array[Any] =>
            composer(arr.iterator.zipWithIndex.map { case (value, idx) => edit(value, fields(idx).fieldType) },
                     dataType)
          case list: util.ArrayList[Any] =>
            composer(list
                       .iterator()
                       .asScala
                       .zipWithIndex
                       .map { case (value, idx) => edit(value, fields(idx).fieldType) },
                     dataType)
        }
      case ListType(elemType) =>
        value match {
          case list: util.ArrayList[Any] =>
            collector(list.iterator().asScala.map(edit(_, elemType)), list.size())
          case arr: Array[Any] => // avro only recognizes arrayList for its ArrayType/ListType
            collector(arr.iterator.map(edit(_, elemType)), arr.length)
        }
      case MapType(keyType, valueType) =>
        value match {
          case map: util.Map[Any, Any] =>
            val newMap = new util.HashMap[Any, Any](map.size())
            map
              .entrySet()
              .iterator()
              .asScala
              .foreach { entry => newMap.put(edit(entry.getKey, keyType), edit(entry.getValue, valueType)) }
            mapper(newMap)
        }
      case BinaryType => binarizer(value.asInstanceOf[Array[Byte]])
      case _          => value
    }
  }
}
