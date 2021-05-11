package ai.zipline.fetcher

import ai.zipline.aggregator.base.{BinaryType, DataType, ListType, MapType, StructField, StructType}
import org.apache.avro.generic.{GenericData, GenericRecord}

import java.nio.ByteBuffer
import java.util
import scala.collection.JavaConverters._

object RowConversions {
  def toAvroRecord(value: Any, dataType: DataType): Any = {
    // TODO: avoid schema generation multiple times
    // But this also has to happen at the recursive depth - data type and schema inside the compositor need to
    recursiveEdit[GenericRecord, ByteBuffer](
      value,
      dataType,
      { (data: Array[Any], elemDataType: DataType) =>
        val schema = AvroUtils.fromZiplineSchema(elemDataType)
        val record = new GenericData.Record(schema)
        for (i <- data.indices) {
          record.put(i, data(i))
        }
        record
      },
      ByteBuffer.wrap
    )
  }

  def fromAvroRecord(value: Any, dataType: DataType): Any = {
    inverseEdit[GenericRecord, ByteBuffer, GenericData.Array[Any]](
      value,
      dataType,
      { (record: GenericRecord, fields: Seq[StructField]) => fields.indices.map(record.get).toArray },
      { (byteBuffer: ByteBuffer) => byteBuffer.array() },
      { (garr: GenericData.Array[Any]) => (0 until garr.size()).map { garr.get }.toArray }
    )
  }

  def inverseEdit[CompositeType, BinaryType, ArrayType](value: Any,
                                                        dataType: DataType,
                                                        decomposer: (CompositeType, Seq[StructField]) => Array[Any],
                                                        debinarizer: BinaryType => Array[Byte],
                                                        delister: ArrayType => Array[Any]): Any = {
    if (value == null) return null
    def edit(value: Any, dataType: DataType): Any = inverseEdit(value, dataType, decomposer, debinarizer, delister)
    dataType match {
      case StructType(_, fields) =>
        value match {
          case record: CompositeType =>
            val arr = decomposer(record, fields)
            fields.indices.map { idx => edit(arr(idx), fields(idx).fieldType) }.toArray
        }
      case ListType(elemType) =>
        value match {
          case list: ArrayType =>
            val arr = delister(list)
            arr.indices.foreach { idx => arr.update(idx, edit(arr(idx), elemType)) }
            arr
        }
      case BinaryType => debinarizer(value.asInstanceOf[BinaryType])
      case _          => value
    }
  }

  // compositor converts a bare array of java types into a format specific row/record type
  // recursively explore the ZDataType to explore where composition/record building happens
  def recursiveEdit[CompositeType, BinaryType](value: Any,
                                               dataType: DataType,
                                               composer: (Array[Any], DataType) => CompositeType,
                                               binarizer: Array[Byte] => BinaryType): Any = {
    if (value == null) return null
    def edit(value: Any, dataType: DataType): Any = recursiveEdit(value, dataType, composer, binarizer)
    dataType match {
      case StructType(_, fields) =>
        value match {
          case arr: Array[Any] =>
            composer(arr.indices.map { idx => edit(arr(idx), fields(idx).fieldType) }.toArray, dataType)
          case list: util.ArrayList[Any] =>
            composer(list.asScala.indices.map { idx => edit(list.get(idx), fields(idx).fieldType) }.toArray, dataType)
        }
      case ListType(elemType) =>
        value match {
          case list: util.ArrayList[Any] =>
            val newList = new util.ArrayList[Any](list.size())
            (0 until list.size()).foreach { idx => newList.add(edit(list.get(idx), elemType)) }
            newList
          case arr: Array[Any] => // avro only recognizes arrayList for its ArrayType/ListType
            val newArr = new util.ArrayList[Any](arr.length)
            arr.foreach { elem => newArr.add(edit(elem, elemType)) }
            newArr
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
            newMap
        }
      case BinaryType => binarizer(value.asInstanceOf[Array[Byte]])
      case _          => value
    }
  }
}
