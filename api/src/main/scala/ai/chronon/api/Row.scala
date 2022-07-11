package ai.chronon.api

import java.util
import scala.collection.JavaConverters.asScalaIteratorConverter

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

object Row {
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
      case _          => value
    }
  }

  // recursively traverse a chronon dataType value, and convert it to an external type
  def to[StructType, BinaryType, ListType, MapType](value: Any,
                                                    dataType: DataType,
                                                    composer: (Iterator[Any], DataType) => StructType,
                                                    binarizer: Array[Byte] => BinaryType,
                                                    collector: (Iterator[Any], Int) => ListType,
                                                    mapper: (util.Map[Any, Any] => MapType),
                                                    extraneousRecord: Any => Array[Any] = null
                                                   ): Any = {

    if (value == null) return null
    def edit(value: Any, dataType: DataType): Any =
      to(value, dataType, composer, binarizer, collector, mapper, extraneousRecord)
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
          case value: Any =>
            assert(extraneousRecord != null, s"No handler for $value")
            composer(extraneousRecord(value).iterator.zipWithIndex.map { case (value, idx) => edit(value, fields(idx).fieldType) },
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
      case IntType => value.asInstanceOf[Number].intValue()
      case LongType => value.asInstanceOf[Number].longValue()
      case DoubleType => value.asInstanceOf[Number].doubleValue()
      case FloatType => value.asInstanceOf[Number].floatValue()
      case ShortType => value.asInstanceOf[Number].shortValue()
      case ByteType => value.asInstanceOf[Number].byteValue()
      case BooleanType => value.asInstanceOf[Boolean]
      case StringType => value.toString
      case _ => value
    }
  }
}
