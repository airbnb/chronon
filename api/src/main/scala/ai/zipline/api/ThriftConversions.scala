package ai.zipline.api

import org.apache.thrift.meta_data._
import org.apache.thrift.protocol.TType
import org.apache.thrift.{TBase, TFieldIdEnum}

import java.nio.ByteBuffer
import java.util
import scala.collection.JavaConverters.asScalaIteratorConverter

// Utility methods to convert thrift objects into zipline rows.
// The main purpose of which is to handle streams of data encoded in thrift and convert them to thrift.
// Long term, we could use this to execute sql over service responses.
object ThriftConversions {

  def buildStructMetaData(className: String): StructMetaData = {
    val clazz = Class
      .forName(className)
      .asInstanceOf[Class[_ <: TBase[T, F] forSome { type T <: TBase[T, F]; type F <: TFieldIdEnum }]]
    new StructMetaData(TType.STRUCT, clazz)
  }
// Class[_ <: TBase[?0(in trait TBase),?1] forSome { type ?0(in trait TBase) <: TBase[?0(in trait TBase),?1]; type ?1 <: TFieldIdEnum }]
  def extractSchema(structMetaData: StructMetaData, tokens: util.Set[String] = null): StructType = {
    toZType(structMetaData, tokens).asInstanceOf[StructType]
  }

  def extractRow(obj: Any, structMetaData: StructMetaData, tokens: util.Set[String] = null): Any = {
    toZValue(obj, structMetaData, tokens)
  }

  def tokenize(query: String): util.Set[String] = {
    if (query == null) return null
    val hashSet = new util.HashSet[String]
    query.split("[^A-Za-z0-9_]+").foreach(hashSet.add)
    hashSet
  }

  def toZValue(obj: Any, valueMetaData: FieldValueMetaData, tokens: util.Set[String]): Any = {
    if (obj == null) return null
    valueMetaData.`type` match {
      case TType.BOOL | TType.BYTE | TType.DOUBLE | TType.I16 | TType.I32 | TType.ENUM | TType.I64 => obj

      case TType.STRING =>
        if (valueMetaData.isBinary) {
          obj.asInstanceOf[ByteBuffer].array()
        } else {
          obj
        }

      case TType.MAP =>
        val mapMeta = valueMetaData.asInstanceOf[MapMetaData]
        val map = obj.asInstanceOf[util.Map[_, _]]
        val result = new util.HashMap[Any, Any]()
        val entries = map.entrySet().iterator().asScala
        entries.foreach { entry =>
          result.put(toZValue(entry.getKey, mapMeta.keyMetaData, tokens),
                     toZValue(entry.getValue, mapMeta.valueMetaData, tokens))
        }
        result

      case TType.LIST =>
        val listMeta = valueMetaData.asInstanceOf[ListMetaData]
        val list = obj.asInstanceOf[util.List[_]]
        val result = new util.ArrayList[Any](list.size())
        var i = 0
        while (i < list.size()) {
          result.add(toZValue(list.get(i), listMeta.elemMetaData, tokens))
          i += 1
        }
        result

      case TType.SET =>
        val setMeta = valueMetaData.asInstanceOf[SetMetaData]
        val set = obj.asInstanceOf[util.Set[_]]
        val result = new util.ArrayList[Any](set.size())
        var i = 0
        val iter = set.iterator()
        while (i < set.size()) {
          result.add(toZValue(iter.next(), setMeta.elemMetaData, tokens))
          i += 1
        }
        result

      case TType.STRUCT =>
        val structMeta = valueMetaData.asInstanceOf[StructMetaData]
        val metaMap = FieldMetaData.getStructMetaDataMap(structMeta.structClass)
        val result = new util.ArrayList[Any]()
        val value = obj.asInstanceOf[TBase[_, TFieldIdEnum]]
        metaMap.entrySet().iterator().asScala.foreach { entry =>
          val fieldName = entry.getKey.getFieldName
          val fieldId = entry.getKey.getThriftFieldId
          val valueMeta = entry.getValue.valueMetaData
          if (tokens == null || tokens.contains(fieldName)) {
            val obj = value.getFieldValue(value.fieldForId(fieldId))
            result.add(toZValue(obj, valueMeta, tokens))
          }
        }
        result.toArray

      case _ => throw new IllegalArgumentException(s"Cannot cast ${valueMetaData.`type`}")
    }
  }

  // recursively walk the struct metaData to create zipline schema
  def toZType(valueMetaData: FieldValueMetaData, tokens: util.Set[String]): DataType = {
    valueMetaData.`type` match {
      case TType.BOOL             => BooleanType
      case TType.BYTE             => ByteType
      case TType.DOUBLE           => DoubleType
      case TType.I16              => ShortType
      case TType.I32 | TType.ENUM => IntType
      case TType.I64              => LongType

      case TType.STRING =>
        if (valueMetaData.isBinary) {
          BinaryType
        } else {
          StringType
        }

      case TType.MAP =>
        val mapMeta = valueMetaData.asInstanceOf[MapMetaData]
        MapType(toZType(mapMeta.keyMetaData, tokens), toZType(mapMeta.valueMetaData, tokens))

      case TType.LIST =>
        val listMeta = valueMetaData.asInstanceOf[ListMetaData]
        ListType(toZType(listMeta.elemMetaData, tokens))

      case TType.SET =>
        val setMeta = valueMetaData.asInstanceOf[SetMetaData]
        ListType(toZType(setMeta.elemMetaData, tokens))

      case TType.STRUCT =>
        val structMeta = valueMetaData.asInstanceOf[StructMetaData]
        val metaMap = FieldMetaData.getStructMetaDataMap(structMeta.structClass)
        val fields = metaMap.entrySet().iterator().asScala.flatMap { entry =>
          val fieldName = entry.getKey.getFieldName
          if (tokens == null || tokens.contains(fieldName))
            Option(StructField(fieldName, toZType(entry.getValue.valueMetaData, tokens)))
          else None
        }
        StructType(structMeta.structClass.getName, fields.toArray)

      case _ => throw new IllegalArgumentException(s"Cannot cast ${valueMetaData.`type`}")
    }
  }
}
