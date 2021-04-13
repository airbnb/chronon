package ai.zipline.spark

import Extensions._
import ai.zipline.aggregator.base.{
  ListType,
  MapType,
  DataType => ZDataType,
  StructType => ZStructType,
  BinaryType => ZBinaryType
}
import ai.zipline.fetcher.{AvroCodec, AvroUtils}
import com.google.gson.Gson
import org.apache.avro.generic.{GenericData, GenericRecord}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.apache.spark.sql.types.{BinaryType, StructField, StructType}

import java.nio.{ByteBuffer, CharBuffer, DoubleBuffer, FloatBuffer, IntBuffer, LongBuffer, ShortBuffer}
import java.util
import scala.collection.JavaConverters.{asScalaBufferConverter, asScalaIteratorConverter}

case class KvRdd(data: RDD[(Array[Any], Array[Any])], keySchema: StructType, valueSchema: StructType)(implicit
    sparkSession: SparkSession) {

  val keyZSchema: ZStructType = keySchema.toZiplineSchema("Key")
  val valueZSchema: ZStructType = valueSchema.toZiplineSchema("Value")
  val flatSchema: StructType = StructType(keySchema ++ valueSchema)
  val flatZSchema: ZStructType = flatSchema.toZiplineSchema("Flat")

  def toDf: DataFrame = {
    val rowSchema = StructType(
      Seq(
        StructField("key", StructType(keySchema)),
        StructField("value", valueSchema)
      )
    )
    val rowRdd: RDD[Row] = data.map {
      case (keys, values) =>
        val result = new Array[Any](2)
        result.update(0, RowConversions.toSparkRow(keys, keyZSchema))
        result.update(0, RowConversions.toSparkRow(values, valueZSchema))
        new GenericRow(result)
    }
    sparkSession.createDataFrame(rowRdd, rowSchema)
  }

  def toAvroDf: DataFrame = {
    val rowSchema = StructType(
      Seq(
        StructField("key", BinaryType),
        StructField("value", BinaryType)
      )
    )
    def encodeBytes(schema: ZStructType): Any => Array[Byte] = {
      val codec: AvroCodec = new AvroCodec(AvroUtils.fromZiplineSchema(schema).toString(true));
      { data: Any =>
        val record = RowConversions.toAvroRow(data, schema).asInstanceOf[GenericData.Record]
        val gson = new Gson()
        val bytes = codec.encodeRecord(record)
//        println(s"""
//                   |data: ${gson.toJson(data)}
//                   |record: ${record.toString}
//                   |bytes: ${gson.toJson(bytes)}
//                   |""".stripMargin)
        bytes
      }
    }

    println(s"""
         |keySchema: 
         |  ${AvroUtils.fromZiplineSchema(keyZSchema).toString(true)}
         |valueSchema: 
         |  ${AvroUtils.fromZiplineSchema(valueZSchema).toString(true)}
         |""".stripMargin)
    val keyToBytes = encodeBytes(keyZSchema)
    val valueToBytes = encodeBytes(valueZSchema)

    val rowRdd: RDD[Row] = data.map {
      case (keys, values) =>
        val result = new Array[Any](2)
        result.update(0, keyToBytes(keys))
        result.update(1, valueToBytes(values))
        val gson = new Gson()
        println(gson.toJson(result))
        new GenericRow(result)
    }
    sparkSession.createDataFrame(rowRdd, rowSchema)
  }

  def toFlatDf: DataFrame = {
    val rowRdd: RDD[Row] = data.map {
      case (keys, values) =>
        val result = new Array[Any](keys.length + values.length)
        System.arraycopy(keys, 0, result, 0, keys.length)
        System.arraycopy(values, 0, result, keys.length, values.length)
        RowConversions.toSparkRow(result, flatZSchema).asInstanceOf[GenericRow]
    }
    sparkSession.createDataFrame(rowRdd, flatSchema)
  }
}

object RowConversions {
  def toSparkRow(value: Any, dataType: ZDataType): Any = {
    recursiveEdit[GenericRow, Array[Byte]](value,
                                           dataType,
                                           { (data: Array[Any], _) => new GenericRow(data) },
                                           { bytes: Array[Byte] => bytes })
  }

  def toAvroRow(value: Any, dataType: ZDataType): Any = {
    // TODO: avoid schema generation multiple times
    // But this also has to happen at the recursive depth - data type and schema inside the compositor need to
    recursiveEdit[GenericRecord, ByteBuffer](
      value,
      dataType,
      { (data: Array[Any], elemDataType: ZDataType) =>
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

  // compositor converts a bare array of java types into a format specific row/record type
  // recursively explore the ZDataType to explore where composition/record building happens
  def recursiveEdit[CompositeType, BinaryType](value: Any,
                                               dataType: ZDataType,
                                               compositor: (Array[Any], ZDataType) => CompositeType,
                                               binarizer: Array[Byte] => BinaryType): Any = {
    if (value == null) return null
    def edit(value: Any, dataType: ZDataType): Any = recursiveEdit(value, dataType, compositor, binarizer)
    dataType match {
      case ZStructType(_, fields) =>
        value match {
          case arr: Array[Any] =>
            compositor(arr.indices.map { idx => edit(arr(idx), fields(idx).fieldType) }.toArray, dataType)
          case list: util.ArrayList[Any] =>
            compositor(list.asScala.indices.map { idx => edit(list.get(idx), fields(idx).fieldType) }.toArray, dataType)
        }
      case ListType(elemType) =>
        value match {
          case list: util.ArrayList[Any] =>
            val newList = new util.ArrayList[Any](list.size())
            (0 until list.size()).foreach { idx => newList.add(edit(list.get(idx), elemType)) }
            newList
          case arr: Array[Any] => // avro only recognizes arrayList for its ArrayType/ListType
            val newArr = new util.ArrayList[Any](arr.size)
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
      case ZBinaryType => binarizer(value.asInstanceOf[Array[Byte]])
      case _           => value
    }
  }
}
