package ai.zipline.spark

import ai.zipline.aggregator.base.{DataType => ZDataType, StructType => ZStructType}
import ai.zipline.fetcher.RowConversions.recursiveEdit
import ai.zipline.fetcher.{AvroCodec, AvroUtils, RowConversions}
import ai.zipline.spark.Extensions._
import com.google.gson.Gson
import org.apache.avro.generic.GenericData
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.apache.spark.sql.types.{BinaryType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import java.util
import scala.collection.JavaConverters.mapAsScalaMapConverter
import scala.collection.mutable
case class KvRdd(data: RDD[(Array[Any], Array[Any])], keySchema: StructType, valueSchema: StructType)(implicit
    sparkSession: SparkSession) {

  val keyZSchema: ZStructType = keySchema.toZiplineSchema("Key")
  val valueZSchema: ZStructType = valueSchema.toZiplineSchema("Value")
  val flatSchema: StructType = StructType(keySchema ++ valueSchema)
  val flatZSchema: ZStructType = flatSchema.toZiplineSchema("Flat")

  def toAvroDf: DataFrame = {
    val rowSchema = StructType(
      Seq(
        StructField("key_bytes", BinaryType),
        StructField("value_bytes", BinaryType),
        StructField("key_json", StringType),
        StructField("value_json", StringType)
      )
    )
    def encodeBytes(schema: ZStructType): Any => Array[Byte] = {
      val codec: AvroCodec = new AvroCodec(AvroUtils.fromZiplineSchema(schema).toString(true));
      { data: Any =>
        val record = RowConversions.toAvroRecord(data, schema).asInstanceOf[GenericData.Record]
        val bytes = codec.encodeBinary(record)
        bytes
      }
    }

    def encodeJson(schema: ZStructType): Any => String = {
      val codec: AvroCodec = new AvroCodec(AvroUtils.fromZiplineSchema(schema).toString(true));
      { data: Any =>
        val record = RowConversions.toAvroRecord(data, schema).asInstanceOf[GenericData.Record]
        val bytes = codec.encodeJson(record)
        bytes
      }
    }

    println(s"""
         |key schema: 
         |  ${AvroUtils.fromZiplineSchema(keyZSchema).toString(true)}
         |value schema: 
         |  ${AvroUtils.fromZiplineSchema(valueZSchema).toString(true)}
         |""".stripMargin)
    val keyToBytes = encodeBytes(keyZSchema)
    val valueToBytes = encodeBytes(valueZSchema)
    val keyToJson = encodeJson(keyZSchema)
    val valueToJson = encodeJson(valueZSchema)

    val rowRdd: RDD[Row] = data.map {
      case (keys, values) =>
        val result: Array[Any] = Array(keyToBytes(keys), valueToBytes(values), keyToJson(keys), valueToJson(values))
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
        KvRdd.toSparkRow(result, flatZSchema).asInstanceOf[GenericRow]
    }
    sparkSession.createDataFrame(rowRdd, flatSchema)
  }
}

object KvRdd {
  def toSparkRow(value: Any, dataType: ZDataType): Any = {
    recursiveEdit[GenericRow, Array[Byte], Array[Any], mutable.Map[Any, Any]](
      value,
      dataType,
      { (data: Array[Any], _) => new GenericRow(data) },
      { bytes: Array[Byte] => bytes },
      { (elems: Iterator[Any], size: Int) =>
        val result = new Array[Any](size)
        var i = 0
        for (x <- elems) { result(i) = x; i += 1 }
        result
      },
      { m: util.Map[Any, Any] =>
        val result = new mutable.HashMap[Any, Any]
        val it = m.entrySet().iterator()
        while (it.hasNext) {
          val entry = it.next()
          result.update(entry.getKey, entry.getValue)
        }
        result
      }
    )
  }

}
