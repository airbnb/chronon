package ai.chronon.spark

import ai.chronon.api
import ai.chronon.online.{AvroCodec, AvroConversions, SparkConversions}
import ai.chronon.spark.Extensions._
import org.apache.avro.generic.GenericData
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.{GenericRow, GenericRowWithSchema}
import org.apache.spark.sql.types.{BinaryType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

object GenericRowHandler {
  val func: Any => Array[Any] = { input: Any =>
    input match {
      // TODO: optimize this later - to iterator
      case x: GenericRowWithSchema => x.toSeq.toArray
    }
  }
}

case class KvRdd(data: RDD[(Array[Any], Array[Any])], keySchema: StructType, valueSchema: StructType)(implicit
    sparkSession: SparkSession) {

  val keyZSchema: api.StructType = keySchema.toChrononSchema("Key")
  val valueZSchema: api.StructType = valueSchema.toChrononSchema("Value")
  val flatSchema: StructType = StructType(keySchema ++ valueSchema)
  val flatZSchema: api.StructType = flatSchema.toChrononSchema("Flat")

  def toAvroDf: DataFrame = {
    val rowSchema = StructType(
      Seq(
        StructField("key_bytes", BinaryType),
        StructField("value_bytes", BinaryType),
        StructField("key_json", StringType),
        StructField("value_json", StringType)
      )
    )

    println(s"""
         |key schema: 
         |  ${AvroConversions.fromChrononSchema(keyZSchema).toString(true)}
         |value schema: 
         |  ${AvroConversions.fromChrononSchema(valueZSchema).toString(true)}
         |""".stripMargin)
    val keyToBytes = AvroConversions.encodeBytes(keyZSchema, GenericRowHandler.func)
    val valueToBytes = AvroConversions.encodeBytes(valueZSchema, GenericRowHandler.func)
    val keyToJson = AvroConversions.encodeJson(keyZSchema, GenericRowHandler.func)
    val valueToJson = AvroConversions.encodeJson(valueZSchema, GenericRowHandler.func)

    val rowRdd: RDD[Row] = data.map {
      case (keys, values) =>
        // json encoding is very expensive (50% of entire job).
        // Only do it for a small fraction to retain debuggability.
        val (keyJson, valueJson) = if (math.random < 0.01) {
          (keyToJson(keys), valueToJson(values))
        } else {
          (null, null)
        }
        val result: Array[Any] = Array(keyToBytes(keys), valueToBytes(values), keyJson, valueJson)
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
        SparkConversions.toSparkRow(result, flatZSchema, GenericRowHandler.func).asInstanceOf[GenericRow]
    }
    sparkSession.createDataFrame(rowRdd, flatSchema)
  }
}
