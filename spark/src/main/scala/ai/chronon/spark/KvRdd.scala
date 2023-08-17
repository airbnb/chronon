package ai.chronon.spark

import ai.chronon.api
import ai.chronon.online.{AvroCodec, AvroConversions, SparkConversions}
import ai.chronon.spark.Extensions._
import org.apache.avro.generic.GenericData
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.{GenericRow, GenericRowWithSchema}
import org.apache.spark.sql.types.{BinaryType, LongType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

object GenericRowHandler {
  val func: Any => Array[Any] = {
    // TODO: optimize this later - to iterator
    case x: GenericRowWithSchema => x.toSeq.toArray
  }
}

sealed trait BaseKvRdd {

  def keySchema: StructType
  def valueSchema: StructType
  def withTime: Boolean
  val timeField: StructField = StructField(api.Constants.TimeColumn, LongType)
  val keyZSchema: api.StructType = keySchema.toChrononSchema("Key")
  val valueZSchema: api.StructType = valueSchema.toChrononSchema("Value")
  val baseFlatSchema: StructType = StructType(keySchema ++ valueSchema)
  def flatSchema: StructType = if (withTime) StructType(baseFlatSchema :+ timeField) else baseFlatSchema
  def flatZSchema: api.StructType = flatSchema.toChrononSchema("Flat")
  lazy val keyToBytes: Any => Array[Byte] = AvroConversions.encodeBytes(keyZSchema, GenericRowHandler.func)
  lazy val valueToBytes: Any => Array[Byte] = AvroConversions.encodeBytes(valueZSchema, GenericRowHandler.func)
  lazy val keyToJson: Any => String = AvroConversions.encodeJson(keyZSchema, GenericRowHandler.func)
  lazy val valueToJson: Any => String = AvroConversions.encodeJson(valueZSchema, GenericRowHandler.func)
  private val baseRowSchema = StructType(
    Seq(
      StructField("key_bytes", BinaryType),
      StructField("value_bytes", BinaryType),
      StructField("key_json", StringType),
      StructField("value_json", StringType)
    )
  )
  def rowSchema: StructType = if (withTime) StructType(baseRowSchema :+ timeField) else baseRowSchema

  def toFlatDf: DataFrame
}

case class KvRdd(data: RDD[(Array[Any], Array[Any])], keySchema: StructType, valueSchema: StructType)(implicit
    sparkSession: SparkSession)
    extends BaseKvRdd {
  val withTime = false

  def toAvroDf(jsonPercent: Int = 1): DataFrame = {
    val avroRdd: RDD[Row] = data.map {
      case (keys: Array[Any], values: Array[Any]) =>
        // json encoding is very expensive (50% of entire job).
        // We only do it for a specified fraction to retain debuggability.
        val (keyJson, valueJson) = if (math.random < jsonPercent.toDouble / 100) {
          (keyToJson(keys), valueToJson(values))
        } else {
          (null, null)
        }
        val result: Array[Any] = Array(keyToBytes(keys), valueToBytes(values), keyJson, valueJson)
        new GenericRow(result)
    }
    println(s"""
          |key schema:
          |  ${AvroConversions.fromChrononSchema(keyZSchema).toString(true)}
          |value schema:
          |  ${AvroConversions.fromChrononSchema(valueZSchema).toString(true)}
          |""".stripMargin)
    sparkSession.createDataFrame(avroRdd, rowSchema)
  }

  override def toFlatDf: DataFrame = {
    val flatRdd: RDD[Row] = data.map {
      case (keys: Array[Any], values: Array[Any]) =>
        val result = new Array[Any](keys.length + values.length)
        System.arraycopy(keys, 0, result, 0, keys.length)
        System.arraycopy(values, 0, result, keys.length, values.length)
        SparkConversions.toSparkRow(result, flatZSchema, GenericRowHandler.func).asInstanceOf[GenericRow]
    }
    sparkSession.createDataFrame(flatRdd, flatSchema)
  }
}

case class TimedKvRdd(data: RDD[(Array[Any], Array[Any], Long)], keySchema: StructType, valueSchema: StructType)(
    implicit sparkSession: SparkSession)
    extends BaseKvRdd {
  val withTime = true

  // TODO make json percent configurable
  def toAvroDf: DataFrame = {
    val avroRdd: RDD[Row] = data.map {
      case (keys, values, ts) =>
        val (keyJson, valueJson) = if (math.random < 0.01) {
          (keyToJson(keys), valueToJson(values))
        } else {
          (null, null)
        }
        val result: Array[Any] = Array(keyToBytes(keys), valueToBytes(values), keyJson, valueJson, ts)
        new GenericRow(result)
    }
    println(s"""
         |key schema:
         |  ${AvroConversions.fromChrononSchema(keyZSchema).toString(true)}
         |value schema:
         |  ${AvroConversions.fromChrononSchema(valueZSchema).toString(true)}
         |""".stripMargin)
    sparkSession.createDataFrame(avroRdd, rowSchema)
  }

  override def toFlatDf: DataFrame = {
    val flatRdd: RDD[Row] = data.map {
      case (keys, values, ts) =>
        val result = new Array[Any](keys.length + values.length + 1)
        System.arraycopy(keys, 0, result, 0, keys.length)
        System.arraycopy(values, 0, result, keys.length, values.length)
        result(result.length - 1) = ts
        SparkConversions.toSparkRow(result, flatZSchema, GenericRowHandler.func).asInstanceOf[GenericRow]
    }
    sparkSession.createDataFrame(flatRdd, flatSchema)
  }
}
