package ai.zipline.spark

import Extensions._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.apache.spark.sql.types.{BinaryType, StructField, StructType}

case class KvRdd(data: RDD[(Array[Any], Array[Any])], keySchema: StructType, valueSchema: StructType)(implicit
    sparkSession: SparkSession) {
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
        result.update(0, new GenericRow(keys))
        result.update(0, new GenericRow(values))
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

    val keyCodec = keySchema.toAvroCodec()
    val valueCodec = valueSchema.toAvroCodec()
    val rowRdd: RDD[Row] = data.map {
      case (keys, values) =>
        val result = new Array[Any](2)
        result.update(0, keyCodec.encodeArray(keys))
        result.update(0, valueCodec.encodeArray(values))
        new GenericRow(result)
    }
    sparkSession.createDataFrame(rowRdd, rowSchema)
  }

  def toFlatDf: DataFrame = {
    val rowSchema = StructType(
      Seq(
        StructField("key", StructType(keySchema)),
        StructField("value", valueSchema)
      )
    )
    val rowRdd: RDD[Row] = data.map {
      case (keys, values) =>
        val result = new Array[Any](keys.length + values.length)
        System.arraycopy(keys, 0, result, 0, keys.length)
        System.arraycopy(values, 0, result, keys.length, values.length)
        new GenericRow(result)
    }
    sparkSession.createDataFrame(rowRdd, rowSchema)
  }
}
