package ai.zipline.spark

import ai.zipline.aggregator.base.{DataType => ZDataType, StructType => ZStructType}
import ai.zipline.fetcher.RowConversions.recursiveEdit
import ai.zipline.fetcher.{AvroCodec, AvroUtils, RowConversions}
import ai.zipline.spark.Extensions._
import org.apache.avro.generic.GenericData
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.apache.spark.sql.types.{BinaryType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

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
        result.update(0, KvRdd.toSparkRow(keys, keyZSchema))
        result.update(0, KvRdd.toSparkRow(values, valueZSchema))
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
        val record = RowConversions.toAvroRecord(data, schema).asInstanceOf[GenericData.Record]
        val bytes = codec.encodeRecord(record)
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
    recursiveEdit[GenericRow, Array[Byte]](value,
                                           dataType,
                                           { (data: Array[Any], _) => new GenericRow(data) },
                                           { bytes: Array[Byte] => bytes })
  }

}
