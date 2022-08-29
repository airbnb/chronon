package ai.chronon.spark.test

import ai.chronon.api.StructType
import ai.chronon.online.AvroConversions
import ai.chronon.spark.{Conversions, GenericRowHandler}
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

class InMemoryStream {

  private val MemoryStreamID = 42

  // encode input as avro byte array and insert into memory stream.
  def getInMemoryStreamDF(spark: SparkSession, inputDf: Dataset[Row]): DataFrame = {
    val schema: StructType = StructType.from("input", Conversions.toChrononSchema(inputDf.schema))
    import spark.implicits._
    val input: MemoryStream[Array[Byte]] = new MemoryStream[Array[Byte]](MemoryStreamID, spark.sqlContext)
    val encoder = AvroConversions.encodeBytes(schema, GenericRowHandler.func)
    input.addData(inputDf.collect.map { row: Row =>
      encoder(row)
    })
    input.toDF
  }
}
