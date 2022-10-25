package ai.chronon.spark.test
import ai.chronon.aggregator.test.Column
import ai.chronon.api._
import ai.chronon.spark.SparkSessionBuilder
import org.apache.spark.sql.SparkSession
import org.junit.Test
import ai.chronon.spark.stats.StatsGenerator

class StatsComputeTest {
  lazy val spark: SparkSession = SparkSessionBuilder.build("StatsComputeTest", local = true)

  @Test
  def summaryTest(): Unit = {
    val data = Seq(
      ("1", Some(1L), Some(1.0), Some("a")),
      ("1", Some(1L), None, Some("b")),
      ("2", Some(2L), None, None),
      ("3", None, None, Some("d"))
    )
    val columns = Seq("keyId", "value", "double_value", "string_value")
    val rdd = spark.sparkContext.parallelize(data)
    val df = spark.createDataFrame(rdd).toDF(columns: _*)
    val result = StatsGenerator.normalizedSummary(df, Seq("keyId"))
    result.show()
  }

  @Test
  def dailySummaryTest(): Unit = {
    val schema = List(
      Column("user", StringType, 10),
      Column("session_length", IntType, 10000)
    )
    val df = DataFrameGen.events(spark, schema, 100000, 10)
    val result = StatsGenerator.dailySummary(df, Seq("user") ).toFlatDf
    result.show()
  }

  @Test
  def driftTest(): Unit = {
    // Generate two datasets with the same distribution and check the drift
    val schema = List(
      Column("user", StringType, 10),
      Column("session_length", IntType, 10000)
    )
    val df1 = DataFrameGen.events(spark, schema, 100000, 10)
    val df2 = DataFrameGen.events(spark, schema, 100000, 10)
    val drift = StatsGenerator.summaryWithDrift(df1, df2, keys=Seq("user"))
    drift.foreach(println(_))
  }
}
