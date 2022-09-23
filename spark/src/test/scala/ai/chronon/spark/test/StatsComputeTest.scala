package ai.chronon.spark.test
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
    val df = spark.createDataFrame(rdd).toDF(columns:_*)
    val result = StatsGenerator.summary(df, Seq("keyId"))
    result.show()
    result.printSchema()
  }

}
