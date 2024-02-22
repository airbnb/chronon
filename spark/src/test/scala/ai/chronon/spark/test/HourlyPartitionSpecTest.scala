package ai.chronon.spark.test

import ai.chronon.api.Extensions.{WindowOps, WindowUtils}
import ai.chronon.api.{PartitionSpec, TimeUnit, Window}
import ai.chronon.spark.{SparkSessionBuilder, TestHourlyTableUtils}
import ai.chronon.spark.Extensions.DataframeOps
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
import org.junit.Test
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

class HourlyPartitionSpecTest {
  val partitionSpec = PartitionSpec(format = "yyyyMMddHH", spanMillis = WindowUtils.Hour.millis)

  @Test
  def testToAndFromMillis(): Unit = {
    val ts = 1704216057000L // Tuesday, January 2, 2024 17:20:57 UTC => 2024010217
    assert(partitionSpec.at(ts) == "2024010217")
    assert(partitionSpec.epochMillis("2024010217") == 1704214800000L)
  }

  @Test
  def testPartitionSpecMinus(): Unit = {
    val ts = 1704216057000L // Tuesday, January 2, 2024 17:20:57 UTC => 2024010217
    val today = partitionSpec.at(ts)
    assert(partitionSpec.minus(today, new Window(1, TimeUnit.DAYS)) == "2024010117")
    assert(partitionSpec.minus(today, new Window(1, TimeUnit.HOURS)) == "2024010216")
    assert(partitionSpec.shift(today, 1) == "2024010218")
    assert(partitionSpec.shift(today, -3) == "2024010214")
  }

  @Test
  def dataFrameOpsHourlyTest(): Unit = {
    val spark: SparkSession = SparkSessionBuilder.build("HourlyPartitionSpecTest", local = true)
    val testHourlyTableUtils = TestHourlyTableUtils(spark)
    val rows = Seq(
      Seq("A", 1704069360000L, "2024010100"), // Monday, January 1, 2024 12:36:00 AM UTC
      Seq("A", 1704237300000L, "2024010223") // Tuesday, January 2, 2024 11:15:00 PM UTC
    )

    val itemQuerySparkSchema = StructType(
      List(
        StructField("item", StringType, nullable = true),
        StructField("ts", LongType, nullable = true),
        StructField("ds", StringType, nullable = true)
      )
    )
    val itemQueryRdd: RDD[Row] = spark.sparkContext.parallelize(
      rows.map(Row(_: _*))
    )
    val itemQueryDf: DataFrame = spark.createDataFrame(
      itemQueryRdd,
      itemQuerySparkSchema
    )
    val shifted = itemQueryDf.withShiftedPartition("shifted_ds", 2, testHourlyTableUtils)
    val shiftedTimestamps = shifted.collect().map(_.getAs[String]("shifted_ds")).toSet
    assert(shiftedTimestamps == Set("2024010102", "2024010301"))
  }

}
