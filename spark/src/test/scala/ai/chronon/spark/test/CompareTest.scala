package ai.chronon.spark.test

import ai.chronon.aggregator.windowing.TsUtils
import ai.chronon.online.DataMetrics
import ai.chronon.spark.stats.CompareJob
import ai.chronon.spark.{SparkSessionBuilder, TableUtils}
import org.apache.spark.sql.SparkSession
import org.junit.Test

class CompareTest {
  lazy val spark: SparkSession = SparkSessionBuilder.build("CompareTest", local = true)

  private val tableUtils = TableUtils(spark)

  def toTs(arg: String): Long = TsUtils.datetimeToTs(arg)

  @Test
  def basicTest(): Unit = {
    val leftData = Seq(
      (1, Some(1), 1.0, "a", toTs("2021-04-10 09:00:00"), "2021-04-10"),
      (1, Some(2), 2.0, "b", toTs("2021-04-10 11:00:00"), "2021-04-10"),
      (2, Some(2), 2.0, "c", toTs("2021-04-10 15:00:00"), "2021-04-10"),
      (3, None, 1.0, "d", toTs("2021-04-10 19:00:00"), "2021-04-10")
    )

    val rightData = Seq(
      (1, Some(1), 5.0, "a", toTs("2021-04-10 09:00:00"), "2021-04-10"),
      (2, Some(3), 2.0, "b", toTs("2021-04-10 11:00:00"), "2021-04-10"),
      (2, Some(5), 6.0, "c", toTs("2021-04-10 15:00:00"), "2021-04-10"),
      (3, None, 1.0, "d", toTs("2021-04-10 19:00:00"), "2021-04-10")
    )

    val columns = Seq("serial", "value", "rating", "keyId", "ts", "ds")
    val leftRdd = spark.sparkContext.parallelize(leftData)
    val leftDf = spark.createDataFrame(leftRdd).toDF(columns:_*)
    val rightRdd = spark.sparkContext.parallelize(rightData)
    val rightDf = spark.createDataFrame(rightRdd).toDF(columns:_*)

    leftDf.show()
    rightDf.show()

    val keys = Seq("keyId", "ts", "ds")
    val result: DataMetrics = CompareJob.compare(leftDf, rightDf, keys)
    println(result)
    assert(result.series.length == 4, "Invalid result length")
    for (rowIndex <- 0 until leftData.length) {
      for ((colName, index) <- columns.zipWithIndex) {
        // The current column is either a key or a time column skipping it
        if (!keys.contains(colName)) {
          val actualMismatchCount = result.series(rowIndex)._2(s"${colName}_mismatch_sum")
          if (leftData(rowIndex).productElement(index) == rightData(rowIndex).productElement(index)) {
            assert(actualMismatchCount == 0, "Expected no mismatches")
          } else {
            assert(actualMismatchCount == 1, "Expected one mismatch")
          }
        }
      }
    }
  }

  @Test
  def mappingTest(): Unit = {
    val leftData = Seq(
      (1, Some(1), 1.0, "a", toTs("2021-04-10 09:00:00"), "2021-04-10"),
      (1, Some(2), 2.0, "b", toTs("2021-04-10 11:00:00"), "2021-04-10"),
      (2, Some(2), 2.0, "c", toTs("2021-04-10 15:00:00"), "2021-04-10"),
      (3, None, 1.0, "d", toTs("2021-04-10 19:00:00"), "2021-04-10")
    )

    val rightData = Seq(
      (1, Some(1), 5.0, "a", toTs("2021-04-10 09:00:00"), "2021-04-10"),
      (2, Some(3), 2.0, "b", toTs("2021-04-10 11:00:00"), "2021-04-10"),
      (2, Some(5), 6.0, "c", toTs("2021-04-10 15:00:00"), "2021-04-10"),
      (3, None, 1.0, "d", toTs("2021-04-10 19:00:00"), "2021-04-10")
    )

    val leftColumns = Seq("serial", "value", "rating", "keyId", "ts", "ds")
    val leftRdd = spark.sparkContext.parallelize(leftData)
    val leftDf = spark.createDataFrame(leftRdd).toDF(leftColumns:_*)
    val rightColumns = Seq("rev_serial", "rev_value", "rev_rating", "keyId", "ts", "ds")
    val rightRdd = spark.sparkContext.parallelize(rightData)
    val rightDf = spark.createDataFrame(rightRdd).toDF(rightColumns:_*)

    leftDf.show()
    rightDf.show()

    val keys = Seq("keyId", "ts", "ds")
    val result = CompareJob.compare(
        leftDf,
        rightDf,
        keys,
        Map("serial" -> "rev_serial", "value" -> "rev_value", "rating" -> "rev_rating")
    )
    println(result)
    assert(result.series.length == 4, "Invalid result length")
    for (rowIndex <- 0 until leftData.length) {
      for ((colName, index) <- leftColumns.zipWithIndex) {
        // The current column is either a key or a time column skipping it
        if (!keys.contains(colName)) {
          val actualMismatchCount = result.series(rowIndex)._2(s"${colName}_mismatch_sum")
          if (leftData(rowIndex).productElement(index) == rightData(rowIndex).productElement(index)) {
            assert(actualMismatchCount == 0, "Expected no mismatches")
          } else {
            assert(actualMismatchCount == 1, "Expected one mismatch")
          }
        }
      }
    }
  }
}
