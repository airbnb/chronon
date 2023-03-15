package ai.chronon.spark.test

import ai.chronon.aggregator.windowing.TsUtils
import ai.chronon.online.DataMetrics
import ai.chronon.spark.stats.CompareBaseJob
import ai.chronon.spark.{SparkSessionBuilder, TableUtils}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.junit.Test

class CompareTest {
  lazy val spark: SparkSession = SparkSessionBuilder.build("CompareTest", local = true)

  private val tableUtils = TableUtils(spark)

  def toTs(arg: String): Long = TsUtils.datetimeToTs(arg)

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
  val rightColumns = Seq("rev_serial", "rev_value", "rev_rating", "keyId", "ts", "ds")

  @Test
  def basicTest(): Unit = {
    val leftRdd = spark.sparkContext.parallelize(leftData)
    val leftDf = spark.createDataFrame(leftRdd).toDF(leftColumns:_*)
    val rightRdd = spark.sparkContext.parallelize(rightData)
    val rightDf = spark.createDataFrame(rightRdd).toDF(leftColumns:_*)

    leftDf.show()
    rightDf.show()

    val keys = Seq("keyId", "ts", "ds")
    val (compareDf, metricsDf, result): (DataFrame, DataFrame, DataMetrics) = CompareBaseJob.compare(leftDf, rightDf, keys)
    metricsDf.show()
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

  @Test
  def mappingTest(): Unit = {
    val leftRdd = spark.sparkContext.parallelize(leftData)
    val leftDf = spark.createDataFrame(leftRdd).toDF(leftColumns:_*)
    val rightRdd = spark.sparkContext.parallelize(rightData)
    val rightDf = spark.createDataFrame(rightRdd).toDF(rightColumns:_*)

    leftDf.show()
    rightDf.show()

    val keys = Seq("keyId", "ts", "ds")
    val (compareDf, metricsDf, result) = CompareBaseJob.compare(
        leftDf,
        rightDf,
        keys,
        Map("serial" -> "rev_serial", "value" -> "rev_value", "rating" -> "rev_rating")
    )
    metricsDf.show()
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

  @Test
  def checkKeysTest(): Unit = {
    val leftRdd = spark.sparkContext.parallelize(leftData)
    val leftDf = spark.createDataFrame(leftRdd).toDF(leftColumns:_*)
    val rightRdd = spark.sparkContext.parallelize(rightData)
    val rightDf = spark.createDataFrame(rightRdd).toDF(rightColumns:_*)

    val keys1 = Seq("keyId", "ts", "ds", "nonexistantKey")
    val mapping1 = Map("serial" -> "rev_serial", "value" -> "rev_value", "rating" -> "rev_rating")
    runFailureScenario(leftDf, rightDf, keys1, mapping1)

    val keys2 = Seq("keyId")
    val mapping2 = Map("serial" -> "rev_serial", "value" -> "rev_value", "rating" -> "rev_rating")
    runFailureScenario(leftDf, rightDf, keys2, mapping2)
  }

  @Test
  def checkDataTypeTest(): Unit = {
    val leftData = Seq(
      (1, Some(1), 1.0, "a", toTs("2021-04-10 09:00:00"), "2021-04-10")
    )

    val rightData = Seq(
      (1, Some(1), "5.0", "a", toTs("2021-04-10 09:00:00"), "2021-04-10")
    )

    val leftColumns = Seq("serial", "value", "rating", "keyId", "ts", "ds")
    val rightColumns = Seq("rev_serial", "rev_value", "rev_rating", "keyId", "ts", "ds")

    val leftRdd = spark.sparkContext.parallelize(leftData)
    val leftDf = spark.createDataFrame(leftRdd).toDF(leftColumns:_*)
    val rightRdd = spark.sparkContext.parallelize(rightData)
    val rightDf = spark.createDataFrame(rightRdd).toDF(rightColumns:_*)

    val keys = Seq("keyId", "ds")
    val mapping = Map("serial" -> "rev_serial", "value" -> "rev_value", "rating" -> "rev_rating")
    runFailureScenario(leftDf, rightDf, keys, mapping)
  }

  @Test
  def checkForWrongColumnCount(): Unit = {
    val leftData = Seq(
      (1, Some(1), 1.0, "a", "2021-04-10")
    )

    val rightData = Seq(
      (1, Some(1), 5.0, "a", toTs("2021-04-10 09:00:00"), "2021-04-10")
    )

    val leftColumns = Seq("serial", "value", "rating", "keyId", "ds")
    val rightColumns = Seq("rev_serial", "rev_value", "rev_rating", "keyId", "ts", "ds")

    val leftRdd = spark.sparkContext.parallelize(leftData)
    val leftDf = spark.createDataFrame(leftRdd).toDF(leftColumns:_*)
    val rightRdd = spark.sparkContext.parallelize(rightData)
    val rightDf = spark.createDataFrame(rightRdd).toDF(rightColumns:_*)

    val keys = Seq("keyId", "ds")
    val mapping = Map("serial" -> "rev_serial", "value" -> "rev_value", "rating" -> "rev_rating")
    runFailureScenario(leftDf, rightDf, keys, mapping)
  }

  @Test
  def checkForMappingConsistency(): Unit = {
    val leftData = Seq(
      (1, Some(1), 1.0, "a", toTs("2021-04-10 09:00:00"), "2021-04-10")
    )

    val rightData = Seq(
      (1, Some(1), 5.0, "a", toTs("2021-04-10 09:00:00"), "2021-04-10")
    )

    val leftColumns = Seq("serial", "value", "rating", "keyId", "ts", "ds")
    val rightColumns = Seq("rev_serial", "rev_value", "rev_rating", "keyId", "ts", "ds")

    val leftRdd = spark.sparkContext.parallelize(leftData)
    val leftDf = spark.createDataFrame(leftRdd).toDF(leftColumns:_*)
    val rightRdd = spark.sparkContext.parallelize(rightData)
    val rightDf = spark.createDataFrame(rightRdd).toDF(rightColumns:_*)

    val keys = Seq("keyId", "ds")
    val wrongMapping1 = Map("non_existant_key" -> "rev_serial", "value" -> "rev_value", "rating" -> "rev_rating")
    runFailureScenario(leftDf, rightDf, keys, wrongMapping1)

    val wrongMapping2 = Map("serial" -> "non_existant_key", "value" -> "rev_value", "rating" -> "rev_rating")
    runFailureScenario(leftDf, rightDf, keys, wrongMapping2)
  }

  def runFailureScenario(
      leftDf: DataFrame,
      rightDf: DataFrame,
      keys: Seq[String],
      mapping: Map[String, String]): Unit = {
    leftDf.show()
    rightDf.show()
    try {
      CompareBaseJob.compare(
        leftDf,
        rightDf,
        keys,
        mapping
      )
      throw new AssertionError("Expecting an error")
    } catch {
      case e: AssertionError => true
      case ex: Throwable => throw ex
    }
  }
}
