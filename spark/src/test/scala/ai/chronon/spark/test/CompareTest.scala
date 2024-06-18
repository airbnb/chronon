/*
 *    Copyright (C) 2023 The Chronon Authors.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package ai.chronon.spark.test

import org.slf4j.LoggerFactory
import ai.chronon.aggregator.windowing.TsUtils
import ai.chronon.online.DataMetrics
import ai.chronon.spark.stats.CompareBaseJob
import ai.chronon.spark.{SparkSessionBuilder, TableUtils, TimedKvRdd}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.junit.Test

class CompareTest {
  @transient lazy val logger = LoggerFactory.getLogger(getClass)
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
    val leftDf = spark.createDataFrame(leftRdd).toDF(leftColumns: _*)
    val rightRdd = spark.sparkContext.parallelize(rightData)
    val rightDf = spark.createDataFrame(rightRdd).toDF(leftColumns: _*)

    leftDf.show()
    rightDf.show()

    val keys = Seq("keyId", "ts", "ds")
    val (compareDf, metricsKvRdd, result): (DataFrame, TimedKvRdd, DataMetrics) =
      CompareBaseJob.compare(leftDf, rightDf, keys, tableUtils)
    val metricsDf = metricsKvRdd.toFlatDf
    metricsDf.show()
    logger.info(result.toString)
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
    val leftDf = spark.createDataFrame(leftRdd).toDF(leftColumns: _*)
    val rightRdd = spark.sparkContext.parallelize(rightData)
    val rightDf = spark.createDataFrame(rightRdd).toDF(rightColumns: _*)

    leftDf.show()
    rightDf.show()

    val keys = Seq("keyId", "ts", "ds")
    val (compareDf, metricsKvRdd, result) = CompareBaseJob.compare(
      leftDf,
      rightDf,
      keys,
      tableUtils,
      mapping = Map("serial" -> "rev_serial", "value" -> "rev_value", "rating" -> "rev_rating")
    )
    val metricsDf = metricsKvRdd.toFlatDf
    metricsDf.show()
    logger.info(result.toString)
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
    val leftDf = spark.createDataFrame(leftRdd).toDF(leftColumns: _*)
    val rightRdd = spark.sparkContext.parallelize(rightData)
    val rightDf = spark.createDataFrame(rightRdd).toDF(rightColumns: _*)

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
    val leftDf = spark.createDataFrame(leftRdd).toDF(leftColumns: _*)
    val rightRdd = spark.sparkContext.parallelize(rightData)
    val rightDf = spark.createDataFrame(rightRdd).toDF(rightColumns: _*)

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
    val leftDf = spark.createDataFrame(leftRdd).toDF(leftColumns: _*)
    val rightRdd = spark.sparkContext.parallelize(rightData)
    val rightDf = spark.createDataFrame(rightRdd).toDF(rightColumns: _*)

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
    val leftDf = spark.createDataFrame(leftRdd).toDF(leftColumns: _*)
    val rightRdd = spark.sparkContext.parallelize(rightData)
    val rightDf = spark.createDataFrame(rightRdd).toDF(rightColumns: _*)

    val keys = Seq("keyId", "ds")
    val wrongMapping1 = Map("non_existant_key" -> "rev_serial", "value" -> "rev_value", "rating" -> "rev_rating")
    runFailureScenario(leftDf, rightDf, keys, wrongMapping1)

    val wrongMapping2 = Map("serial" -> "non_existant_key", "value" -> "rev_value", "rating" -> "rev_rating")
    runFailureScenario(leftDf, rightDf, keys, wrongMapping2)
  }

  def runFailureScenario(leftDf: DataFrame,
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
        tableUtils,
        mapping = mapping
      )
      throw new AssertionError("Expecting an error")
    } catch {
      case e: AssertionError => true
      case ex: Throwable     => throw ex
    }
  }
}
