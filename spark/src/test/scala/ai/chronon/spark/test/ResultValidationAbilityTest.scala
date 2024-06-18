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

import ai.chronon.api.Extensions.{WindowOps, WindowUtils}
import ai.chronon.api.PartitionSpec
import ai.chronon.spark.Driver.{OfflineSubcommand, ResultValidationAbility}
import ai.chronon.spark.{SparkSessionBuilder, TableUtils}
import org.apache.spark.sql.SparkSession
import org.junit.Assert.{assertFalse, assertTrue}
import org.junit.{Before, Test}
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.{mock, when}
import org.rogach.scallop.ScallopConf

class ResultValidationAbilityTest {
  val confPath = "joins/team/example_join.v1"
  val spark: SparkSession = SparkSessionBuilder.build("test", local = true)
  val mockTableUtils: TableUtils = mock(classOf[TableUtils])

  @Before
  def setup(): Unit = {
    when(mockTableUtils.partitionColumn).thenReturn("ds")
    when(mockTableUtils.partitionSpec).thenReturn(PartitionSpec("yyyy-MM-dd", WindowUtils.Day.millis))
  }

  class TestArgs(args: Array[String]) extends ScallopConf(args) with OfflineSubcommand with ResultValidationAbility {
    verify()

    override def subcommandName: String = "test"
    override def buildSparkSession(): SparkSession = spark
  }

  @Test
  def shouldNotValidateWhenComparisonTableIsNotSpecified(): Unit = {
    val args = new TestArgs(Seq("--conf-path", confPath).toArray)
    assertFalse(args.shouldPerformValidate())
  }

  @Test
  def shouldValidateWhenComparisonTableIsSpecified(): Unit = {
    val args = new TestArgs(Seq("--conf-path", confPath, "--expected-result-table", "a_table").toArray)
    assertTrue(args.shouldPerformValidate())
  }

  @Test
  def testSuccessfulValidation(): Unit = {
    val args = new TestArgs(Seq("--conf-path", confPath, "--expected-result-table", "a_table").toArray)

    // simple testing, more comprehensive testing are already done in CompareTest.scala
    val leftData = Seq((1, Some(1), 1.0, "a", "2021-04-10"), (2, Some(2), 2.0, "b", "2021-04-10"))
    val columns = Seq("serial", "value", "rating", "keyId", "ds")
    val rdd = args.sparkSession.sparkContext.parallelize(leftData)
    val df = args.sparkSession.createDataFrame(rdd).toDF(columns: _*)

    when(mockTableUtils.loadEntireTable(any())).thenReturn(df)

    assertTrue(args.validateResult(df, Seq("keyId", "ds"), mockTableUtils))
  }

  @Test
  def testFailedValidation(): Unit = {
    val args = new TestArgs(Seq("--conf-path", confPath, "--expected-result-table", "a_table").toArray)

    val columns = Seq("serial", "value", "rating", "keyId", "ds")
    val leftData = Seq((1, Some(1), 1.0, "a", "2021-04-10"), (1, Some(2), 2.0, "b", "2021-04-10"))
    val leftRdd = args.sparkSession.sparkContext.parallelize(leftData)
    val leftDf = args.sparkSession.createDataFrame(leftRdd).toDF(columns: _*)
    val rightData = Seq((1, Some(1), 5.0, "a", "2021-04-10"), (2, Some(3), 2.0, "b", "2021-04-10"))
    val rightRdd = args.sparkSession.sparkContext.parallelize(rightData)
    val rightDf = args.sparkSession.createDataFrame(rightRdd).toDF(columns: _*)

    when(mockTableUtils.loadEntireTable(any())).thenReturn(rightDf)

    assertFalse(args.validateResult(leftDf, Seq("keyId", "ds"), mockTableUtils))
  }
}
