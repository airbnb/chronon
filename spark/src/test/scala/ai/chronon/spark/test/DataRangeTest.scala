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

import ai.chronon.aggregator.test.Column
import ai.chronon.api
import ai.chronon.api.{Builders, Constants, QueryUtils, Source}
import ai.chronon.spark.{PartitionRange, SparkSessionBuilder, TableUtils}
import ai.chronon.spark.Extensions._
import org.apache.spark.sql.SparkSession
import org.junit.Assert.assertEquals
import org.junit.Test

class DataRangeTest {
  val spark: SparkSession = SparkSessionBuilder.build("DataRangeTest", local = true)
  private val tableUtils = TableUtils(spark)

  @Test
  def testGenScanQuery(): Unit = {
    val namespace = "date_range_test_namespace"
    spark.sql(s"CREATE DATABASE IF NOT EXISTS $namespace")
    val testTable = s"$namespace.test_gen_scan_query"
    val viewsSchema = List(
      Column("col_1", api.StringType, 1),
      Column("col_2", api.StringType, 1)
    )
    DataFrameGen
      .events(spark, viewsSchema, count = 1000, partitions = 200)
      .save(testTable, partitionColumns = Seq())
    val partitionRange: PartitionRange = PartitionRange("2024-03-01", "2024-04-01")(tableUtils)
    val source: Source = Builders.Source.events(
      query = Builders.Query(
        selects = Builders.Selects("col_1", "col_2"),
        wheres = Seq("col_1 = 'TEST'"),
        timeColumn = "ts"
      ),
      table = testTable
    )

    val (scanQuery, _) = partitionRange.scanQueryStringAndDf(
      source.getEvents.query,
      testTable,
      fillIfAbsent=Seq(Constants.TimeColumn -> Option(source.getEvents.query).map(_.timeColumn).orNull).toMap
    )

    val expected: String =
      """SELECT
        |  ts as `ts`,
        |  col_1 as `col_1`,
        |  col_2 as `col_2`
        |FROM date_range_test_namespace.test_gen_scan_query 
        |WHERE
        |  (ds >= '2024-03-01') AND (ds <= '2024-04-01') AND (col_1 = 'TEST')"""
    assertEquals(expected.stripMargin, scanQuery.stripMargin)
  }

  @Test
  def testGenScanQueryWithPartitionColumn(): Unit = {
    val namespace = "date_range_test_namespace_with_partition_column"
    spark.sql(s"CREATE DATABASE IF NOT EXISTS $namespace")
    val customPartitionCol = "custom_partition_date"
    val testTable = s"$namespace.test_gen_scan_query"
    val viewsSchema = List(
      Column("col_1", api.StringType, 1),
      Column("col_2", api.StringType, 1),
      Column(customPartitionCol, api.StringType, 1)
    )
    DataFrameGen
      .events(spark, viewsSchema, count = 1000, partitions = 200)
      .save(testTable, partitionColumns = Seq(customPartitionCol))
    val partitionRange: PartitionRange = PartitionRange("2024-03-01", "2024-04-01")(tableUtils)
    val source: Source = Builders.Source.events(
      query = Builders.Query(
        selects = Builders.Selects("col_1", "col_2"),
        wheres = Seq("col_1 = 'TEST'"),
        timeColumn = "ts",
        partitionColumn = customPartitionCol
      ),
      table = testTable
    )

    val (scanQuery, _) = partitionRange.scanQueryStringAndDf(
      source.getEvents.query,
      testTable,
      fillIfAbsent = Seq(Constants.TimeColumn -> Option(source.getEvents.query).map(_.timeColumn).orNull,
        customPartitionCol -> null).toMap,
      partitionColOpt = Some(customPartitionCol)
    )

    val expected: String =
      s"""SELECT
        |  ts as `ts`,
        |  `custom_partition_date`,
        |  col_1 as `col_1`,
        |  col_2 as `col_2`
        |FROM date_range_test_namespace_with_partition_column.test_gen_scan_query 
        |WHERE
        |  ($customPartitionCol >= '2024-03-01') AND ($customPartitionCol <= '2024-04-01') AND (col_1 = 'TEST')"""
    assertEquals(expected.stripMargin, scanQuery.stripMargin)
  }

  @Test
  def testIntersect(): Unit = {
    val range1 = PartitionRange(null, null)(tableUtils)
    val range2 = PartitionRange("2023-01-01", "2023-01-02")(tableUtils)
    assertEquals(range2, range1.intersect(range2))
  }

  @Test
  def testWhereClauses(): Unit = {
    val range = PartitionRange("2023-01-01", "2023-01-02")(tableUtils)

    val clauses = range.whereClauses("ds")

    assertEquals(Seq("ds >= '2023-01-01'",  "ds <= '2023-01-02'"), clauses)
  }

  @Test
  def testWhereClausesNullStart(): Unit = {
    val range = PartitionRange(null, "2023-01-02")(tableUtils)

    val clauses = range.whereClauses("ds")

    assertEquals(Seq("ds <= '2023-01-02'"), clauses)
  }

  @Test
  def testWhereClausesNullEnd(): Unit = {
    val range = PartitionRange("2023-01-01", null)(tableUtils)

    val clauses = range.whereClauses("ds")

    assertEquals(Seq("ds >= '2023-01-01'"), clauses)
  }
}
