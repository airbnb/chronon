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
import ai.chronon.api.Extensions._
import ai.chronon.api.{Builders, _}
import ai.chronon.online.DataMetrics
import ai.chronon.spark.Extensions._
import ai.chronon.spark.stats.CompareJob
import ai.chronon.spark.{Join, SparkSessionBuilder, StagingQuery, TableUtils}
import org.apache.spark.sql.SparkSession
import org.junit.Test

class MigrationCompareTest {
  lazy val spark: SparkSession = SparkSessionBuilder.build("MigrationCompareTest", local = true)
  private val tableUtils = TableUtils(spark)
  private val today = tableUtils.partitionSpec.at(System.currentTimeMillis())
  private val ninetyDaysAgo = tableUtils.partitionSpec.minus(today, new Window(90, TimeUnit.DAYS))
  private val namespace = "migration_compare_chronon_test"
  private val monthAgo = tableUtils.partitionSpec.minus(today, new Window(30, TimeUnit.DAYS))
  private val yearAgo = tableUtils.partitionSpec.minus(today, new Window(365, TimeUnit.DAYS))
  tableUtils.createDatabase(namespace)

  def setupTestData(): (api.Join, api.StagingQuery) = {
    // ------------------------------------------JOIN------------------------------------------
    val viewsSchema = List(
      Column("user", api.StringType, 10000),
      Column("item", api.StringType, 100),
      Column("time_spent_ms", api.LongType, 5000)
    )

    val viewsTable = s"$namespace.view_events"
    DataFrameGen.events(spark, viewsSchema, count = 1000, partitions = 200).drop("ts").save(viewsTable)

    val viewsSource = Builders.Source.events(
      query = Builders.Query(selects = Builders.Selects("time_spent_ms"), startPartition = yearAgo),
      table = viewsTable
    )

    val viewsGroupBy = Builders.GroupBy(
      sources = Seq(viewsSource),
      keyColumns = Seq("item"),
      aggregations = Seq(
        Builders.Aggregation(operation = Operation.AVERAGE, inputColumn = "time_spent_ms"),
        Builders.Aggregation(operation = Operation.COUNT, inputColumn = "time_spent_ms")
      ),
      metaData = Builders.MetaData(name = "unit_test.item_views", namespace = namespace),
      accuracy = Accuracy.SNAPSHOT
    )

    val itemQueries = List(Column("item", api.StringType, 100))
    val itemQueriesTable = s"$namespace.item_queries"
    DataFrameGen
      .events(spark, itemQueries, 1000, partitions = 100)
      .save(itemQueriesTable)

    val joinConf = Builders.Join(
      left = Builders.Source.events(Builders.Query(startPartition = ninetyDaysAgo), table = itemQueriesTable),
      joinParts = Seq(Builders.JoinPart(groupBy = viewsGroupBy, prefix = "user")),
      metaData = Builders.MetaData(name = "test.item_snapshot_features_2", namespace = namespace, team = "chronon")
    )

    val join = new Join(joinConf = joinConf, endPartition = today, tableUtils)
    join.computeJoin()

    //--------------------------------Staging Query-----------------------------
    val stagingQueryConf = Builders.StagingQuery(
      query = s"select * from ${joinConf.metaData.outputTable} WHERE ds BETWEEN '{{ start_date }}' AND '{{ end_date }}'",
      startPartition = ninetyDaysAgo,
      metaData = Builders.MetaData(name = "test.item_snapshot_features_sq_3",
        namespace = namespace,
        tableProperties = Map("key" -> "val"))
    )

    (joinConf, stagingQueryConf)
  }

  @Test
  def testMigrateCompare(): Unit = {
    val (joinConf, stagingQueryConf) = setupTestData()

    val (compareDf, metricsDf, metrics: DataMetrics) =
      new CompareJob(tableUtils, joinConf, stagingQueryConf, ninetyDaysAgo, today).run()
    val result = CompareJob.printAndGetBasicMetrics(metrics, tableUtils.partitionSpec)
    assert(result.size == 0)
  }

  @Test
  def testMigrateCompareWithLessColumns(): Unit = {
    val (joinConf, _) = setupTestData()

    // Run the staging query to generate the corresponding table for comparison
    val stagingQueryConf = Builders.StagingQuery(
      query = s"select item, ts, ds from ${joinConf.metaData.outputTable}",
      startPartition = ninetyDaysAgo,
      metaData = Builders.MetaData(name = "test.item_snapshot_features_sq_4",
        namespace = namespace,
        tableProperties = Map("key" -> "val"))
    )

    val (compareDf, metricsDf, metrics: DataMetrics) =
      new CompareJob(tableUtils, joinConf, stagingQueryConf, ninetyDaysAgo, today).run()
    val result = CompareJob.printAndGetBasicMetrics(metrics, tableUtils.partitionSpec)
    assert(result.size == 0)
  }

  @Test
  def testMigrateCompareWithWindows(): Unit = {
    val (joinConf, stagingQueryConf) = setupTestData()

    val (compareDf, metricsDf, metrics: DataMetrics) =
      new CompareJob(tableUtils, joinConf, stagingQueryConf, ninetyDaysAgo, today).run()
    val result = CompareJob.printAndGetBasicMetrics(metrics, tableUtils.partitionSpec)
    assert(result.size == 0)
  }

  @Test
  def testMigrateCompareWithLessData(): Unit = {
    val (joinConf, _) = setupTestData()

    val stagingQueryConf = Builders.StagingQuery(
      query = s"select * from ${joinConf.metaData.outputTable} where ds BETWEEN '${monthAgo}' AND '${today}'",
      startPartition = ninetyDaysAgo,
      metaData = Builders.MetaData(name = "test.item_snapshot_features_sq_5",
        namespace = namespace,
        tableProperties = Map("key" -> "val"))
    )

    val (compareDf, metricsDf, metrics: DataMetrics) =
      new CompareJob(tableUtils, joinConf, stagingQueryConf, ninetyDaysAgo, today).run()

    val result = CompareJob.printAndGetBasicMetrics(metrics, tableUtils.partitionSpec)
    assert(result.size != 0)
  }
}
