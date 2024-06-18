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
import ai.chronon.aggregator.test.Column
import ai.chronon.api
import ai.chronon.api.{Accuracy, Builders, Operation, TimeUnit, Window}
import ai.chronon.spark.{Analyzer, Join, SparkSessionBuilder, TableUtils}
import ai.chronon.spark.Extensions._
import org.apache.spark.sql.SparkSession
import org.junit.Assert.assertTrue
import org.junit.Test

class AnalyzerTest {
  @transient lazy val logger = LoggerFactory.getLogger(getClass)
  val spark: SparkSession = SparkSessionBuilder.build("AnalyzerTest", local = true)
  private val tableUtils = TableUtils(spark)

  private val today = tableUtils.partitionSpec.at(System.currentTimeMillis())
  private val oneMonthAgo = tableUtils.partitionSpec.minus(today, new Window(30, TimeUnit.DAYS))
  private val oneYearAgo = tableUtils.partitionSpec.minus(today, new Window(365, TimeUnit.DAYS))

  private val namespace = "analyzer_test_ns"
  tableUtils.createDatabase(namespace)

  private val viewsSource = getTestEventSource()

  @Test
  def testJoinAnalyzerSchemaWithValidation(): Unit = {
    val viewsGroupBy = getViewsGroupBy("join_analyzer_test.item_gb", Operation.AVERAGE)
    val anotherViewsGroupBy = getViewsGroupBy("join_analyzer_test.another_item_gb", Operation.SUM)

    // left side
    val itemQueries = List(Column("item", api.StringType, 100))
    val itemQueriesTable = s"$namespace.item_queries_table"
    DataFrameGen
      .events(spark, itemQueries, 1000, partitions = 100)
      .save(itemQueriesTable)

    val start = tableUtils.partitionSpec.minus(today, new Window(100, TimeUnit.DAYS))

    val joinConf = Builders.Join(
      left = Builders.Source.events(Builders.Query(startPartition = start), table = itemQueriesTable),
      joinParts = Seq(
        Builders.JoinPart(groupBy = viewsGroupBy, prefix = "prefix_one", keyMapping = Map("item" -> "item_id")),
        Builders.JoinPart(groupBy = anotherViewsGroupBy, prefix = "prefix_two", keyMapping = Map("item" -> "item_id"))
      ),
      metaData =
        Builders.MetaData(name = "test_join_analyzer.item_snapshot_features", namespace = namespace, team = "chronon")
    )

    //run analyzer and validate output schema
    val analyzer = new Analyzer(tableUtils, joinConf, oneMonthAgo, today, enableHitter = true)
    val analyzerSchema = analyzer.analyzeJoin(joinConf)._1.map { case (k, v) => s"${k} => ${v}" }.toList.sorted
    val join = new Join(joinConf = joinConf, endPartition = oneMonthAgo, tableUtils)
    val computed = join.computeJoin()
    val expectedSchema = computed.schema.fields.map(field => s"${field.name} => ${field.dataType}").sorted
    logger.info("=== expected schema =====")
    logger.info(expectedSchema.mkString("\n"))

    assertTrue(expectedSchema sameElements analyzerSchema)
  }

  @Test(expected = classOf[java.lang.AssertionError])
  def testJoinAnalyzerValidationFailure(): Unit = {
    val viewsGroupBy = getViewsGroupBy("join_analyzer_test.item_gb", Operation.AVERAGE, source = getTestGBSource())
    val usersGroupBy = getUsersGroupBy("join_analyzer_test.user_gb", Operation.AVERAGE, source = getTestGBSource())

    // left side
    val itemQueries = List(Column("item", api.StringType, 100), Column("guest", api.StringType, 100))
    val itemQueriesTable = s"$namespace.item_queries_with_user_table"
    DataFrameGen
      .events(spark, itemQueries, 1000, partitions = 100)
      .save(itemQueriesTable)

    val start = tableUtils.partitionSpec.minus(today, new Window(10, TimeUnit.DAYS))

    val joinConf = Builders.Join(
      left = Builders.Source.events(Builders.Query(startPartition = start), table = itemQueriesTable),
      joinParts = Seq(
        Builders.JoinPart(groupBy = viewsGroupBy, prefix = "mismatch", keyMapping = Map("item" -> "item_id")),
        Builders.JoinPart(groupBy = usersGroupBy, prefix = "mismatch", keyMapping = Map("guest" -> "user"))
      ),
      metaData =
        Builders.MetaData(name = "test_join_analyzer.item_type_mismatch", namespace = namespace, team = "chronon")
    )

    //run analyzer and validate output schema
    val analyzer = new Analyzer(tableUtils, joinConf, oneMonthAgo, today, enableHitter = true)
    analyzer.analyzeJoin(joinConf, validationAssert = true)
  }

  @Test(expected = classOf[java.lang.AssertionError])
  def testJoinAnalyzerValidationDataAvailability(): Unit = {
    // left side
    val itemQueries = List(Column("item", api.StringType, 100), Column("guest", api.StringType, 100))
    val itemQueriesTable = s"$namespace.item_queries_with_user_table"
    DataFrameGen
      .events(spark, itemQueries, 500, partitions = 100)
      .save(itemQueriesTable)

    val start = tableUtils.partitionSpec.minus(today, new Window(90, TimeUnit.DAYS))

    val viewsGroupBy = Builders.GroupBy(
      sources = Seq(viewsSource),
      keyColumns = Seq("item_id"),
      aggregations = Seq(
        Builders.Aggregation( windows = Seq(new Window(365, TimeUnit.DAYS)), // greater than one year
          operation = Operation.AVERAGE,
          inputColumn = "time_spent_ms")
      ),
      metaData = Builders.MetaData(name = "join_analyzer_test.item_data_avail_gb", namespace = namespace),
      accuracy = Accuracy.SNAPSHOT
    )

    val joinConf = Builders.Join(
      left = Builders.Source.events(Builders.Query(startPartition = start), table = itemQueriesTable),
      joinParts = Seq(
        Builders.JoinPart(groupBy = viewsGroupBy, prefix = "validation", keyMapping = Map("item" -> "item_id"))
      ),
      metaData = Builders.MetaData(name = "test_join_analyzer.item_validation", namespace = namespace, team = "chronon")
    )

    //run analyzer and validate data availability
    val analyzer = new Analyzer(tableUtils, joinConf, oneMonthAgo, today, enableHitter = true)
    analyzer.analyzeJoin(joinConf, validationAssert = true)
  }

  def getTestGBSource(): api.Source = {
    val viewsSchema = List(
      Column("user", api.StringType, 10000),
      Column("item_id", api.IntType, 100), // type mismatch
      Column("time_spent_ms", api.LongType, 5000),
      Column("user_review", api.LongType, 5000)
    )

    val viewsTable = s"$namespace.view_events_gb_table_2"
    DataFrameGen.events(spark, viewsSchema, count = 1000, partitions = 200).drop("ts").save(viewsTable)

    Builders.Source.events(
      query = Builders.Query(selects = Builders.Selects("time_spent_ms", "user_review"), startPartition = oneYearAgo),
      table = viewsTable
    )
  }

  def getTestEventSource(): api.Source = {
    val viewsSchema = List(
      Column("user", api.StringType, 10000),
      Column("item_id", api.StringType, 100),
      Column("time_spent_ms", api.LongType, 5000)
    )

    val viewsTable = s"$namespace.view_events_gb_table"
    DataFrameGen.events(spark, viewsSchema, count = 1000, partitions = 200).drop("ts").save(viewsTable)

    Builders.Source.events(
      query = Builders.Query(selects = Builders.Selects("time_spent_ms"), startPartition = oneYearAgo),
      table = viewsTable
    )
  }

  def getViewsGroupBy(name: String, operation: Operation, source: api.Source = viewsSource): api.GroupBy = {
    Builders.GroupBy(
      sources = Seq(source),
      keyColumns = Seq("item_id"),
      aggregations = Seq(
        Builders.Aggregation(operation = operation, inputColumn = "time_spent_ms")
      ),
      metaData = Builders.MetaData(name = name, namespace = namespace),
      accuracy = Accuracy.SNAPSHOT
    )
  }

  def getUsersGroupBy(name: String, operation: Operation, source: api.Source = viewsSource): api.GroupBy = {
    Builders.GroupBy(
      sources = Seq(source),
      keyColumns = Seq("user"),
      aggregations = Seq(
        Builders.Aggregation(operation = operation, inputColumn = "user_review")
      ),
      metaData = Builders.MetaData(name = name, namespace = namespace),
      accuracy = Accuracy.SNAPSHOT
    )
  }
}
