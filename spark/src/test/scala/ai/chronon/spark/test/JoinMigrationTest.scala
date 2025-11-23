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
import ai.chronon.api.{
  Accuracy,
  Builders,
  Constants,
  JoinPart,
  LongType,
  Operation,
  StringType,
  TimeUnit,
  Window
}
import ai.chronon.api.Extensions._
import ai.chronon.spark.Extensions._
import ai.chronon.spark.SemanticHashUtils.{tableHashesChanged, tablesToRecompute}
import ai.chronon.spark._
import ai.chronon.spark.catalog.TableUtils
import com.google.gson.Gson
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.junit.Assert._
import org.junit.Test

import scala.util.Random
import scala.util.ScalaJavaConversions.{JListOps, JMapOps, ListOps, MapOps}
import scala.util.Try

class JoinMigrationTest {
  val dummySpark: SparkSession = SparkSessionBuilder.build("JoinMigrationTest", local = true)
  private val dummyTableUtils = TableUtils(dummySpark)
  private val today = dummyTableUtils.partitionSpec.at(System.currentTimeMillis())
  private val monthAgo = dummyTableUtils.partitionSpec.minus(today, new Window(30, TimeUnit.DAYS))
  private val yearAgo = dummyTableUtils.partitionSpec.minus(today, new Window(365, TimeUnit.DAYS))
  private val dayAndMonthBefore = dummyTableUtils.partitionSpec.before(monthAgo)

  private def getViewsGroupBy(suffix: String, makeCumulative: Boolean = false, namespace: String) = {
    val viewsSchema = List(
      Column("user", api.StringType, 10000),
      Column("item", api.StringType, 100),
      Column("time_spent_ms", api.LongType, 5000)
    )
    val spark: SparkSession =
      SparkSessionBuilder.build("JoinMigrationTest" + "_" + Random.alphanumeric.take(6).mkString, local = true)
    val tableUtils = TableUtils(spark)
    val viewsTable = s"$namespace.view_$suffix"
    val df = DataFrameGen.events(spark, viewsSchema, count = 1000, partitions = 200)

    val viewsSource = Builders.Source.events(
      table = viewsTable,
      query = Builders.Query(selects = Builders.Selects("time_spent_ms"), startPartition = yearAgo),
      isCumulative = makeCumulative
    )

    val dfToWrite = if (makeCumulative) {
      // Move all events into latest partition and set isCumulative on thrift object
      df.drop("ds").withColumn("ds", lit(today))
    } else { df }

    spark.sql(s"DROP TABLE IF EXISTS $viewsTable")
    dfToWrite.save(viewsTable, Map("tblProp1" -> "1"))

    Builders.GroupBy(
      sources = Seq(viewsSource),
      keyColumns = Seq("item"),
      aggregations = Seq(
        Builders.Aggregation(operation = Operation.AVERAGE, inputColumn = "time_spent_ms"),
        Builders.Aggregation(operation = Operation.MIN, inputColumn = "ts"),
        Builders.Aggregation(operation = Operation.MAX, inputColumn = "ts")
      ),
      metaData = Builders.MetaData(name = "unit_test.item_views", namespace = namespace, team = "item_team"),
      accuracy = Accuracy.TEMPORAL
    )
  }

  private def getEventsEventsTemporal(nameSuffix: String = "", namespace: String) = {
    val spark: SparkSession =
      SparkSessionBuilder.build("JoinMigrationTest" + "_" + Random.alphanumeric.take(6).mkString, local = true)
    val tableUtils = TableUtils(spark)
    // left side
    val itemQueries = List(Column("item", api.StringType, 100))
    val itemQueriesTable = s"$namespace.item_queries"
    val itemQueriesDf = DataFrameGen
      .events(spark, itemQueries, 10000, partitions = 100)
    // duplicate the events
    itemQueriesDf.union(itemQueriesDf).save(itemQueriesTable)

    val start = tableUtils.partitionSpec.minus(today, new Window(100, TimeUnit.DAYS))
    val suffix = if (nameSuffix.isEmpty) "" else s"_$nameSuffix"
    Builders.Join(
      left = Builders.Source.events(Builders.Query(startPartition = start), table = itemQueriesTable),
      joinParts = Seq(Builders.JoinPart(groupBy = getViewsGroupBy(nameSuffix, namespace = namespace), prefix = "user")),
      metaData =
        Builders.MetaData(name = s"test.item_temporal_features${suffix}", namespace = namespace, team = "item_team")
    )
  }

  @Test
  def testVersioning(): Unit = {
    val spark: SparkSession =
      SparkSessionBuilder.build("JoinMigrationTest" + "_" + Random.alphanumeric.take(6).mkString, local = true)
    val tableUtils = TableUtils(spark)
    val namespace = "test_namespace_jointest" + "_" + Random.alphanumeric.take(6).mkString
    tableUtils.createDatabase(namespace)
    val joinConf = getEventsEventsTemporal("versioning", namespace)

    // Run the old join to ensure that tables exist
    val oldJoin = new Join(joinConf = joinConf, endPartition = dayAndMonthBefore, tableUtils)
    oldJoin.computeJoin(Some(100))

    // Make sure that there is no versioning-detected changes at this phase
    val joinPartsToRecomputeNoChange = tablesToRecompute(joinConf, joinConf.metaData.outputTable, tableUtils, false)
    assertEquals(joinPartsToRecomputeNoChange._1.size, 0)

    // First test changing the left side table - this should trigger a full recompute
    val leftChangeJoinConf = joinConf.deepCopy()
    leftChangeJoinConf.getLeft.getEvents.setTable("some_other_table_name")
    val leftChangeJoin = new Join(joinConf = leftChangeJoinConf, endPartition = dayAndMonthBefore, tableUtils)
    val leftChangeRecompute =
      tablesToRecompute(leftChangeJoinConf, leftChangeJoinConf.metaData.outputTable, tableUtils, false)
    println(leftChangeRecompute)
    assertEquals(leftChangeRecompute._1.size, 3)
    val partTable = s"${leftChangeJoinConf.metaData.outputTable}_user_unit_test_item_views"
    assertEquals(
      leftChangeRecompute._1.sorted,
      Seq(partTable, leftChangeJoinConf.metaData.bootstrapTable, leftChangeJoinConf.metaData.outputTable).sorted)
    assertTrue(leftChangeRecompute._2)

    // Test adding a joinPart
    val addPartJoinConf = joinConf.deepCopy()
    val existingJoinPart = addPartJoinConf.getJoinParts.get(0)
    val newJoinPart =
      Builders.JoinPart(groupBy = getViewsGroupBy(suffix = "versioning", namespace = namespace), prefix = "user_2")
    addPartJoinConf.setJoinParts(Seq(existingJoinPart, newJoinPart).toJava)
    val addPartJoin = new Join(joinConf = addPartJoinConf, endPartition = dayAndMonthBefore, tableUtils)
    val addPartRecompute = tablesToRecompute(addPartJoinConf, addPartJoinConf.metaData.outputTable, tableUtils, false)
    assertEquals(addPartRecompute._1.size, 1)
    assertEquals(addPartRecompute._1, Seq(addPartJoinConf.metaData.outputTable))
    assertTrue(addPartRecompute._2)
    // Compute to ensure that it works and to set the stage for the next assertion
    addPartJoin.computeJoin(Some(100))

    // Test modifying only one of two joinParts
    val rightModJoinConf = addPartJoinConf.deepCopy()
    rightModJoinConf.getJoinParts.get(1).setPrefix("user_3")
    val rightModJoin = new Join(joinConf = rightModJoinConf, endPartition = dayAndMonthBefore, tableUtils)
    val rightModRecompute =
      tablesToRecompute(rightModJoinConf, rightModJoinConf.metaData.outputTable, tableUtils, false)
    assertEquals(rightModRecompute._1.size, 2)
    val rightModPartTable = s"${addPartJoinConf.metaData.outputTable}_user_2_unit_test_item_views"
    assertEquals(rightModRecompute._1, Seq(rightModPartTable, addPartJoinConf.metaData.outputTable))
    assertTrue(rightModRecompute._2)
    // Modify both
    rightModJoinConf.getJoinParts.get(0).setPrefix("user_4")
    val rightModBothJoin = new Join(joinConf = rightModJoinConf, endPartition = dayAndMonthBefore, tableUtils)
    // Compute to ensure that it works
    val computed = rightModBothJoin.computeJoin(Some(100))

    // Now assert that the actual output is correct after all these runs
    computed.show()
    val itemQueriesTable = joinConf.getLeft.getEvents.table
    val start = joinConf.getLeft.getEvents.getQuery.getStartPartition
    val viewsTable = s"$namespace.view_versioning"

    val expected = tableUtils.sql(s"""
                                     |WITH
                                     |   queries AS (SELECT item, ts, ds from $itemQueriesTable where ds >= '$start' and ds <= '$dayAndMonthBefore')
                                     | SELECT queries.item, queries.ts, queries.ds, part.user_4_unit_test_item_views_ts_min, part.user_4_unit_test_item_views_ts_max, part.user_4_unit_test_item_views_time_spent_ms_average, part.user_3_unit_test_item_views_ts_min, part.user_3_unit_test_item_views_ts_max, part.user_3_unit_test_item_views_time_spent_ms_average
                                     | FROM (SELECT queries.item,
                                     |        queries.ts,
                                     |        queries.ds,
                                     |        MIN(IF(queries.ts > $viewsTable.ts, $viewsTable.ts, null)) as user_4_unit_test_item_views_ts_min,
                                     |        MAX(IF(queries.ts > $viewsTable.ts, $viewsTable.ts, null)) as user_4_unit_test_item_views_ts_max,
                                     |        AVG(IF(queries.ts > $viewsTable.ts, time_spent_ms, null)) as user_4_unit_test_item_views_time_spent_ms_average,
                                     |        MIN(IF(queries.ts > $viewsTable.ts, $viewsTable.ts, null)) as user_3_unit_test_item_views_ts_min,
                                     |        MAX(IF(queries.ts > $viewsTable.ts, $viewsTable.ts, null)) as user_3_unit_test_item_views_ts_max,
                                     |        AVG(IF(queries.ts > $viewsTable.ts, time_spent_ms, null)) as user_3_unit_test_item_views_time_spent_ms_average
                                     |     FROM queries left outer join $viewsTable
                                     |     ON queries.item = $viewsTable.item
                                     |     WHERE $viewsTable.ds >= '$yearAgo' AND $viewsTable.ds <= '$dayAndMonthBefore'
                                     |     GROUP BY queries.item, queries.ts, queries.ds) as part
                                     | JOIN queries
                                     | ON queries.item <=> part.item AND queries.ts <=> part.ts AND queries.ds <=> part.ds
                                     |""".stripMargin)
    if (logger.isDebugEnabled) {
      expected.show()
    }
    val diff = Comparison.sideBySide(expected, computed, List("item", "ts", "ds"))
    val diffCount = diff.count()
    val queriesBare =
      tableUtils.sql(s"SELECT item, ts, ds from $itemQueriesTable where ds >= '$start' and ds <= '$dayAndMonthBefore'")
    assertEquals(queriesBare.count(), computed.count())
    if (diffCount > 0) {
      logger.debug(s"Diff count: ${diffCount}")
      logger.debug(s"diff result rows")
      diff
        .replaceWithReadableTime(
          Seq("ts", "a_user_3_unit_test_item_views_ts_max", "b_user_3_unit_test_item_views_ts_max"),
          dropOriginal = true)
        .show()
    }
    assertEquals(0, diffCount)
  }

  @Test
  def testMigrationForBootstrap(): Unit = {
    val spark: SparkSession =
      SparkSessionBuilder.build("JoinMigrationTest" + "_" + Random.alphanumeric.take(6).mkString, local = true)
    val tableUtils = TableUtils(spark)
    val namespaceMigration = "test_namespace_jointest"
    tableUtils.createDatabase(namespaceMigration)

    // Left
    val itemQueriesTable = s"$namespaceMigration.item_queries"
    val ds = "2023-01-01"
    val leftStart = tableUtils.partitionSpec.minus(ds, new Window(100, TimeUnit.DAYS))
    val leftSource = Builders.Source.events(Builders.Query(startPartition = leftStart), table = itemQueriesTable)

    // Right
    val viewsTable = s"$namespaceMigration.view_events"
    val viewsSource = Builders.Source.events(
      table = viewsTable,
      query = Builders.Query(selects = Builders.Selects("time_spent_ms"),
                             startPartition = tableUtils.partitionSpec.minus(ds, new Window(200, TimeUnit.DAYS)))
    )
    val groupBy = Builders.GroupBy(
      sources = Seq(viewsSource),
      keyColumns = Seq("item"),
      aggregations = Seq(
        Builders.Aggregation(operation = Operation.AVERAGE, inputColumn = "time_spent_ms"),
        Builders.Aggregation(operation = Operation.MIN, inputColumn = "ts"),
        Builders.Aggregation(operation = Operation.MAX, inputColumn = "ts")
      ),
      metaData = Builders.MetaData(name = "unit_test.item_views", namespace = namespaceMigration, team = "chronon"),
      accuracy = Accuracy.TEMPORAL
    )

    // Join
    val join = Builders.Join(
      left = leftSource,
      joinParts = Seq(Builders.JoinPart(groupBy = groupBy, prefix = "user")),
      metaData = Builders.MetaData(name = s"test.join_migration", namespace = namespaceMigration, team = "chronon")
    )
    val newSemanticHash = join.semanticHash(excludeTopic = false)

    // test older versions before migration
    // older versions do not have the bootstrap hash, but should not trigger recompute if no bootstrap_parts
    val productionHashV1 = Map(
      "left_source" -> "vbQc07vaqm",
      s"${namespaceMigration}.test_join_migration_user_unit_test_item_views" -> "OLFBDTqwMX"
    )
    assertEquals(0, tableHashesChanged(productionHashV1, newSemanticHash, join).length)

    // test newer versions
    val productionHashV2 = productionHashV1 ++ Map(
      s"${namespaceMigration}.test_join_migration_bootstrap" -> "1B2M2Y8Asg"
    )
    assertEquals(0, tableHashesChanged(productionHashV2, newSemanticHash, join).length)
  }

  private def prepareTopicTestConfs(prefix: String): (api.Join, String) = {
    val spark: SparkSession =
      SparkSessionBuilder.build("JoinMigrationTest" + "_" + Random.alphanumeric.take(6).mkString, local = true)
    val tableUtils = TableUtils(spark)
    val namespace = "test_namespace_jointest" + "_" + Random.alphanumeric.take(6).mkString
    tableUtils.createDatabase(namespace)
    // left part
    val querySchema = Seq(Column("user", api.LongType, 100))
    val queryTable = s"$namespace.${prefix}_left_table"
    DataFrameGen
      .events(spark, querySchema, 400, partitions = 10)
      .where(col("user").isNotNull)
      .dropDuplicates("user")
      .save(queryTable)
    val querySource = Builders.Source.events(
      table = queryTable,
      query = Builders.Query(Builders.Selects("user"), timeColumn = "ts")
    )

    // right part
    val transactionSchema = Seq(
      Column("user", LongType, 100),
      Column("amount", LongType, 1000)
    )
    val transactionsTable = s"$namespace.${prefix}_transactions"
    DataFrameGen
      .events(spark, transactionSchema, 2000, partitions = 50)
      .where(col("user").isNotNull)
      .save(transactionsTable)

    val joinPart: JoinPart = Builders.JoinPart(groupBy = Builders.GroupBy(
      keyColumns = Seq("user"),
      sources = Seq(
        Builders.Source.events(
          query = Builders.Query(
            selects = Builders.Selects("amount"),
            timeColumn = "ts"
          ),
          table = transactionsTable,
          topic = "transactions_topic_v1"
        )),
      aggregations = Seq(
        Builders
          .Aggregation(operation = Operation.SUM, inputColumn = "amount", windows = Seq(new Window(3, TimeUnit.DAYS)))),
      accuracy = Accuracy.SNAPSHOT,
      metaData = Builders.MetaData(name = s"join_test.${prefix}_txn", namespace = namespace, team = "chronon")
    ))

    // join
    val join = Builders.Join(
      left = querySource,
      joinParts = Seq(joinPart),
      metaData = Builders.MetaData(name = s"unit_test.${prefix}_join", namespace = namespace, team = "chronon")
    )

    val endDs = tableUtils.partitions(queryTable).max
    (join, endDs)
  }

  private def overwriteWithOldSemanticHash(join: api.Join, gson: Gson): Unit = {
    // Compute and manually set the semantic_hash computed from using old logic
    val oldVersionSemanticHash = join.semanticHash(excludeTopic = false)
    val oldTableProperties = Map(
      Constants.SemanticHashKey -> gson.toJson(oldVersionSemanticHash.toJava),
      Constants.SemanticHashOptionsKey -> gson.toJson(
        Map(
          Constants.SemanticHashExcludeTopic -> "false"
        ).toJava)
    )
    dummyTableUtils.alterTableProperties(join.metaData.outputTable, oldTableProperties)
    dummyTableUtils.alterTableProperties(join.metaData.bootstrapTable, oldTableProperties)
  }

  private def hasExcludeTopicFlag(tableProps: Map[String, String], gson: Gson): Boolean = {
    val optionsString = tableProps(Constants.SemanticHashOptionsKey)
    val options = gson.fromJson(optionsString, classOf[java.util.HashMap[String, String]]).toScala
    options.get(Constants.SemanticHashExcludeTopic).contains("true")
  }

  @Test
  def testMigrationForTopicSuccess(): Unit = {
    val spark: SparkSession =
      SparkSessionBuilder.build("JoinMigrationTest" + "_" + Random.alphanumeric.take(6).mkString, local = true)
    val tableUtils = TableUtils(spark)
    val (join, endDs) = prepareTopicTestConfs("test_migration_for_topic_success")
    def runJob(join: api.Join, shiftDays: Int): Unit = {
      val deepCopy = join.deepCopy()
      val joinJob = new Join(deepCopy, tableUtils.partitionSpec.shift(endDs, shiftDays), tableUtils)
      joinJob.computeJoin()
    }
    runJob(join, -2)

    // Compute and manually set the semantic_hash computed from using old logic
    val gson = new Gson()
    overwriteWithOldSemanticHash(join, gson)

    // Compare semantic hash
    val (tablesChanged, autoArchive) =
      SemanticHashUtils.tablesToRecompute(join, join.metaData.outputTable, tableUtils, unsetSemanticHash = false)

    assertEquals(0, tablesChanged.length)
    assertEquals(false, autoArchive)

    val (shouldRecomputeLeft, autoArchiveLeft) =
      SemanticHashUtils.shouldRecomputeLeft(join, join.metaData.bootstrapTable, tableUtils, unsetSemanticHash = false)
    assertEquals(false, shouldRecomputeLeft)
    assertEquals(false, autoArchiveLeft)

    // Rerun job and update semantic_hash with new logic
    runJob(join, -1)

    val newVersionSemanticHash = join.semanticHash(excludeTopic = true)

    val tablePropsV1 = tableUtils.getTableProperties(join.metaData.outputTable).get
    assertTrue(hasExcludeTopicFlag(tablePropsV1, gson))
    assertEquals(gson.toJson(newVersionSemanticHash.toJava), tablePropsV1(Constants.SemanticHashKey))

    // Modify the topic and rerun
    val joinPartNew = join.joinParts.get(0).deepCopy()
    joinPartNew.groupBy.sources.toScala.head.getEvents.setTopic("transactions_topic_v2")
    val joinNew = join.deepCopy()
    joinNew.setJoinParts(Seq(joinPartNew).toJava)
    runJob(joinNew, 0)

    // Verify that the semantic hash has NOT changed
    val tablePropsV2 = tableUtils.getTableProperties(join.metaData.outputTable).get
    assertTrue(hasExcludeTopicFlag(tablePropsV2, gson))
    assertEquals(gson.toJson(newVersionSemanticHash.toJava), tablePropsV2(Constants.SemanticHashKey))
  }

  @Test
  def testMigrationForTopicManualArchive(): Unit = {
    val spark: SparkSession =
      SparkSessionBuilder.build("JoinMigrationTest" + "_" + Random.alphanumeric.take(6).mkString, local = true)
    val tableUtils = TableUtils(spark)
    val (join, endDs) = prepareTopicTestConfs("test_migration_for_topic_manual_archive")
    def runJob(join: api.Join, shiftDays: Int, unsetSemanticHash: Boolean = false): Unit = {
      val deepCopy = join.deepCopy()
      val joinJob = new Join(deepCopy,
                             tableUtils.partitionSpec.shift(endDs, shiftDays),
                             tableUtils,
                             unsetSemanticHash = unsetSemanticHash)
      joinJob.computeJoin()
    }
    runJob(join, -2)

    // Compute and manually set the semantic_hash computed from using old logic
    val gson = new Gson()
    overwriteWithOldSemanticHash(join, gson)

    // Make real semantic hash change to join_part
    val joinPartNew = join.getJoinParts.get(0).deepCopy()
    joinPartNew.getGroupBy.getSources.toScala.head.getEvents.setTopic("transactions_topic_v2")
    joinPartNew.getGroupBy.getAggregations.toScala.head.setWindows(Seq(new Window(7, TimeUnit.DAYS)).toJava)
    val joinNew = join.deepCopy()
    joinNew.setJoinParts(Seq(joinPartNew).toJava)

    // Rerun job and update semantic_hash with new logic
    // Expect that a failure is thrown to ask for manual archive
    val runJobTry = Try(runJob(joinNew, -1))
    assertTrue(runJobTry.isFailure)
    assertTrue(runJobTry.failed.get.isInstanceOf[SemanticHashException])

    // Explicitly unsetSemanticHash to rerun the job. Note: technically the correct behavior here
    // should be drop table and rerun. But this is to test the unsetSemanticHash flag.
    runJob(joinNew, 0, unsetSemanticHash = true)

    // Verify that semantic_hash has been updated
    val newVersionSemanticHash = join.semanticHash(excludeTopic = true)
    val tableProps = tableUtils.getTableProperties(join.metaData.outputTable).get
    assertTrue(hasExcludeTopicFlag(tableProps, gson))
    assertNotEquals(gson.toJson(newVersionSemanticHash.toJava), tableProps(Constants.SemanticHashKey))
  }
}
