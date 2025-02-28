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
import ai.chronon.api.StructField
import ai.chronon.api.Builders.Derivation
import ai.chronon.api.{Accuracy, Builders, Constants, JoinPart, LongType, Operation, PartitionSpec, StringType, TimeUnit, Window}
import ai.chronon.api.Extensions._
import ai.chronon.spark.Extensions._
import ai.chronon.spark.GroupBy.{logger, renderDataSourceQuery}
import ai.chronon.spark.SemanticHashUtils.{tableHashesChanged, tablesToRecompute}
import ai.chronon.spark._
import ai.chronon.spark.stats.SummaryJob
import com.google.gson.Gson
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StructType, LongType => SparkLongType, StringType => SparkStringType}
import org.apache.spark.sql.{AnalysisException, DataFrame, Row, SparkSession}
import org.junit.Assert._
import org.junit.Test
import org.scalatest.Assertions.intercept
import org.apache.spark.sql.types._
import org.apache.spark.sql.Row

import scala.collection.JavaConverters._
import scala.util.Random
import scala.util.ScalaJavaConversions.ListOps
import scala.util.Try

class JoinTest {
  val dummySpark: SparkSession = SparkSessionBuilder.build("JoinTest", local = true)
  private val dummyTableUtils = TableUtils(dummySpark)
  private val today = dummyTableUtils.partitionSpec.at(System.currentTimeMillis())
  private val monthAgo = dummyTableUtils.partitionSpec.minus(today, new Window(30, TimeUnit.DAYS))
  private val yearAgo = dummyTableUtils.partitionSpec.minus(today, new Window(365, TimeUnit.DAYS))
  private val dayAndMonthBefore = dummyTableUtils.partitionSpec.before(monthAgo)

  @Test
  def testEventsEntitiesSnapshot(): Unit = {
    val spark: SparkSession = SparkSessionBuilder.build("JoinTest" + "_" + Random.alphanumeric.take(6).mkString, local = true)
    val tableUtils = TableUtils(spark)
    val namespace = "test_namespace_jointest" + "_" + Random.alphanumeric.take(6).mkString
    tableUtils.createDatabase(namespace)
    val dollarTransactions = List(
      Column("user", StringType, 100),
      Column("user_name", api.StringType, 100),
      Column("ts", LongType, 200),
      Column("amount_dollars", LongType, 1000)
    )

    val rupeeTransactions = List(
      Column("user", StringType, 100),
      Column("user_name", api.StringType, 100),
      Column("ts", LongType, 200),
      Column("amount_rupees", LongType, 70000)
    )

    val dollarTable = s"$namespace.dollar_transactions"
    val rupeeTable = s"$namespace.rupee_transactions"
    spark.sql(s"DROP TABLE IF EXISTS $dollarTable")
    spark.sql(s"DROP TABLE IF EXISTS $rupeeTable")
    DataFrameGen.entities(spark, dollarTransactions, 3000, partitions = 200).save(dollarTable, Map("tblProp1" -> "1"))
    DataFrameGen.entities(spark, rupeeTransactions, 500, partitions = 80).save(rupeeTable)

    val dollarSource = Builders.Source.entities(
      query = Builders.Query(
        selects = Builders.Selects("ts", "amount_dollars", "user_name", "user"),
        startPartition = yearAgo,
        endPartition = dayAndMonthBefore,
        setups =
          Seq("create temporary function temp_replace_right_a as 'org.apache.hadoop.hive.ql.udf.UDFRegExpReplace'")
      ),
      snapshotTable = dollarTable
    )

    //println("Rupee Source start partition $month")
    val rupeeSource =
      Builders.Source.entities(
        query = Builders.Query(
          selects = Map("ts" -> "ts",
                        "amount_dollars" -> "CAST(amount_rupees/70 as long)",
                        "user_name" -> "user_name",
                        "user" -> "user"),
          startPartition = monthAgo,
          setups = Seq(
            "create temporary function temp_replace_right_b as 'org.apache.hadoop.hive.ql.udf.UDFRegExpReplace'",
            "create temporary function temp_replace_right_c as 'org.apache.hadoop.hive.ql.udf.UDFRegExpReplace'",
            "create temporary function temp_replace_right_c as 'org.apache.hadoop.hive.ql.udf.UDFRegExpReplace'"
          )
        ),
        snapshotTable = rupeeTable
      )

    val groupBy = Builders.GroupBy(
      sources = Seq(dollarSource, rupeeSource),
      keyColumns = Seq("user", "user_name"),
      aggregations = Seq(
        Builders.Aggregation(operation = Operation.SUM,
                             inputColumn = "amount_dollars",
                             windows = Seq(new Window(30, TimeUnit.DAYS)))),
      metaData = Builders.MetaData(name = "unit_test.user_transactions", namespace = namespace, team = "chronon")
    )
    val queriesSchema = List(
      Column("user_name", api.StringType, 100),
      Column("user", api.StringType, 100)
    )

    val queryTable = s"$namespace.queries"
    DataFrameGen
      .events(spark, queriesSchema, 3000, partitions = 180)
      .save(queryTable)

    val start = tableUtils.partitionSpec.minus(today, new Window(60, TimeUnit.DAYS))
    val end = tableUtils.partitionSpec.minus(today, new Window(30, TimeUnit.DAYS))
    val joinConf = Builders.Join(
      left = Builders.Source.events(
        query = Builders.Query(
          startPartition = start,
          setups = Seq(
            "create temporary function temp_replace_left as 'org.apache.hadoop.hive.ql.udf.UDFRegExpReplace'",
            "create temporary function temp_replace_right_c as 'org.apache.hadoop.hive.ql.udf.UDFRegExpReplace'"
          )
        ),
        table = queryTable
      ),
      joinParts =
        Seq(Builders.JoinPart(groupBy = groupBy, keyMapping = Map("user_name" -> "user", "user" -> "user_name"))),
      metaData = Builders.MetaData(name = "test.user_transaction_features", namespace = namespace, team = "chronon")
    )

    val runner1 = new Join(joinConf, tableUtils.partitionSpec.minus(today, new Window(40, TimeUnit.DAYS)), tableUtils)
    runner1.computeJoin()
    val dropStart = tableUtils.partitionSpec.minus(today, new Window(55, TimeUnit.DAYS))
    val dropEnd = tableUtils.partitionSpec.minus(today, new Window(45, TimeUnit.DAYS))
    tableUtils.dropPartitionRange(
      s"$namespace.test_user_transaction_features",
      dropStart,
      dropEnd
    )
    println(tableUtils.partitions(s"$namespace.test_user_transaction_features"))

    joinConf.joinParts.toScala
      .map(jp => joinConf.partOutputTable(jp))
      .foreach(tableUtils.dropPartitionRange(_, dropStart, dropEnd))

    def resetUDFs(): Unit = {
      Seq("temp_replace_left", "temp_replace_right_a", "temp_replace_right_b", "temp_replace_right_c")
        .foreach(function => tableUtils.sql(s"DROP TEMPORARY FUNCTION IF EXISTS $function"))
    }

    resetUDFs()
    val runner2 = new Join(joinConf, end, tableUtils)
    val computed = runner2.computeJoin(Some(3))
    println(s"join start = $start")

    val expectedQuery = s"""
                           |WITH
                           |   queries AS (
                           |     SELECT user_name,
                           |         user,
                           |         ts,
                           |         ds
                           |     from $queryTable
                           |     where user_name IS NOT null
                           |         AND user IS NOT NULL
                           |         AND ts IS NOT NULL
                           |         AND ds IS NOT NULL
                           |         AND ds >= '$start'
                           |         AND ds <= '$end'),
                           |   grouped_transactions AS (
                           |      SELECT user,
                           |             user_name,
                           |             ds,
                           |             SUM(IF(transactions.ts  >= (unix_timestamp(transactions.ds, 'yyyy-MM-dd') - (86400*(30-1))) * 1000, amount_dollars, null)) AS unit_test_user_transactions_amount_dollars_sum_30d,
                           |             SUM(amount_dollars) AS amount_dollars_sum
                           |      FROM
                           |         (SELECT user, user_name, ts, ds, CAST(amount_rupees/70 as long) as amount_dollars from $rupeeTable
                           |          WHERE ds >= '$monthAgo'
                           |          UNION
                           |          SELECT user, user_name, ts, ds, amount_dollars from $dollarTable
                           |          WHERE ds >= '$yearAgo' and ds <= '$dayAndMonthBefore') as transactions
                           |      WHERE unix_timestamp(ds, 'yyyy-MM-dd')*1000 + 86400*1000> ts
                           |        AND user IS NOT NULL
                           |        AND user_name IS NOT NULL
                           |        AND ds IS NOT NULL
                           |      GROUP BY user, user_name, ds)
                           | SELECT queries.user_name,
                           |        queries.user,
                           |        queries.ts,
                           |        queries.ds,
                           |        grouped_transactions.unit_test_user_transactions_amount_dollars_sum_30d
                           | FROM queries left outer join grouped_transactions
                           | ON queries.user_name = grouped_transactions.user
                           | AND queries.user = grouped_transactions.user_name
                           | AND from_unixtime(queries.ts/1000, 'yyyy-MM-dd') = date_add(grouped_transactions.ds, 1)
                           | WHERE queries.user_name IS NOT NULL AND queries.user IS NOT NULL
                           |""".stripMargin
    val expected = spark.sql(expectedQuery)
    val queries = tableUtils.sql(
      s"SELECT user_name, user, ts, ds from $queryTable where user IS NOT NULL AND user_name IS NOT null AND ts IS NOT NULL AND ds IS NOT NULL AND ds >= '$start' AND ds <= '$end'")
    val diff = Comparison.sideBySide(computed, expected, List("user_name", "user", "ts", "ds"))

    if (diff.count() > 0) {
      println(s"Actual count: ${computed.count()}")
      println(s"Expected count: ${expected.count()}")
      println(s"Diff count: ${diff.count()}")
      println(s"Queries count: ${queries.count()}")
      println(s"diff result rows")
      diff.show()
    }
    assertEquals(0, diff.count())

    // test join part table hole detection:
    // in events<>entities<>snapshot case, join part table partitions and left partitions are offset by 1
    //
    // drop only end-1 from join output table, and only end-2 from join part table. trying to trick the job into
    // thinking that end-1 is already computed for the join part, since end-1 already exists in join part table
    val endMinus1 = tableUtils.partitionSpec.minus(end, new Window(1, TimeUnit.DAYS))
    val endMinus2 = tableUtils.partitionSpec.minus(end, new Window(2, TimeUnit.DAYS))

    tableUtils.dropPartitionRange(s"$namespace.test_user_transaction_features", endMinus1, endMinus1)
    println(tableUtils.partitions(s"$namespace.test_user_transaction_features"))

    joinConf.joinParts.asScala
      .map(jp => joinConf.partOutputTable(jp))
      .foreach(tableUtils.dropPartitionRange(_, endMinus2, endMinus2))

    resetUDFs()
    val runner3 = new Join(joinConf, end, tableUtils)

    val expected2 = spark.sql(expectedQuery)
    val computed2 = runner3.computeJoin(Some(3))
    val diff2 = Comparison.sideBySide(computed2, expected2, List("user_name", "user", "ts", "ds"))

    if (diff2.count() > 0) {
      println(s"Actual count: ${computed2.count()}")
      println(s"Expected count: ${expected2.count()}")
      println(s"Diff count: ${diff2.count()}")
      println(s"Queries count: ${queries.count()}")
      println(s"diff result rows")
      diff2.show()
    }
    assertEquals(0, diff2.count())
  }

  @Test
  def testEntitiesEntities(): Unit = {
    val spark: SparkSession = SparkSessionBuilder.build("JoinTest" + "_" + Random.alphanumeric.take(6).mkString, local = true)
    val tableUtils = TableUtils(spark)
    val namespace = "test_namespace_jointest" + "_" + Random.alphanumeric.take(6).mkString
    tableUtils.createDatabase(namespace)
    // untimed/unwindowed entities on right
    // right side
    val weightSchema = List(
      Column("user", api.StringType, 1000),
      Column("country", api.StringType, 100),
      Column("weight", api.DoubleType, 500)
    )
    val weightTable = s"$namespace.weights"
    DataFrameGen.entities(spark, weightSchema, 1000, partitions = 400).save(weightTable)

    val weightSource = Builders.Source.entities(
      query = Builders.Query(selects = Builders.Selects("weight"),
                             startPartition = yearAgo,
                             endPartition = dayAndMonthBefore),
      snapshotTable = weightTable
    )

    val weightGroupBy = Builders.GroupBy(
      sources = Seq(weightSource),
      keyColumns = Seq("country"),
      aggregations = Seq(Builders.Aggregation(operation = Operation.AVERAGE, inputColumn = "weight")),
      metaData = Builders.MetaData(name = "unit_test.country_weights", namespace = namespace)
    )

    val heightSchema = List(
      Column("user", api.StringType, 1000),
      Column("country", api.StringType, 100),
      Column("height", api.LongType, 200)
    )
    val heightTable = s"$namespace.heights"
    DataFrameGen.entities(spark, heightSchema, 1000, partitions = 400).save(heightTable)
    val heightSource = Builders.Source.entities(
      query = Builders.Query(selects = Builders.Selects("height"), startPartition = monthAgo),
      snapshotTable = heightTable
    )

    val heightGroupBy = Builders.GroupBy(
      sources = Seq(heightSource),
      keyColumns = Seq("country"),
      aggregations = Seq(Builders.Aggregation(operation = Operation.AVERAGE, inputColumn = "height")),
      metaData = Builders.MetaData(name = "unit_test.country_heights", namespace = namespace)
    )

    // left side
    val countrySchema = List(Column("country", api.StringType, 100))
    val countryTable = s"$namespace.countries"
    DataFrameGen.entities(spark, countrySchema, 1000, partitions = 400).save(countryTable)

    val start = tableUtils.partitionSpec.minus(today, new Window(60, TimeUnit.DAYS))
    val end = tableUtils.partitionSpec.minus(today, new Window(15, TimeUnit.DAYS))
    val joinConf = Builders.Join(
      left = Builders.Source.entities(Builders.Query(startPartition = start), snapshotTable = countryTable),
      joinParts = Seq(Builders.JoinPart(groupBy = weightGroupBy), Builders.JoinPart(groupBy = heightGroupBy)),
      metaData = Builders.MetaData(name = "test.country_features", namespace = namespace, team = "chronon")
    )

    val runner = new Join(joinConf, end, tableUtils)
    val computed = runner.computeJoin(Some(7))
    val expected = tableUtils.sql(s"""
                                     |WITH
                                     |   countries AS (SELECT country, ds from $countryTable where ds >= '$start' and ds <= '$end'),
                                     |   grouped_weights AS (
                                     |      SELECT country,
                                     |             ds,
                                     |             avg(weight) as unit_test_country_weights_weight_average
                                     |      FROM $weightTable
                                     |      WHERE ds >= '$yearAgo' and ds <= '$dayAndMonthBefore'
                                     |      GROUP BY country, ds),
                                     |   grouped_heights AS (
                                     |      SELECT country,
                                     |             ds,
                                     |             avg(height) as unit_test_country_heights_height_average
                                     |      FROM $heightTable
                                     |      WHERE ds >= '$monthAgo'
                                     |      GROUP BY country, ds)
                                     |   SELECT countries.country,
                                     |        countries.ds,
                                     |        grouped_weights.unit_test_country_weights_weight_average,
                                     |        grouped_heights.unit_test_country_heights_height_average
                                     | FROM countries left outer join grouped_weights
                                     | ON countries.country = grouped_weights.country
                                     | AND countries.ds = grouped_weights.ds
                                     | left outer join grouped_heights
                                     | ON countries.ds = grouped_heights.ds
                                     | AND countries.country = grouped_heights.country
    """.stripMargin)

    println("showing join result")
    computed.show()
    println("showing query result")
    expected.show()
    println(
      s"Left side count: ${spark.sql(s"SELECT country, ds from $countryTable where ds >= '$start' and ds <= '$end'").count()}")
    println(s"Actual count: ${computed.count()}")
    println(s"Expected count: ${expected.count()}")
    val diff = Comparison.sideBySide(computed, expected, List("country", "ds"))
    if (diff.count() > 0) {
      println(s"Diff count: ${diff.count()}")
      println(s"diff result rows")
      diff.show()
    }
    assertEquals(diff.count(), 0)
    /* the below testing case to cover the scenario when input table and output table
     * have same partitions, in other words, the frontfill is done, the join job
     * should not trigger a backfill and exit the program properly
     * TODO: Revisit this in a logger world.
    // use console to redirect println message to Java IO
    val stream = new java.io.ByteArrayOutputStream()
    Console.withOut(stream) {
      // rerun the same join job
      runner.computeJoin(Some(7))
    }
    val stdOutMsg = stream.toString()
    println(s"std out message =\n $stdOutMsg")
    // make sure that the program exits with target print statements
    assertTrue(stdOutMsg.contains(s"There is no data to compute based on end partition of $end."))
     */
  }

  @Test
  def testEntitiesEntitiesNoHistoricalBackfill(): Unit = {
    val spark: SparkSession = SparkSessionBuilder.build("JoinTest" + "_" + Random.alphanumeric.take(6).mkString, local = true)
    val tableUtils = TableUtils(spark)
    val namespace = "test_namespace_jointest" + "_" + Random.alphanumeric.take(6).mkString
    tableUtils.createDatabase(namespace)
    // Only backfill latest partition if historical_backfill is turned off
    val weightSchema = List(
      Column("user", api.StringType, 1000),
      Column("country", api.StringType, 100),
      Column("weight", api.DoubleType, 500)
    )
    val weightTable = s"$namespace.weights_no_historical_backfill"
    DataFrameGen.entities(spark, weightSchema, 1000, partitions = 400).save(weightTable)

    val weightSource = Builders.Source.entities(
      query = Builders.Query(selects = Builders.Selects("weight"), startPartition = yearAgo, endPartition = today),
      snapshotTable = weightTable
    )

    val weightGroupBy = Builders.GroupBy(
      sources = Seq(weightSource),
      keyColumns = Seq("country"),
      aggregations = Seq(Builders.Aggregation(operation = Operation.AVERAGE, inputColumn = "weight")),
      metaData = Builders.MetaData(name = "test.country_weights_no_backfill", namespace = namespace)
    )

    // left side
    val countrySchema = List(Column("country", api.StringType, 100))
    val countryTable = s"$namespace.countries_no_historical_backfill"
    DataFrameGen.entities(spark, countrySchema, 1000, partitions = 30).save(countryTable)

    val start = tableUtils.partitionSpec.minus(today, new Window(30, TimeUnit.DAYS))
    val end = tableUtils.partitionSpec.minus(today, new Window(5, TimeUnit.DAYS))
    val joinConf = Builders.Join(
      left = Builders.Source.entities(Builders.Query(startPartition = start), snapshotTable = countryTable),
      joinParts = Seq(Builders.JoinPart(groupBy = weightGroupBy)),
      metaData = Builders.MetaData(name = "test.country_no_historical_backfill",
                                   namespace = namespace,
                                   team = "chronon",
                                   historicalBackill = false)
    )

    val runner = new Join(joinConf, end, tableUtils)
    val computed = runner.computeJoin(Some(7))
    println("showing join result")
    computed.show()

    val leftSideCount = spark.sql(s"SELECT country, ds from $countryTable where ds == '$end'").count()
    println(s"Left side expected count: $leftSideCount")
    println(s"Actual count: ${computed.count()}")
    assertEquals(leftSideCount, computed.count())
    // There should be only one partition in computed df which equals to end partition
    val allPartitions = computed.select("ds").rdd.map(row => row(0)).collect().toSet
    assert(allPartitions.size == 1)
    assertEquals(allPartitions.toList(0), end)
  }

  @Test
  def testEventsEventsSnapshot(): Unit = {
    val spark: SparkSession = SparkSessionBuilder.build("JoinTest" + "_" + Random.alphanumeric.take(6).mkString, local = true)
    val tableUtils = TableUtils(spark)
    val namespace = "test_namespace_jointest" + "_" + Random.alphanumeric.take(6).mkString
    tableUtils.createDatabase(namespace)
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
        Builders.Aggregation(operation = Operation.AVERAGE, inputColumn = "time_spent_ms")
      ),
      metaData = Builders.MetaData(name = "unit_test.item_views", namespace = namespace),
      accuracy = Accuracy.SNAPSHOT
    )

    // left side
    val itemQueries = List(Column("item", api.StringType, 100))
    val itemQueriesTable = s"$namespace.item_queries"
    DataFrameGen
      .events(spark, itemQueries, 1000, partitions = 100)
      .save(itemQueriesTable)

    val start = tableUtils.partitionSpec.minus(today, new Window(100, TimeUnit.DAYS))

    val joinConf = Builders.Join(
      left = Builders.Source.events(Builders.Query(startPartition = start), table = itemQueriesTable),
      joinParts = Seq(Builders.JoinPart(groupBy = viewsGroupBy, prefix = "user")),
      metaData = Builders.MetaData(name = "test.item_snapshot_features_2", namespace = namespace, team = "chronon")
    )

    (new Analyzer(tableUtils, joinConf, monthAgo, today)).run()
    val join = new Join(joinConf = joinConf, endPartition = monthAgo, tableUtils)
    val computed = join.computeJoin()
    computed.show()

    val expected = tableUtils.sql(s"""
                                     |WITH
                                     |   queries AS (SELECT item, ts, ds from $itemQueriesTable where ds >= '$start' and ds <= '$monthAgo')
                                     | SELECT queries.item,
                                     |        queries.ts,
                                     |        queries.ds,
                                     |        AVG(IF(queries.ds > $viewsTable.ds, time_spent_ms, null)) as user_unit_test_item_views_time_spent_ms_average
                                     | FROM queries left outer join $viewsTable
                                     |  ON queries.item = $viewsTable.item
                                     | WHERE ($viewsTable.item IS NOT NULL) AND $viewsTable.ds >= '$yearAgo' AND $viewsTable.ds <= '$dayAndMonthBefore'
                                     | GROUP BY queries.item, queries.ts, queries.ds, from_unixtime(queries.ts/1000, 'yyyy-MM-dd')
                                     |""".stripMargin)
    expected.show()

    val diff = Comparison.sideBySide(computed, expected, List("item", "ts", "ds"))

    if (diff.count() > 0) {
      println(s"Diff count: ${diff.count()}")
      println(s"diff result rows")
      diff.show()
    }
    assertEquals(diff.count(), 0)
  }

  @Test
  def testEventsEventsTemporal(): Unit = {
    val spark: SparkSession = SparkSessionBuilder.build("JoinTest" + "_" + Random.alphanumeric.take(6).mkString, local = true)
    val tableUtils = TableUtils(spark)
    val namespace = "test_namespace_jointest" + "_" + Random.alphanumeric.take(6).mkString
    tableUtils.createDatabase(namespace)
    val joinConf = getEventsEventsTemporal("temporal", namespace)
    val viewsSchema = List(
      Column("user", api.StringType, 10000),
      Column("item", api.StringType, 100),
      Column("time_spent_ms", api.LongType, 5000)
    )

    val viewsTable = s"$namespace.view_temporal"
    DataFrameGen.events(spark, viewsSchema, count = 1000, partitions = 200).save(viewsTable, Map("tblProp1" -> "1"))

    val viewsSource = Builders.Source.events(
      table = viewsTable,
      query = Builders.Query(selects = Builders.Selects("time_spent_ms"), startPartition = yearAgo)
    )
    val viewsGroupBy = Builders.GroupBy(
      sources = Seq(viewsSource),
      keyColumns = Seq("item"),
      aggregations = Seq(
        Builders.Aggregation(operation = Operation.AVERAGE, inputColumn = "time_spent_ms"),
        Builders.Aggregation(operation = Operation.MIN, inputColumn = "ts"),
        Builders.Aggregation(operation = Operation.MAX, inputColumn = "ts")
        // Builders.Aggregation(operation = Operation.APPROX_UNIQUE_COUNT, inputColumn = "ts")
        // sql - APPROX_COUNT_DISTINCT(IF(queries.ts > $viewsTable.ts, time_spent_ms, null)) as user_ts_approx_unique_count
      ),
      metaData = Builders.MetaData(name = "unit_test.item_views", namespace = namespace)
    )

    // left side
    val itemQueries = List(Column("item", api.StringType, 100))
    val itemQueriesTable = s"$namespace.item_queries"
    val itemQueriesDf = DataFrameGen
      .events(spark, itemQueries, 1000, partitions = 100)
    // duplicate the events
    itemQueriesDf.union(itemQueriesDf).save(itemQueriesTable) //.union(itemQueriesDf)

    val start = tableUtils.partitionSpec.minus(today, new Window(100, TimeUnit.DAYS))
    (new Analyzer(tableUtils, joinConf, monthAgo, today)).run()
    val join = new Join(joinConf = joinConf, endPartition = dayAndMonthBefore, tableUtils)
    val computed = join.computeJoin(Some(100))
    computed.show()

    val expected = tableUtils.sql(s"""
                                     |WITH
                                     |   queries AS (SELECT item, ts, ds from $itemQueriesTable where ds >= '$start' and ds <= '$dayAndMonthBefore')
                                     | SELECT queries.item, queries.ts, queries.ds, part.user_unit_test_item_views_ts_min, part.user_unit_test_item_views_ts_max, part.user_unit_test_item_views_time_spent_ms_average
                                     | FROM (SELECT queries.item,
                                     |        queries.ts,
                                     |        queries.ds,
                                     |        MIN(IF(queries.ts > $viewsTable.ts, $viewsTable.ts, null)) as user_unit_test_item_views_ts_min,
                                     |        MAX(IF(queries.ts > $viewsTable.ts, $viewsTable.ts, null)) as user_unit_test_item_views_ts_max,
                                     |        AVG(IF(queries.ts > $viewsTable.ts, time_spent_ms, null)) as user_unit_test_item_views_time_spent_ms_average
                                     |     FROM queries left outer join $viewsTable
                                     |     ON queries.item = $viewsTable.item
                                     |     WHERE $viewsTable.item IS NOT NULL AND $viewsTable.ds >= '$yearAgo' AND $viewsTable.ds <= '$dayAndMonthBefore'
                                     |     GROUP BY queries.item, queries.ts, queries.ds) as part
                                     | JOIN queries
                                     | ON queries.item <=> part.item AND queries.ts <=> part.ts AND queries.ds <=> part.ds
                                     |""".stripMargin)
    expected.show()

    val diff = Comparison.sideBySide(computed, expected, List("item", "ts", "ds"))
    val queriesBare =
      tableUtils.sql(s"SELECT item, ts, ds from $itemQueriesTable where ds >= '$start' and ds <= '$dayAndMonthBefore'")
    assertEquals(queriesBare.count(), computed.count())
    if (diff.count() > 0) {
      println(s"Diff count: ${diff.count()}")
      println(s"diff result rows")
      diff
        .replaceWithReadableTime(Seq("ts", "a_user_unit_test_item_views_ts_max", "b_user_unit_test_item_views_ts_max"),
                                 dropOriginal = true)
        .show()
    }
    assertEquals(diff.count(), 0)
  }

  @Test
  def testEventsEventsCumulative(): Unit = {
    val spark: SparkSession = SparkSessionBuilder.build("JoinTest" + "_" + Random.alphanumeric.take(6).mkString, local = true)
    val tableUtils = TableUtils(spark)
    val namespace = "test_namespace_jointest" + "_" + Random.alphanumeric.take(6).mkString
    tableUtils.createDatabase(namespace)
    // Create a cumulative source GroupBy
    val viewsTable = s"$namespace.view_cumulative"
    val viewsGroupBy = getViewsGroupBy(suffix = "cumulative", makeCumulative = true, namespace)
    // Copy and modify existing events/events case to use cumulative GroupBy
    val joinConf = getEventsEventsTemporal("cumulative", namespace)
    joinConf.setJoinParts(Seq(Builders.JoinPart(groupBy = viewsGroupBy)).asJava)

    // Run job
    val itemQueriesTable = s"$namespace.item_queries"
    println("Item Queries DF: ")
    val q =
      s"""
         |SELECT
         |  `ts`,
         |  `ds`,
         |  `item`,
         |  time_spent_ms as `time_spent_ms`
         |FROM $viewsTable
         |WHERE
         |  ds >= '2021-06-03' AND ds <= '2021-06-03'""".stripMargin
    spark.sql(q).show()
    val start = tableUtils.partitionSpec.minus(today, new Window(100, TimeUnit.DAYS))
    val join = new Join(joinConf = joinConf, endPartition = dayAndMonthBefore, tableUtils)
    val computed = join.computeJoin(Some(100))
    computed.show()

    val expected = tableUtils.sql(s"""
                                     |WITH
                                     |   queries AS (SELECT item, ts, ds from $itemQueriesTable where ds >= '$start' and ds <= '$dayAndMonthBefore')
                                     | SELECT queries.item, queries.ts, queries.ds, part.unit_test_item_views_ts_min, part.unit_test_item_views_ts_max, part.unit_test_item_views_time_spent_ms_average
                                     | FROM (SELECT queries.item,
                                     |        queries.ts,
                                     |        queries.ds,
                                     |        MIN(IF(queries.ts > $viewsTable.ts, $viewsTable.ts, null)) as unit_test_item_views_ts_min,
                                     |        MAX(IF(queries.ts > $viewsTable.ts, $viewsTable.ts, null)) as unit_test_item_views_ts_max,
                                     |        AVG(IF(queries.ts > $viewsTable.ts, time_spent_ms, null)) as unit_test_item_views_time_spent_ms_average
                                     |     FROM queries left outer join $viewsTable
                                     |     ON queries.item = $viewsTable.item
                                     |     WHERE $viewsTable.item IS NOT NULL AND $viewsTable.ds = '$today'
                                     |     GROUP BY queries.item, queries.ts, queries.ds) as part
                                     | JOIN queries
                                     | ON queries.item <=> part.item AND queries.ts <=> part.ts AND queries.ds <=> part.ds
                                     |""".stripMargin)
    expected.show()

    val diff = Comparison.sideBySide(computed, expected, List("item", "ts", "ds"))
    val queriesBare =
      tableUtils.sql(s"SELECT item, ts, ds from $itemQueriesTable where ds >= '$start' and ds <= '$dayAndMonthBefore'")
    assertEquals(queriesBare.count(), computed.count())
    if (diff.count() > 0) {
      println(s"Diff count: ${diff.count()}")
      println(s"diff result rows")
      diff.show()
    }
    assertEquals(diff.count(), 0)
  }

  def getGroupByForIncrementalSourceTest() = {
    val spark: SparkSession = SparkSessionBuilder.build("JoinTest" + "_" + Random.alphanumeric.take(6).mkString, local = true)
    val tableUtils = TableUtils(spark)
    val namespace = "test_namespace_jointest" + "_" + Random.alphanumeric.take(6).mkString
    tableUtils.createDatabase(namespace)
    val messageData = Seq(
      Row("Hello", "a", "2021-01-01"),
      Row("World", "a", "2021-01-02"),
      Row("Hello!", "b", "2021-01-03")
    )

    val messageSchema: StructType = new StructType()
      .add("message", SparkStringType)
      .add("id", SparkStringType)
      .add("ds", SparkStringType)

    val rdd: RDD[Row] = spark.sparkContext.parallelize(messageData)
    val df: DataFrame = spark.createDataFrame(rdd, messageSchema)

    val table = s"$namespace.messages"

    spark.sql(s"DROP TABLE IF EXISTS $table")

    df.write.partitionBy("ds").saveAsTable(table)

    val source = Builders.Source.events(
      table = table,
      query = Builders.Query(selects = Builders.Selects("message", s"{{ ${Constants.ChrononRunDs}  }}"),
                             startPartition = "2021-01-01")
    )

    Builders.GroupBy(
      sources = Seq(source),
      keyColumns = Seq("id"),
      aggregations = Seq(
        Builders.Aggregation(operation = Operation.MAX, inputColumn = "message")
        // Builders.Aggregation(operation = Operation.APPROX_UNIQUE_COUNT, inputColumn = "ts")
        // sql - APPROX_COUNT_DISTINCT(IF(queries.ts > $viewsTable.ts, time_spent_ms, null)) as user_ts_approx_unique_count
      ),
      metaData = Builders.MetaData(name = "unit_test.messages", namespace = namespace, team = "item_team"),
      accuracy = Accuracy.TEMPORAL
    )

  }

  @Test
  def testSourceQueryRender(): Unit = {
    val spark: SparkSession = SparkSessionBuilder.build("JoinTest" + "_" + Random.alphanumeric.take(6).mkString, local = true)
    val tableUtils = TableUtils(spark)
    val namespace = "test_namespace_jointest" + "_" + Random.alphanumeric.take(6).mkString
    tableUtils.createDatabase(namespace)
    // Test cumulative
    val viewsGroupByCumulative = getViewsGroupBy(suffix = "render", makeCumulative = true, namespace)
    val renderedCumulative = renderDataSourceQuery(
      viewsGroupByCumulative,
      viewsGroupByCumulative.sources.asScala.head,
      Seq("item"),
      PartitionRange("2021-02-23", "2021-05-03")(tableUtils),
      tableUtils,
      None,
      viewsGroupByCumulative.inferredAccuracy,
      tableUtils.partitionColumn
    )
    // Only checking that the date logic is correct in the query
    assert(renderedCumulative.contains(s"(ds >= '${today}') AND (ds <= '${today}')"))

    // Test incremental
    val viewsGroupByIncremental = getGroupByForIncrementalSourceTest()
    val renderedIncremental = renderDataSourceQuery(
      viewsGroupByCumulative,
      viewsGroupByIncremental.sources.asScala.head,
      Seq("item"),
      PartitionRange("2021-01-01", "2021-01-01")(tableUtils),
      tableUtils,
      None,
      viewsGroupByCumulative.inferredAccuracy,
      tableUtils.partitionColumn
    )
    println(renderedIncremental)
    assert(renderedIncremental.contains(s"(ds >= '2021-01-01') AND (ds <= '2021-01-01')"))
  }

  @Test
  def testNoAgg(): Unit = {
    val spark: SparkSession = SparkSessionBuilder.build("JoinTest" + "_" + Random.alphanumeric.take(6).mkString, local = true)
    val tableUtils = TableUtils(spark)
    val namespace = "test_namespace_jointest" + "_" + Random.alphanumeric.take(6).mkString
    tableUtils.createDatabase(namespace)
    // Left side entities, right side entities no agg
    // Also testing specific select statement (rather than select *)
    val namesSchema = List(
      Column("user", api.StringType, 1000),
      Column("name", api.StringType, 500)
    )
    val namesTable = s"$namespace.names"
    DataFrameGen.entities(spark, namesSchema, 1000, partitions = 400).save(namesTable)

    val namesSource = Builders.Source.entities(
      query =
        Builders.Query(selects = Builders.Selects("name"), startPartition = yearAgo, endPartition = dayAndMonthBefore),
      snapshotTable = namesTable
    )

    val namesGroupBy = Builders.GroupBy(
      sources = Seq(namesSource),
      keyColumns = Seq("user"),
      aggregations = null,
      metaData = Builders.MetaData(name = "unit_test.user_names", team = "chronon")
    )

    DataFrameGen
      .entities(spark, namesSchema, 1000, partitions = 400)
      .groupBy("user", "ds")
      .agg(Map("name" -> "max"))
      .save(namesTable)

    // left side
    val userSchema = List(Column("user", api.StringType, 100))
    val usersTable = s"$namespace.users"
    DataFrameGen.entities(spark, userSchema, 1000, partitions = 400).dropDuplicates().save(usersTable)

    val start = tableUtils.partitionSpec.minus(today, new Window(60, TimeUnit.DAYS))
    val end = tableUtils.partitionSpec.minus(today, new Window(15, TimeUnit.DAYS))
    val joinConf = Builders.Join(
      left = Builders.Source.entities(Builders.Query(selects = Map("user" -> "user"), startPartition = start),
                                      snapshotTable = usersTable),
      joinParts = Seq(Builders.JoinPart(groupBy = namesGroupBy)),
      metaData = Builders.MetaData(name = "test.user_features", namespace = namespace, team = "chronon")
    )

    val runner = new Join(joinConf, end, tableUtils)
    val computed = runner.computeJoin(Some(7))
    println(s"join start = $start")
    val expected = tableUtils.sql(s"""
                                     |WITH
                                     |   users AS (SELECT user, ds from $usersTable where ds >= '$start' and ds <= '$end'),
                                     |   grouped_names AS (
                                     |      SELECT user,
                                     |             name as unit_test_user_names_name,
                                     |             ds
                                     |      FROM $namesTable
                                     |      WHERE ds >= '$yearAgo' and ds <= '$dayAndMonthBefore')
                                     |   SELECT users.user,
                                     |        grouped_names.unit_test_user_names_name,
                                     |        users.ds
                                     | FROM users left outer join grouped_names
                                     | ON users.user = grouped_names.user
                                     | AND users.ds = grouped_names.ds
    """.stripMargin)

    println("showing join result")
    computed.show()
    println("showing query result")
    expected.show()
    println(
      s"Left side count: ${spark.sql(s"SELECT user, ds from $namesTable where ds >= '$start' and ds <= '$end'").count()}")
    println(s"Actual count: ${computed.count()}")
    println(s"Expected count: ${expected.count()}")
    val diff = Comparison.sideBySide(computed, expected, List("user", "ds"))
    if (diff.count() > 0) {
      println(s"Diff count: ${diff.count()}")
      println(s"diff result rows")
      diff.show()
    }
    assertEquals(diff.count(), 0)
  }

  @Test
  def testVersioning(): Unit = {
    val spark: SparkSession = SparkSessionBuilder.build("JoinTest" + "_" + Random.alphanumeric.take(6).mkString, local = true)
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
    assertEquals(leftChangeRecompute._1.sorted,
                 Seq(partTable, leftChangeJoinConf.metaData.bootstrapTable, leftChangeJoinConf.metaData.outputTable).sorted)
    assertTrue(leftChangeRecompute._2)

    // Test adding a joinPart
    val addPartJoinConf = joinConf.deepCopy()
    val existingJoinPart = addPartJoinConf.getJoinParts.get(0)
    val newJoinPart = Builders.JoinPart(groupBy = getViewsGroupBy(suffix = "versioning", namespace=namespace), prefix = "user_2")
    addPartJoinConf.setJoinParts(Seq(existingJoinPart, newJoinPart).asJava)
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
    expected.show()

    val diff = Comparison.sideBySide(expected, computed, List("item", "ts", "ds"))
    val queriesBare =
      tableUtils.sql(s"SELECT item, ts, ds from $itemQueriesTable where ds >= '$start' and ds <= '$dayAndMonthBefore'")
    assertEquals(queriesBare.count(), computed.count())
    if (diff.count() > 0) {
      println(s"Diff count: ${diff.count()}")
      println(s"diff result rows")
      diff
        .replaceWithReadableTime(
          Seq("ts", "a_user_3_unit_test_item_views_ts_max", "b_user_3_unit_test_item_views_ts_max"),
          dropOriginal = true)
        .show()
    }
    assertEquals(0, diff.count())
  }

  private def getViewsGroupBy(suffix: String, makeCumulative: Boolean = false, namespace: String) = {
    val viewsSchema = List(
      Column("user", api.StringType, 10000),
      Column("item", api.StringType, 100),
      Column("time_spent_ms", api.LongType, 5000)
    )
    val spark: SparkSession = SparkSessionBuilder.build("JoinTest" + "_" + Random.alphanumeric.take(6).mkString, local = true)
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
        // Builders.Aggregation(operation = Operation.APPROX_UNIQUE_COUNT, inputColumn = "ts")
        // sql - APPROX_COUNT_DISTINCT(IF(queries.ts > $viewsTable.ts, time_spent_ms, null)) as user_ts_approx_unique_count
      ),
      metaData = Builders.MetaData(name = "unit_test.item_views", namespace = namespace, team = "item_team"),
      accuracy = Accuracy.TEMPORAL
    )
  }

  private def getViewsGroupByWithKeyMapping(suffix: String, makeCumulative: Boolean = false, namespace: String) = {
    val viewsSchema = List(
      Column("user", api.StringType, 10000),
      Column("item_id", api.StringType, 100),
      Column("time_spent_ms", api.LongType, 5000)
    )
    val spark: SparkSession = SparkSessionBuilder.build("JoinTest" + "_" + Random.alphanumeric.take(6).mkString, local = true)
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
      keyColumns = Seq("item_id"),
      aggregations = Seq(
        Builders.Aggregation(operation = Operation.AVERAGE, inputColumn = "time_spent_ms"),
        Builders.Aggregation(operation = Operation.MIN, inputColumn = "ts"),
        Builders.Aggregation(operation = Operation.MAX, inputColumn = "ts")
      ),
      metaData = Builders.MetaData(name = "unit_test.item_views_key_mapping", namespace = namespace, team = "item_team"),
      accuracy = Accuracy.TEMPORAL
    )
  }

  private def getEventsEventsTemporal(nameSuffix: String = "", namespace: String) = {
    val spark: SparkSession = SparkSessionBuilder.build("JoinTest" + "_" + Random.alphanumeric.take(6).mkString, local = true)
    val tableUtils = TableUtils(spark)
    // left side
    val itemQueries = List(Column("item", api.StringType, 100))
    val itemQueriesTable = s"$namespace.item_queries"
    val itemQueriesDf = DataFrameGen
      .events(spark, itemQueries, 10000, partitions = 100)
    // duplicate the events
    itemQueriesDf.union(itemQueriesDf).save(itemQueriesTable) //.union(itemQueriesDf)

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
  def testEndPartitionJoin(): Unit = {
    val spark: SparkSession = SparkSessionBuilder.build("JoinTest" + "_" + Random.alphanumeric.take(6).mkString, local = true)
    val tableUtils = TableUtils(spark)
    val namespace = "test_namespace_jointest" + "_" + Random.alphanumeric.take(6).mkString
    tableUtils.createDatabase(namespace)
    val join = getEventsEventsTemporal("end_partition_test", namespace)
    val start = join.getLeft.query.startPartition
    val end = tableUtils.partitionSpec.after(start)
    val limitedJoin = Builders.Join(
      left =
        Builders.Source.events(Builders.Query(startPartition = start, endPartition = end), table = join.getLeft.table),
      joinParts = join.getJoinParts.toScala,
      metaData = join.metaData
    )
    assertTrue(end < today)
    val toCompute = new Join(limitedJoin, today, tableUtils)
    toCompute.computeJoin()
    val ds = tableUtils.sql(s"SELECT MAX(ds) FROM ${limitedJoin.metaData.outputTable}")
    assertTrue(ds.first().getString(0) < today)
  }

  @Test
  def testSkipBloomFilterJoinBackfill(): Unit = {
    val spark: SparkSession = SparkSessionBuilder.build("JoinTest" + "_" + Random.alphanumeric.take(6).mkString, local = true)
    val tableUtils = TableUtils(spark)
    val namespace = "test_namespace_jointest" + "_" + Random.alphanumeric.take(6).mkString
    tableUtils.createDatabase(namespace)
    val testSpark: SparkSession = SparkSessionBuilder.build(
      "JoinTest",
      local = true,
      additionalConfig = Some(Map("spark.chronon.backfill.bloomfilter.threshold" -> "100")))
    val testTableUtils = TableUtils(testSpark)
    val viewsSchema = List(
      Column("user", api.StringType, 10000),
      Column("item", api.StringType, 100),
      Column("time_spent_ms", api.LongType, 5000)
    )

    val viewsTable = s"$namespace.view_events_bloom_test"
    DataFrameGen.events(testSpark, viewsSchema, count = 1000, partitions = 200).drop("ts").save(viewsTable)

    val viewsSource = Builders.Source.events(
      query = Builders.Query(selects = Builders.Selects("time_spent_ms"), startPartition = yearAgo),
      table = viewsTable
    )

    val viewsGroupBy = Builders.GroupBy(
      sources = Seq(viewsSource),
      keyColumns = Seq("item"),
      aggregations = Seq(
        Builders.Aggregation(operation = Operation.AVERAGE, inputColumn = "time_spent_ms")
      ),
      metaData = Builders.MetaData(name = "bloom_test.item_views", namespace = namespace),
      accuracy = Accuracy.SNAPSHOT
    )

    // left side
    val itemQueries = List(Column("item", api.StringType, 100))
    val itemQueriesTable = s"$namespace.item_queries_bloom_test"
    DataFrameGen
      .events(testSpark, itemQueries, 1000, partitions = 100)
      .save(itemQueriesTable)

    val start = testTableUtils.partitionSpec.minus(today, new Window(100, TimeUnit.DAYS))
    val joinConf = Builders.Join(
      left = Builders.Source.events(Builders.Query(startPartition = start), table = itemQueriesTable),
      joinParts = Seq(Builders.JoinPart(groupBy = viewsGroupBy, prefix = "user")),
      metaData = Builders.MetaData(name = "test.item_snapshot_bloom_test", namespace = namespace, team = "chronon")
    )
    val skipBloomComputed = new Join(joinConf, today, testTableUtils).computeJoin()
    val leftSideCount = testSpark.sql(s"SELECT item, ts, ds from $itemQueriesTable where ds >= '$start'").count()
    println("computed count: " + skipBloomComputed.count())
    assertEquals(leftSideCount, skipBloomComputed.count())
  }

  @Test
  def testStructJoin(): Unit = {
    val spark: SparkSession = SparkSessionBuilder.build("JoinTest" + "_" + Random.alphanumeric.take(6).mkString, local = true)
    val tableUtils = TableUtils(spark)
    val namespace = "test_namespace_jointest" + "_" + Random.alphanumeric.take(6).mkString
    tableUtils.createDatabase(namespace)
    val nameSuffix = "_struct_test"
    val itemQueries = List(Column("item", api.StringType, 100))
    val itemQueriesTable = s"$namespace.item_queries_$nameSuffix"
    val itemQueriesDf = DataFrameGen
      .events(spark, itemQueries, 10000, partitions = 100)

    itemQueriesDf.save(s"${itemQueriesTable}_tmp")
    val structLeftDf = tableUtils.sql(
      s"SELECT item, NAMED_STRUCT('item_repeat', item) as item_struct, ts, ds FROM ${itemQueriesTable}_tmp")
    structLeftDf.save(itemQueriesTable)
    val start = tableUtils.partitionSpec.minus(today, new Window(100, TimeUnit.DAYS))

    val viewsSchema = List(
      Column("user", api.StringType, 10000),
      Column("item", api.StringType, 100),
      Column("time_spent_ms", api.LongType, 5000)
    )

    val viewsTable = s"$namespace.view_$nameSuffix"
    val df = DataFrameGen.events(spark, viewsSchema, count = 1000, partitions = 200)

    val viewsSource = Builders.Source.events(
      table = viewsTable,
      query = Builders.Query(selects = Builders.Selects("time_spent_ms", "item_struct"), startPartition = yearAgo)
    )
    spark.sql(s"DROP TABLE IF EXISTS $viewsTable")
    df.save(s"${viewsTable}_tmp", Map("tblProp1" -> "1"))
    val structSource =
      tableUtils.sql(s"SELECT *, NAMED_STRUCT('item_repeat', item) as item_struct FROM ${viewsTable}_tmp")
    structSource.save(viewsTable)
    val gb = Builders.GroupBy(
      sources = Seq(viewsSource),
      keyColumns = Seq("item"),
      aggregations = Seq(
        Builders.Aggregation(operation = Operation.AVERAGE, inputColumn = "time_spent_ms"),
        Builders.Aggregation(operation = Operation.MIN, inputColumn = "ts"),
        Builders.Aggregation(operation = Operation.LAST_K, argMap = Map("k" -> "1"), inputColumn = "item_struct")
        // Builders.Aggregation(operation = Operation.APPROX_UNIQUE_COUNT, inputColumn = "ts")
        // sql - APPROX_COUNT_DISTINCT(IF(queries.ts > $viewsTable.ts, time_spent_ms, null)) as user_ts_approx_unique_count
      ),
      metaData =
        Builders.MetaData(name = s"unit_test.item_views_$nameSuffix", namespace = namespace, team = "item_team"),
      accuracy = Accuracy.TEMPORAL
    )

    val join = Builders.Join(
      left = Builders.Source.events(Builders.Query(startPartition = start), table = itemQueriesTable),
      joinParts = Seq(Builders.JoinPart(groupBy = gb, prefix = "user")),
      metaData =
        Builders.MetaData(name = s"test.item_temporal_features$nameSuffix", namespace = namespace, team = "item_team")
    )
    val toCompute = new Join(join, today, tableUtils)
    toCompute.computeJoin()
    // Add stats
    new SummaryJob(spark, join, today).dailyRun(stepDays = Some(30))
  }

  @Test
  def testMigrationForBootstrap(): Unit = {
    val spark: SparkSession = SparkSessionBuilder.build("JoinTest" + "_" + Random.alphanumeric.take(6).mkString, local = true)
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
    val spark: SparkSession = SparkSessionBuilder.build("JoinTest" + "_" + Random.alphanumeric.take(6).mkString, local = true)
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
      Constants.SemanticHashKey -> gson.toJson(oldVersionSemanticHash.asJava),
      Constants.SemanticHashOptionsKey -> gson.toJson(
        Map(
          Constants.SemanticHashExcludeTopic -> "false"
        ).asJava)
    )
    dummyTableUtils.alterTableProperties(join.metaData.outputTable, oldTableProperties)
    dummyTableUtils.alterTableProperties(join.metaData.bootstrapTable, oldTableProperties)
  }

  private def hasExcludeTopicFlag(tableProps: Map[String, String], gson: Gson): Boolean = {
    val optionsString = tableProps(Constants.SemanticHashOptionsKey)
    val options = gson.fromJson(optionsString, classOf[java.util.HashMap[String, String]]).asScala
    options.get(Constants.SemanticHashExcludeTopic).contains("true")
  }

  @Test
  def testMigrationForTopicSuccess(): Unit = {
    val spark: SparkSession = SparkSessionBuilder.build("JoinTest" + "_" + Random.alphanumeric.take(6).mkString, local = true)
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
    assertEquals(gson.toJson(newVersionSemanticHash.asJava), tablePropsV1(Constants.SemanticHashKey))

    // Modify the topic and rerun
    val joinPartNew = join.joinParts.get(0).deepCopy()
    joinPartNew.groupBy.sources.asScala.head.getEvents.setTopic("transactions_topic_v2")
    val joinNew = join.deepCopy()
    joinNew.setJoinParts(Seq(joinPartNew).asJava)
    runJob(joinNew, 0)

    // Verify that the semantic hash has NOT changed
    val tablePropsV2 = tableUtils.getTableProperties(join.metaData.outputTable).get
    assertTrue(hasExcludeTopicFlag(tablePropsV2, gson))
    assertEquals(gson.toJson(newVersionSemanticHash.asJava), tablePropsV2(Constants.SemanticHashKey))
  }

  @Test
  def testMigrationForTopicManualArchive(): Unit = {
    val spark: SparkSession = SparkSessionBuilder.build("JoinTest" + "_" + Random.alphanumeric.take(6).mkString, local = true)
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
    joinPartNew.getGroupBy.getSources.asScala.head.getEvents.setTopic("transactions_topic_v2")
    joinPartNew.getGroupBy.getAggregations.asScala.head.setWindows(Seq(new Window(7, TimeUnit.DAYS)).asJava)
    val joinNew = join.deepCopy()
    joinNew.setJoinParts(Seq(joinPartNew).asJava)

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
    assertNotEquals(gson.toJson(newVersionSemanticHash.asJava), tableProps(Constants.SemanticHashKey))
  }

  @Test
  def testKeyMappingOverlappingFields(): Unit = {
    val spark: SparkSession = SparkSessionBuilder.build("JoinTest" + "_" + Random.alphanumeric.take(6).mkString, local = true)
    val tableUtils = TableUtils(spark)
    val namespace = "test_namespace_jointest" + "_" + Random.alphanumeric.take(6).mkString
    tableUtils.createDatabase(namespace)
    // test the scenario when a key_mapping is a -> b, (right key b is mapped to left key a) and
    // a happens to be another field in the same group by

    val namesSchema = List(
      Column("user", api.StringType, 1000),
      Column("attribute", api.StringType, 500)
    )
    val namesTable = s"$namespace.key_overlap_names"
    DataFrameGen.entities(spark, namesSchema, 1000, partitions = 400).save(namesTable)

    val namesSource = Builders.Source.entities(
      query =
        Builders.Query(selects =
                         Builders.Selects.exprs("user" -> "user", "user_id" -> "user", "attribute" -> "attribute"),
                       startPartition = yearAgo,
                       endPartition = dayAndMonthBefore),
      snapshotTable = namesTable
    )

    val namesGroupBy = Builders.GroupBy(
      sources = Seq(namesSource),
      keyColumns = Seq("user"),
      aggregations = null,
      metaData = Builders.MetaData(name = "unit_test.key_overlap.user_names", team = "chronon")
    )

    // left side
    val userSchema = List(Column("user_id", api.StringType, 100))
    val usersTable = s"$namespace.key_overlap_users"
    DataFrameGen.events(spark, userSchema, 1000, partitions = 400).dropDuplicates().save(usersTable)

    val start = tableUtils.partitionSpec.minus(today, new Window(60, TimeUnit.DAYS))
    val end = tableUtils.partitionSpec.minus(today, new Window(15, TimeUnit.DAYS))
    val joinConf = Builders.Join(
      left = Builders.Source.entities(Builders.Query(selects = Map("user_id" -> "user_id"), startPartition = start),
                                      snapshotTable = usersTable),
      joinParts = Seq(
        Builders.JoinPart(groupBy = namesGroupBy,
                          keyMapping = Map(
                            "user_id" -> "user"
                          ))),
      metaData =
        Builders.MetaData(name = "unit_test.key_overlap.user_features", namespace = namespace, team = "chronon")
    )

    val runner = new Join(joinConf, end, tableUtils)
    val computed = runner.computeJoin(Some(7))
    assertFalse(computed.isEmpty)
  }

  /**
    * Create a event table as left side, 3 group bys as right side.
    * Generate data using DataFrameGen and save to the tables.
    * Create a join with only one join part selected.
    * Run computeJoin().
    * Check if the selected join part is computed and the other join parts are not computed.
    */
  @Test
  def testSelectedJoinParts(): Unit = {
    val spark: SparkSession = SparkSessionBuilder.build("JoinTest" + "_" + Random.alphanumeric.take(6).mkString, local = true)
    val tableUtils = TableUtils(spark)
    val namespace = "test_namespace_jointest" + "_" + Random.alphanumeric.take(6).mkString
    tableUtils.createDatabase(namespace)
    // Left
    val itemQueries = List(
      Column("item", api.StringType, 100),
      Column("value", api.LongType, 100)
    )
    val itemQueriesTable = s"$namespace.item_queries_selected_join_parts"
    spark.sql(s"DROP TABLE IF EXISTS $itemQueriesTable")
    spark.sql(s"DROP TABLE IF EXISTS ${itemQueriesTable}_tmp")
    DataFrameGen.events(spark, itemQueries, 10000, partitions = 30).save(s"${itemQueriesTable}_tmp")
    val leftDf = tableUtils.sql(s"SELECT item, value, ts, ds FROM ${itemQueriesTable}_tmp")
    leftDf.save(itemQueriesTable)
    val start = monthAgo

    // Right
    val viewsSchema = List(
      Column("user", api.StringType, 10000),
      Column("item", api.StringType, 100),
      Column("value", api.LongType, 100)
    )
    val viewsTable = s"$namespace.view_selected_join_parts"
    spark.sql(s"DROP TABLE IF EXISTS $viewsTable")
    DataFrameGen.events(spark, viewsSchema, count = 10000, partitions = 30).save(viewsTable)

    // Group By
    val gb1 = Builders.GroupBy(
      sources = Seq(
        Builders.Source.events(
          table = viewsTable,
          query = Builders.Query(startPartition = start)
        )),
      keyColumns = Seq("item"),
      aggregations = Seq(
        Builders.Aggregation(operation = Operation.LAST_K, argMap = Map("k" -> "10"), inputColumn = "user"),
        Builders.Aggregation(operation = Operation.MAX, argMap = Map("k" -> "2"), inputColumn = "value")
      ),
      metaData = Builders.MetaData(name = s"unit_test.item_views_selected_join_parts_1",
                                   namespace = namespace,
                                   team = "item_team"),
      accuracy = Accuracy.SNAPSHOT
    )

    val gb2 = Builders.GroupBy(
      sources = Seq(
        Builders.Source.events(
          table = viewsTable,
          query = Builders.Query(startPartition = start)
        )),
      keyColumns = Seq("item"),
      aggregations = Seq(
        Builders.Aggregation(operation = Operation.MIN, argMap = Map("k" -> "1"), inputColumn = "value")
      ),
      metaData = Builders.MetaData(name = s"unit_test.item_views_selected_join_parts_2",
                                   namespace = namespace,
                                   team = "item_team"),
      accuracy = Accuracy.SNAPSHOT
    )

    val gb3 = Builders.GroupBy(
      sources = Seq(
        Builders.Source.events(
          table = viewsTable,
          query = Builders.Query(startPartition = start)
        )),
      keyColumns = Seq("item"),
      aggregations = Seq(
        Builders.Aggregation(operation = Operation.AVERAGE, inputColumn = "value")
      ),
      metaData = Builders.MetaData(name = s"unit_test.item_views_selected_join_parts_3",
                                   namespace = namespace,
                                   team = "item_team"),
      accuracy = Accuracy.SNAPSHOT
    )

    // Join
    val joinConf = Builders.Join(
      left = Builders.Source.events(Builders.Query(startPartition = start), table = itemQueriesTable),
      joinParts = Seq(
        Builders.JoinPart(groupBy = gb1, prefix = "user1"),
        Builders.JoinPart(groupBy = gb2, prefix = "user2"),
        Builders.JoinPart(groupBy = gb3, prefix = "user3")
      ),
      metaData = Builders.MetaData(name = s"unit_test.item_temporal_features.selected_join_parts",
                                   namespace = namespace,
                                   team = "item_team",
                                   online = true)
    )

    // Drop Join Part tables if any
    val partTable1 = s"${joinConf.metaData.outputTable}_user1_unit_test_item_views_selected_join_parts_1"
    val partTable2 = s"${joinConf.metaData.outputTable}_user2_unit_test_item_views_selected_join_parts_2"
    val partTable3 = s"${joinConf.metaData.outputTable}_user3_unit_test_item_views_selected_join_parts_3"
    spark.sql(s"DROP TABLE IF EXISTS $partTable1")
    spark.sql(s"DROP TABLE IF EXISTS $partTable2")
    spark.sql(s"DROP TABLE IF EXISTS $partTable3")

    // Compute daily join.
    val joinJob = new Join(joinConf,
                           today,
                           tableUtils,
                           selectedJoinParts = Some(List("user1_unit_test_item_views_selected_join_parts_1")))

    joinJob.computeJoinOpt()

    val part1 = tableUtils.sql(s"SELECT * FROM $partTable1")
    assertTrue(part1.count() > 0)

    val thrown2 = intercept[AnalysisException] {
      spark.sql(s"SELECT * FROM $partTable2")
    }
    val thrown3 = intercept[AnalysisException] {
      spark.sql(s"SELECT * FROM $partTable3")
    }
    assert(
      thrown2.getMessage.contains("Table or view not found") && thrown3.getMessage.contains("Table or view not found"))
  }

  def testJoinDerivationAnalyzer(): Unit = {
    lazy val spark: SparkSession = SparkSessionBuilder.build("JoinTest" + "_" + Random.alphanumeric.take(6).mkString, local = true)
    val tableUtils = TableUtils(spark)
    val namespace = "test_join_derivation" + "_" + Random.alphanumeric.take(6).mkString
    tableUtils.createDatabase(namespace)
    val viewsGroupBy = getViewsGroupBy(suffix = "cumulative", makeCumulative = true, namespace)
    val joinConf = getEventsEventsTemporal("cumulative", namespace)
    joinConf.setDerivations(Seq(
      Derivation(
        name = "*",
        expression = "*"
      ), Derivation(
        name = "test_feature_name",
        expression = f"${viewsGroupBy.metaData.name}_time_spent_ms_average"
      )
    ).asJava)


    val today = tableUtils.partitionSpec.at(System.currentTimeMillis())
    val (_, aggregationsMetadata) =
      new Analyzer(tableUtils, joinConf, monthAgo, today).analyzeJoin(joinConf, enableHitter = false)
    aggregationsMetadata.foreach(agg => {assertTrue(agg.operation == "Derivation")})
    aggregationsMetadata.exists(_.name == "test_feature_name")
  }

  def testJoinDerivationOnExternalAnalyzer(): Unit = {
    lazy val spark: SparkSession = SparkSessionBuilder.build("JoinTest" + "_" + Random.alphanumeric.take(6).mkString, local = true)
    val tableUtils = TableUtils(spark)
    val namespace = "test_join_derivation" + "_" + Random.alphanumeric.take(6).mkString
    tableUtils.createDatabase(namespace)
    val joinConfWithExternal = getEventsEventsTemporal("cumulative", namespace)

    joinConfWithExternal.setOnlineExternalParts(Seq(
      Builders.ExternalPart(
        Builders.ContextualSource(
          fields = Array(
            StructField("user_txn_count_30d", LongType),
            StructField("item", StringType)
          )
        )
      )
    ).asJava
    )

    joinConfWithExternal.setDerivations(
      Seq(
        Builders.Derivation(
          name = "*"
        ),
        // contextual feature rename
        Builders.Derivation(
          name = "user_txn_count_30d",
          expression = "ext_contextual_user_txn_count_30d"
        ),
        Builders.Derivation(
          name = "item",
          expression = "ext_contextual_item"
        )
      ).asJava
    )

    val today = tableUtils.partitionSpec.at(System.currentTimeMillis())
    val (_, aggregationsMetadata) =
      new Analyzer(tableUtils, joinConfWithExternal, monthAgo, today).analyzeJoin(joinConfWithExternal, enableHitter = false)
    aggregationsMetadata.foreach(agg => {assertTrue(agg.operation == "Derivation")})
    aggregationsMetadata.exists(_.name == "user_txn_count_30d")
    aggregationsMetadata.exists(_.name == "item")
  }

  def testJoinDerivationWithKeyAnalyzer(): Unit = {
    lazy val spark: SparkSession = SparkSessionBuilder.build("JoinTest" + "_" + Random.alphanumeric.take(6).mkString, local = true)
    val tableUtils = TableUtils(spark)
    val namespace = "test_join_derivation" + "_" + Random.alphanumeric.take(6).mkString
    tableUtils.createDatabase(namespace)
    val joinConfWithDerivationWithKey = getEventsEventsTemporal("cumulative", namespace)
    val viewsGroupBy = getViewsGroupBy(suffix = "cumulative", makeCumulative = true, namespace)
    val viewGroupByWithKepMapping = getViewsGroupByWithKeyMapping("cumulative", makeCumulative = true, namespace)

    joinConfWithDerivationWithKey.setJoinParts(
      Seq(Builders.JoinPart(
        groupBy = viewGroupByWithKepMapping,
        keyMapping = Map("item" -> "item_id")
      )).asJava
    )

    joinConfWithDerivationWithKey.setDerivations(
      Seq(
        Builders.Derivation(
          name = "*"
        ),
        Builders.Derivation(
          name = "item",
          expression = "item"
        )
      ).asJava
    )

    val today = tableUtils.partitionSpec.at(System.currentTimeMillis())
    val (_, aggregationsMetadata) =
      new Analyzer(tableUtils, joinConfWithDerivationWithKey, monthAgo, today).analyzeJoin(joinConfWithDerivationWithKey, enableHitter = false)
    aggregationsMetadata.foreach(agg => {assertTrue(agg.operation == "Derivation")})
    aggregationsMetadata.exists(_.name == f"${viewsGroupBy.metaData.name}_time_spent_ms_average")
    aggregationsMetadata.exists(_.name == f"${viewGroupByWithKepMapping.metaData.name}_time_spent_ms_average")
    aggregationsMetadata.exists(_.name == "item")
  }

  @Test
  def testJoinDifferentPartitionColumns(): Unit = {
    val spark: SparkSession = SparkSessionBuilder.build("JoinTest" + "_" + Random.alphanumeric.take(6).mkString, local = true)
    val tableUtils = TableUtils(spark)
    val namespace = "test_namespace_jointest" + "_" + Random.alphanumeric.take(6).mkString
    tableUtils.createDatabase(namespace)
    // Left side entities, right side entities no agg
    // Also testing specific select statement (rather than select *)
    val namesSchema = List(
      Column("user", api.StringType, 1000),
      Column("name", api.StringType, 500)
    )
    val namePartitionColumn = "name_date"
    val namesTable = s"$namespace.names"
    DataFrameGen.entities(spark, namesSchema, 1000, partitions = 400, partitionColOpt = Some(namePartitionColumn)).save(namesTable, partitionColumns = Seq(namePartitionColumn))

    val namesSource = Builders.Source.entities(
      query =
        Builders.Query(selects = Builders.Selects("name"), startPartition = yearAgo, endPartition = dayAndMonthBefore, partitionColumn = namePartitionColumn),
      snapshotTable = namesTable
    )

    val namesGroupBy = Builders.GroupBy(
      sources = Seq(namesSource),
      keyColumns = Seq("user"),
      aggregations = null,
      metaData = Builders.MetaData(name = "unit_test.user_names", team = "chronon")
    )

    DataFrameGen
      .entities(spark, namesSchema, 1000, partitions = 400, partitionColOpt = Some(namePartitionColumn))
      .groupBy("user", namePartitionColumn)
      .agg(Map("name" -> "max"))
      .save(namesTable, partitionColumns = Seq(namePartitionColumn))

    // left side
    val userPartitionColumn = "user_date"
    val userSchema = List(Column("user", api.StringType, 100))
    val usersTable = s"$namespace.users"
    DataFrameGen.entities(spark, userSchema, 1000, partitions = 400, partitionColOpt = Some(userPartitionColumn))
      .dropDuplicates()
      .save(usersTable, partitionColumns = Seq(userPartitionColumn))

    val start = tableUtils.partitionSpec.minus(today, new Window(60, TimeUnit.DAYS))
    val end = tableUtils.partitionSpec.minus(today, new Window(15, TimeUnit.DAYS))
    val joinConf = Builders.Join(
      left = Builders.Source.entities(Builders.Query(selects = Map("user" -> "user"), startPartition = start, partitionColumn = userPartitionColumn),
        snapshotTable = usersTable),
      joinParts = Seq(Builders.JoinPart(groupBy = namesGroupBy)),
      metaData = Builders.MetaData(name = "test.user_features", namespace = namespace, team = "chronon")
    )

    val runner = new Join(joinConf, end, tableUtils)
    val computed = runner.computeJoin(Some(7))
    println(s"join start = $start")
    val expected = tableUtils.sql(s"""
                                     |WITH
                                     |   users AS (SELECT user, $userPartitionColumn as ds from $usersTable where $userPartitionColumn >= '$start' and $userPartitionColumn <= '$end'),
                                     |   grouped_names AS (
                                     |      SELECT user,
                                     |             name as unit_test_user_names_name,
                                     |             $namePartitionColumn as ds
                                     |      FROM $namesTable
                                     |      WHERE $namePartitionColumn >= '$yearAgo' and $namePartitionColumn <= '$dayAndMonthBefore')
                                     |   SELECT users.user,
                                     |        grouped_names.unit_test_user_names_name,
                                     |        users.ds
                                     | FROM users left outer join grouped_names
                                     | ON users.user = grouped_names.user
                                     | AND users.ds = grouped_names.ds
    """.stripMargin)

    println("showing join result")
    computed.show()
    println("showing query result")
    expected.show()
    println(
      s"Left side count: ${spark.sql(s"SELECT user, $namePartitionColumn as ds from $namesTable where $namePartitionColumn >= '$start' and $namePartitionColumn <= '$end'").count()}")
    println(s"Actual count: ${computed.count()}")
    println(s"Expected count: ${expected.count()}")
    val diff = Comparison.sideBySide(computed, expected, List("user", "ds"))
    if (diff.count() > 0) {
      println(s"Diff count: ${diff.count()}")
      println(s"diff result rows")
      diff.show()
    }
    assertEquals(diff.count(), 0)
  }
}
