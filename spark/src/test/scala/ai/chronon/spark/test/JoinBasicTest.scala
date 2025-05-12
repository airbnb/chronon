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

//These are join tests that are easy to parallelize
//And don't require a lot of orchestration
class JoinBasicTests {
  val dummySpark: SparkSession = SparkSessionBuilder.build("JoinBasicTest", local = true)
  private val dummyTableUtils = TableUtils(dummySpark)
  private val today = dummyTableUtils.partitionSpec.at(System.currentTimeMillis())
  private val monthAgo = dummyTableUtils.partitionSpec.minus(today, new Window(30, TimeUnit.DAYS))
  private val yearAgo = dummyTableUtils.partitionSpec.minus(today, new Window(365, TimeUnit.DAYS))
  private val dayAndMonthBefore = dummyTableUtils.partitionSpec.before(monthAgo)
  
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
    DataFrameGen.entities(spark, namesSchema, 1000, partitions = 200, partitionColOpt = Some(namePartitionColumn)).save(namesTable, partitionColumns = Seq(namePartitionColumn))

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
      .entities(spark, namesSchema, 1000, partitions = 200, partitionColOpt = Some(namePartitionColumn))
      .groupBy("user", namePartitionColumn)
      .agg(Map("name" -> "max"))
      .save(namesTable, partitionColumns = Seq(namePartitionColumn))

    // left side
    val userPartitionColumn = "user_date"
    val userSchema = List(Column("user", api.StringType, 100))
    val usersTable = s"$namespace.users"
    DataFrameGen.entities(spark, userSchema, 1000, partitions = 200, partitionColOpt = Some(userPartitionColumn))
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
    logger.debug(s"join start = $start")
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

    logger.debug("showing join result")
    computed.show()
    logger.debug("showing query result")
    expected.show()
    logger.debug(
      s"Left side count: ${spark.sql(s"SELECT user, $namePartitionColumn as ds from $namesTable where $namePartitionColumn >= '$start' and $namePartitionColumn <= '$end'").count()}")
    logger.debug(s"Actual count: ${computed.count()}")
    logger.debug(s"Expected count: ${expected.count()}")
    val diff = Comparison.sideBySide(computed, expected, List("user", "ds"))
    val diffCount = diff.count()
    if (diffCount > 0) {
      logger.debug(s"Diff count: ${diffCount}")
      logger.debug(s"diff result rows")
      if (logger.isDebugEnabled) {
        diff.show()
      }
    }
    assertEquals(diffCount, 0)
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
    DataFrameGen.entities(spark, weightSchema, 1000, partitions = 200).save(weightTable)

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
    DataFrameGen.entities(spark, heightSchema, 1000, partitions = 200).save(heightTable)
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
    DataFrameGen.entities(spark, countrySchema, 1000, partitions = 200).save(countryTable)

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

    logger.debug("showing join result")
    computed.show()
    logger.debug("showing query result")
    expected.show()
    logger.debug(
      s"Left side count: ${spark.sql(s"SELECT country, ds from $countryTable where ds >= '$start' and ds <= '$end'").count()}")
    logger.debug(s"Actual count: ${computed.count()}")
    logger.debug(s"Expected count: ${expected.count()}")
    val diff = Comparison.sideBySide(computed, expected, List("country", "ds"))
    if (diff.count() > 0) {
      logger.debug(s"Diff count: ${diff.count()}")
      logger.debug(s"diff result rows")
      diff.show()
    }
    assertEquals(diff.count(), 0)
    /* the below testing case to cover the scenario when input table and output table
     * have same partitions, in other words, the frontfill is done, the join job
     * should not trigger a backfill and exit the program properly
     * TODO: Revisit this in a logger world.
    // use console to redirect logger.debug message to Java IO
    val stream = new java.io.ByteArrayOutputStream()
    Console.withOut(stream) {
      // rerun the same join job
      runner.computeJoin(Some(7))
    }
    val stdOutMsg = stream.toString()
    logger.debug(s"std out message =\n $stdOutMsg")
    // make sure that the program exits with target print statements
    assertTrue(stdOutMsg.contains(s"There is no data to compute based on end partition of $end."))
     */
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
    DataFrameGen.entities(spark, weightSchema, 1000, partitions = 200).save(weightTable)

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
    logger.debug("showing join result")
    computed.show()

    val leftSideCount = spark.sql(s"SELECT country, ds from $countryTable where ds == '$end'").count()
    logger.debug(s"Left side expected count: $leftSideCount")
    logger.debug(s"Actual count: ${computed.count()}")
    assertEquals(leftSideCount, computed.count())
    // There should be only one partition in computed df which equals to end partition
    val allPartitions = computed.select("ds").rdd.map(row => row(0)).collect().toSet
    assert(allPartitions.size == 1)
    assertEquals(allPartitions.toList(0), end)
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
    DataFrameGen.entities(spark, namesSchema, 1000, partitions = 200).save(namesTable)

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
      .entities(spark, namesSchema, 1000, partitions = 200)
      .groupBy("user", "ds")
      .agg(Map("name" -> "max"))
      .save(namesTable)

    // left side
    val userSchema = List(Column("user", api.StringType, 100))
    val usersTable = s"$namespace.users"
    DataFrameGen.entities(spark, userSchema, 1000, partitions = 200).dropDuplicates().save(usersTable)

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
    logger.debug(s"join start = $start")
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
    if (logger.isDebugEnabled) {
      logger.debug("showing join result")
      computed.show()
      logger.debug("showing query result")
      expected.show()
      logger.debug(
        s"Left side count: ${spark.sql(s"SELECT user, ds from $namesTable where ds >= '$start' and ds <= '$end'").count()}")
      logger.debug(s"Actual count: ${computed.count()}")
      logger.debug(s"Expected count: ${expected.count()}")
    }
    val diff = Comparison.sideBySide(computed, expected, List("user", "ds"))
    val diffCount = diff.count()
    if (diffCount > 0) {
      logger.debug(s"Diff count: $diffCount")
      logger.debug(s"diff result rows")
      diff.show()
    }
    assertEquals(diffCount, 0)
  }

}
