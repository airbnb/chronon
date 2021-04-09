package ai.zipline.spark.test

import ai.zipline.aggregator.base.{DoubleType, LongType, StringType}
import ai.zipline.aggregator.test.Column
import ai.zipline.api.{Builders, _}
import ai.zipline.spark.Extensions._
import ai.zipline.spark.{Comparison, Join, SparkSessionBuilder, TableUtils}
import org.apache.spark.sql.SparkSession
import org.junit.Assert._
import org.junit.Test

import scala.collection.JavaConverters._

class JoinTest {

  val spark: SparkSession = SparkSessionBuilder.build("JoinTest", local = true)

  private val today = Constants.Partition.at(System.currentTimeMillis())
  private val monthAgo = Constants.Partition.minus(today, new Window(30, TimeUnit.DAYS))
  private val yearAgo = Constants.Partition.minus(today, new Window(365, TimeUnit.DAYS))
  private val dayAndMonthBefore = Constants.Partition.before(monthAgo)

  private val namespace = "test_namespace_jointest"
  spark.sql(s"CREATE DATABASE IF NOT EXISTS $namespace")

  private val tableUtils = TableUtils(spark)

  @Test
  def testEventsEntitiesSnapshot(): Unit = {

    val dollarTransactions = List(
      Column("user", StringType, 100),
      Column("ts", LongType, 200),
      Column("amount_dollars", LongType, 1000)
    )

    val rupeeTransactions = List(
      Column("user", StringType, 100),
      Column("ts", LongType, 200),
      Column("amount_rupees", LongType, 70000)
    )

    val dollarTable = s"$namespace.dollar_transactions"
    val rupeeTable = s"$namespace.rupee_transactions"
    DataFrameGen.entities(spark, dollarTransactions, 10000, partitions = 400).save(dollarTable, Map("tblProp1" -> "1"))
    DataFrameGen.entities(spark, rupeeTransactions, 1000, partitions = 30).save(rupeeTable)

    val dollarSource = Builders.Source.entities(
      query = Builders.Query(
        selects = Builders.Selects("ts", "amount_dollars"),
        startPartition = yearAgo,
        endPartition = dayAndMonthBefore,
        setups =
          Seq("create temporary function temp_replace_right_a as 'org.apache.hadoop.hive.ql.udf.UDFRegExpReplace'")
      ),
      snapshotTable = dollarTable
    )

    val rupeeSource =
      Builders.Source.entities(
        query = Builders.Query(
          selects = Map("ts" -> "ts", "amount_dollars" -> "CAST(amount_rupees/70 as long)"),
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
      keyColumns = Seq("user"),
      aggregations = Seq(
        Builders.Aggregation(operation = Operation.SUM,
                             inputColumn = "amount_dollars",
                             windows = Seq(new Window(30, TimeUnit.DAYS)))),
      metaData = Builders.MetaData(name = "unit_test.user_transactions", namespace = namespace, team = "zipline")
    )
    val queriesSchema = List(
      Column("user", StringType, 100)
    )

    val queryTable = s"$namespace.queries"
    DataFrameGen
      .events(spark, queriesSchema, 1000, partitions = 180)
      .withColumnRenamed("user", "user_name") // to test zipline renaming logic
      .save(queryTable)

    val start = Constants.Partition.minus(today, new Window(60, TimeUnit.DAYS))
    val end = Constants.Partition.minus(today, new Window(30, TimeUnit.DAYS))
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
      joinParts = Seq(Builders.JoinPart(groupBy = groupBy, keyMapping = Map("user_name" -> "user"))),
      metaData = Builders.MetaData(name = "test.user_transaction_features", namespace = namespace, team = "zipline")
    )

    val runner1 = new Join(joinConf, end, tableUtils)

    val computed = runner1.computeJoin(Some(3))
    println(s"join start = $start")

    val expected = spark.sql(s"""
        |WITH 
        |   queries AS (SELECT user_name, ts, ds from $queryTable where ds >= '$start' and ds <= '$end'),
        |   grouped_transactions AS (
        |      SELECT user, 
        |             ds, 
        |             SUM(IF(transactions.ts  >= (unix_timestamp(transactions.ds, 'yyyy-MM-dd') - (86400*30)) * 1000, amount_dollars, null)) AS unit_test_user_transactions_amount_dollars_sum_30d,
        |             SUM(amount_dollars) AS amount_dollars_sum
        |      FROM 
        |         (SELECT user, ts, ds, CAST(amount_rupees/70 as long) as amount_dollars from $rupeeTable
        |          WHERE ds >= '$monthAgo'
        |          UNION 
        |          SELECT user, ts, ds, amount_dollars from $dollarTable
        |          WHERE ds >= '$yearAgo' and ds <= '$dayAndMonthBefore') as transactions
        |      WHERE unix_timestamp(ds, 'yyyy-MM-dd')*1000 > ts
        |      GROUP BY user, ds)
        | SELECT queries.user_name,
        |        queries.ts,
        |        queries.ds,
        |        grouped_transactions.unit_test_user_transactions_amount_dollars_sum_30d
        | FROM queries left outer join grouped_transactions
        | ON queries.user_name = grouped_transactions.user
        | AND from_unixtime(queries.ts/1000, 'yyyy-MM-dd') = grouped_transactions.ds
        |""".stripMargin)
    val queries = tableUtils.sql(s"SELECT user_name, ts, ds from $queryTable where ds >= '$start'")
    println("showing left queries")
    queries.show()
    println("showing join result")
    computed.show()
    println("showing query result")
    expected.show()
    val diff = Comparison.sideBySide(computed, expected, List("user_name", "ts", "ds"))
    if (diff.count() > 0) {
      println(s"Actual count: ${computed.count()}")
      println(s"Expected count: ${expected.count()}")
      println(s"Diff count: ${diff.count()}")
      println(s"Queries count: ${queries.count()}")
      println(s"diff result rows")
      diff.show()
    }
    assertEquals(diff.count(), 0)
  }

  @Test
  def testEntitiesEntities(): Unit = {
    // untimned/unwindowed entities on right
    // right side
    val weightSchema = List(
      Column("user", StringType, 1000),
      Column("country", StringType, 100),
      Column("weight", DoubleType, 500)
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
      metaData = Builders.MetaData(name = "unit_test.country_weights", namespace = namespace, team = "team_a")
    )

    val heightSchema = List(
      Column("user", StringType, 1000),
      Column("country", StringType, 100),
      Column("height", LongType, 200)
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
      metaData = Builders.MetaData(name = "unit_test.country_heights", namespace = namespace, team = "team_b")
    )

    // left side
    val countrySchema = List(Column("country", StringType, 100))
    val countryTable = s"$namespace.countries"
    DataFrameGen.entities(spark, countrySchema, 1000, partitions = 400).save(countryTable)

    val start = Constants.Partition.minus(today, new Window(60, TimeUnit.DAYS))
    val end = Constants.Partition.minus(today, new Window(15, TimeUnit.DAYS))
    val joinConf = Builders.Join(
      left = Builders.Source.entities(Builders.Query(startPartition = start), snapshotTable = countryTable),
      joinParts = Seq(Builders.JoinPart(groupBy = weightGroupBy), Builders.JoinPart(groupBy = heightGroupBy)),
      metaData = Builders.MetaData(name = "test.country_features", namespace = namespace, team = "zipline")
    )

    val runner = new Join(joinConf, end, tableUtils)
    val computed = runner.computeJoin(Some(7))
    println(s"join start = $start")
    val expected = tableUtils.sql(s"""
    |WITH 
    |   countries AS (SELECT country, ds from $countryTable where ds >= '$start' and ds <= '$end'),
    |   grouped_weights AS (
    |      SELECT country, 
    |             ds, 
    |             avg(weight) as team_a_unit_test_country_weights_weight_average
    |      FROM $weightTable
    |      WHERE ds >= '$yearAgo' and ds <= '$dayAndMonthBefore'
    |      GROUP BY country, ds),
    |   grouped_heights AS (
    |      SELECT country, 
    |             ds, 
    |             avg(height) as team_b_unit_test_country_heights_height_average
    |      FROM $heightTable
    |      WHERE ds >= '$monthAgo'
    |      GROUP BY country, ds)
    |   SELECT countries.country,
    |        countries.ds,
    |        grouped_weights.team_a_unit_test_country_weights_weight_average,
    |        grouped_heights.team_b_unit_test_country_heights_height_average
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

  }

  @Test
  def testEventsEventsSnapshot(): Unit = {
    val viewsSchema = List(
      Column("user", StringType, 10000),
      Column("item", StringType, 100),
      Column("time_spent_ms", LongType, 5000)
    )

    val viewsTable = s"$namespace.view"
    DataFrameGen.events(spark, viewsSchema, count = 1000, partitions = 200).save(viewsTable)

    val viewsSource = Builders.Source.events(
      query = Builders.Query(selects = Builders.Selects("time_spent_ms"), startPartition = yearAgo),
      table = viewsTable
    )

    val viewsGroupBy = Builders.GroupBy(
      sources = Seq(viewsSource),
      keyColumns = Seq("item"),
      aggregations = Seq(
        Builders.Aggregation(operation = Operation.COUNT, inputColumn = "time_spent_ms"),
        Builders.Aggregation(operation = Operation.MIN, inputColumn = "ts"),
        Builders.Aggregation(operation = Operation.MAX, inputColumn = "ts")
      ),
      metaData = Builders.MetaData(name = "unit_test.item_views", namespace = namespace, team = "team_a")
    )

    // left side
    val itemQueries = List(Column("item", StringType, 100))
    val itemQueriesTable = s"$namespace.item_queries"
    DataFrameGen
      .events(spark, itemQueries, 1000, partitions = 100)
      .save(itemQueriesTable)

    val start = Constants.Partition.minus(today, new Window(100, TimeUnit.DAYS))

    val joinConf = Builders.Join(
      left = Builders.Source.events(Builders.Query(startPartition = start), table = itemQueriesTable),
      joinParts = Seq(Builders.JoinPart(groupBy = viewsGroupBy, prefix = "user", accuracy = Accuracy.SNAPSHOT)),
      metaData = Builders.MetaData(name = "test.item_snapshot_features", namespace = namespace, team = "zipline")
    )

    val join = new Join(joinConf = joinConf, endPartition = dayAndMonthBefore, tableUtils)
    val computed = join.computeJoin()
    computed.show()

    val expected = tableUtils.sql(s"""
                                |WITH 
                                |   queries AS (SELECT item, ts, ds from $itemQueriesTable where ds >= '$start' and ds <= '$dayAndMonthBefore')
                                | SELECT queries.item,
                                |        queries.ts,
                                |        queries.ds,
                                |        MIN(IF(CAST(queries.ts/(86400*1000) AS BIGINT) > CAST($viewsTable.ts/(86400*1000) AS BIGINT),  $viewsTable.ts, null)) as user_team_a_unit_test_item_views_ts_min,
                                |        MAX(IF(CAST(queries.ts/(86400*1000) AS BIGINT) > CAST($viewsTable.ts/(86400*1000) AS BIGINT),  $viewsTable.ts, null)) as user_team_a_unit_test_item_views_ts_max,
                                |        COUNT(IF(CAST(queries.ts/(86400*1000) AS BIGINT) > CAST($viewsTable.ts/(86400*1000) AS BIGINT), time_spent_ms, null)) as user_team_a_unit_test_item_views_time_spent_ms_count
                                | FROM queries left outer join $viewsTable
                                |  ON queries.item = $viewsTable.item
                                | WHERE $viewsTable.ds >= '$yearAgo' AND $viewsTable.ds <= '$dayAndMonthBefore'
                                | GROUP BY queries.item, queries.ts, queries.ds, from_unixtime(queries.ts/1000, 'yyyy-MM-dd')
                                |""".stripMargin)
    expected.show()

    val diff = Comparison.sideBySide(computed, expected, List("item", "ts", "ds"))

    if (diff.count() > 0) {
      println(s"Diff count: ${diff.count()}")
      println(s"diff result rows")
      diff
        .replaceWithReadableTime(Seq("ts", "a_user_ts_max", "b_user_ts_max"), dropOriginal = false)
        .show()
    }
    assertEquals(diff.count(), 0)
  }

  @Test
  def testEventsEventsTemporal(): Unit = {

    val joinConf = getEventsEventsTemporal()
    val viewsSchema = List(
      Column("user", StringType, 10000),
      Column("item", StringType, 100),
      Column("time_spent_ms", LongType, 5000)
    )

    val viewsTable = s"$namespace.view"
    DataFrameGen.events(spark, viewsSchema, count = 10000, partitions = 200).save(viewsTable, Map("tblProp1" -> "1"))

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
    val itemQueries = List(Column("item", StringType, 100))
    val itemQueriesTable = s"$namespace.item_queries"
    val itemQueriesDf = DataFrameGen
      .events(spark, itemQueries, 10000, partitions = 100)
    // duplicate the events
    itemQueriesDf.union(itemQueriesDf).save(itemQueriesTable) //.union(itemQueriesDf)

    val start = Constants.Partition.minus(today, new Window(100, TimeUnit.DAYS))

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
                                     |     WHERE $viewsTable.ds >= '$yearAgo' AND $viewsTable.ds <= '$dayAndMonthBefore'
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
        .replaceWithReadableTime(Seq("ts", "a_user_ts_max", "b_user_ts_max"), dropOriginal = true)
        .show()
    }
    assertEquals(diff.count(), 0)
  }

  @Test
  def testNoAgg(): Unit = {
    // Left side entities, right side entities no agg
    // Also testing specific select statement (rather than select *)
    val namesSchema = List(
      Column("user", StringType, 1000),
      Column("name", StringType, 500)
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
      metaData = Builders.MetaData(name = "unit_test.user_names", team = "zipline")
    )

    DataFrameGen
      .entities(spark, namesSchema, 1000, partitions = 400)
      .groupBy("user", "ds")
      .agg(Map("name" -> "max"))
      .save(namesTable)

    // left side
    val userSchema = List(Column("user", StringType, 100))
    val usersTable = s"$namespace.users"
    DataFrameGen.entities(spark, userSchema, 1000, partitions = 400).dropDuplicates().save(usersTable)

    val start = Constants.Partition.minus(today, new Window(60, TimeUnit.DAYS))
    val end = Constants.Partition.minus(today, new Window(15, TimeUnit.DAYS))
    val joinConf = Builders.Join(
      left = Builders.Source.entities(Builders.Query(selects = Map("user" -> null), startPartition = start),
                                      snapshotTable = usersTable),
      joinParts = Seq(Builders.JoinPart(groupBy = namesGroupBy)),
      metaData = Builders.MetaData(name = "test.user_features", namespace = namespace, team = "zipline")
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
    val joinConf = getEventsEventsTemporal()
    joinConf.getMetaData.setName(s"${joinConf.getMetaData.getName}_versioning")

    // Run the old join to ensure that tables exist
    val oldJoin = new Join(joinConf = joinConf, endPartition = dayAndMonthBefore, tableUtils)
    oldJoin.computeJoin(Some(100))

    // Make sure that there is no versioning-detected changes at this phase
    val joinPartsToRecomputeNoChange = oldJoin.getJoinPartsToRecompute(oldJoin.getLastRunJoinOpt)
    assertEquals(joinPartsToRecomputeNoChange.size, 0)

    // First test changing the left side table - this should trigger a full recompute
    val leftChangeJoinConf = joinConf.deepCopy()
    leftChangeJoinConf.getLeft.getEvents.setTable("some_other_table_name")
    val leftChangeJoin = new Join(joinConf = leftChangeJoinConf, endPartition = dayAndMonthBefore, tableUtils)
    val leftChangeRecompute = leftChangeJoin.getJoinPartsToRecompute(leftChangeJoin.getLastRunJoinOpt)
    assertEquals(leftChangeRecompute.size, 2)
    assertEquals(leftChangeRecompute.head.groupBy.getMetaData.getName, "unit_test.item_views")

    // Test adding a joinPart
    val addPartJoinConf = joinConf.deepCopy()
    val existingJoinPart = addPartJoinConf.getJoinParts.get(0)
    val newJoinPart = Builders.JoinPart(groupBy = getViewsGroupBy, prefix = "user_2", accuracy = Accuracy.TEMPORAL)
    addPartJoinConf.setJoinParts(Seq(existingJoinPart, newJoinPart).asJava)
    val addPartJoin = new Join(joinConf = addPartJoinConf, endPartition = dayAndMonthBefore, tableUtils)
    val addPartRecompute = addPartJoin.getJoinPartsToRecompute(addPartJoin.getLastRunJoinOpt)
    assertEquals(addPartRecompute.size, 1)
    assertEquals(addPartRecompute.head.groupBy.getMetaData.getName, "unit_test.item_views")
    // Compute to ensure that it works and to set the stage for the next assertion
    addPartJoin.computeJoin(Some(100))

    // Test modifying only one of two joinParts
    val rightModJoinConf = addPartJoinConf.deepCopy()
    rightModJoinConf.getJoinParts.get(1).setPrefix("user_3")
    val rightModJoin = new Join(joinConf = rightModJoinConf, endPartition = dayAndMonthBefore, tableUtils)
    val rightModRecompute = rightModJoin.getJoinPartsToRecompute(rightModJoin.getLastRunJoinOpt)
    assertEquals(rightModRecompute.size, 1)
    assertEquals(rightModRecompute.head.groupBy.getMetaData.getName, "unit_test.item_views")
    // Modify both
    rightModJoinConf.getJoinParts.get(0).setPrefix("user_4")
    val rightModBothJoin = new Join(joinConf = rightModJoinConf, endPartition = dayAndMonthBefore, tableUtils)
    // Compute to ensure that it works
    val computed = rightModBothJoin.computeJoin(Some(100))

    // Now assert that the actual output is correct after all these runs
    computed.show()
    val itemQueriesTable = joinConf.getLeft.getEvents.getTable
    val start = joinConf.getLeft.getEvents.getQuery.getStartPartition
    val viewsTable = s"$namespace.view"

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

  def getViewsGroupBy() = {
    val viewsSchema = List(
      Column("user", StringType, 10000),
      Column("item", StringType, 100),
      Column("time_spent_ms", LongType, 5000)
    )

    val viewsTable = s"$namespace.view"
    DataFrameGen.events(spark, viewsSchema, count = 10000, partitions = 200).save(viewsTable, Map("tblProp1" -> "1"))

    val viewsSource = Builders.Source.events(
      table = viewsTable,
      query = Builders.Query(selects = Builders.Selects("time_spent_ms"), startPartition = yearAgo)
    )
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
      metaData = Builders.MetaData(name = "unit_test.item_views", namespace = namespace, team = "item_team")
    )
  }

  def getEventsEventsTemporal() = {
    // left side
    val itemQueries = List(Column("item", StringType, 100))
    val itemQueriesTable = s"$namespace.item_queries"
    val itemQueriesDf = DataFrameGen
      .events(spark, itemQueries, 10000, partitions = 100)
    // duplicate the events
    itemQueriesDf.union(itemQueriesDf).save(itemQueriesTable) //.union(itemQueriesDf)

    val start = Constants.Partition.minus(today, new Window(100, TimeUnit.DAYS))

    Builders.Join(
      left = Builders.Source.events(Builders.Query(startPartition = start), table = itemQueriesTable),
      joinParts = Seq(Builders.JoinPart(groupBy = getViewsGroupBy, prefix = "user", accuracy = Accuracy.TEMPORAL)),
      metaData = Builders.MetaData(name = "test.item_temporal_features", namespace = namespace, team = "item_team")
    )

  }

}
