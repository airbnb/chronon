package ai.zipline.spark.test

import ai.zipline.aggregator.base.{DoubleType, LongType, StringType}
import ai.zipline.api.{Builders, _}
import ai.zipline.spark.Extensions._
import ai.zipline.spark.{Comparison, Join, SparkSessionBuilder, TableUtils}
import org.apache.spark.sql.SparkSession
import org.junit.Assert._
import org.junit.{AfterClass, BeforeClass, Test}

// clean needs to be a static method
object JoinTest {
  @BeforeClass
  @AfterClass
  def clean(): Unit = {
    SparkSessionBuilder.cleanData()
  }
}

// !!!DO NOT extend Junit.TestCase!!!
// Or the @BeforeClass and @AfterClass annotations fail to run
class JoinTest {

  val spark: SparkSession = SparkSessionBuilder.build("JoinTest", local = true)

  val today = Constants.Partition.at(System.currentTimeMillis())
  val monthAgo = Constants.Partition.minus(today, new Window(30, TimeUnit.DAYS))
  val yearAgo = Constants.Partition.minus(today, new Window(365, TimeUnit.DAYS))
  val dayAndMonthBefore = Constants.Partition.before(monthAgo)

  val namespace = "test_namespace_jointest"
  spark.sql(s"CREATE DATABASE IF NOT EXISTS $namespace")

  val tableUtils = TableUtils(spark)

  @Test
  def testEventsEntitiesSnapshot(): Unit = {

    val dollarTransactions = List(
      DataGen.Column("user", StringType, 100),
      DataGen.Column("ts", LongType, 200),
      DataGen.Column("amount_dollars", LongType, 1000)
    )

    val rupeeTransactions = List(
      DataGen.Column("user", StringType, 100),
      DataGen.Column("ts", LongType, 200),
      DataGen.Column("amount_rupees", LongType, 70000)
    )

    val dollarTable = s"$namespace.dollar_transactions"
    val rupeeTable = s"$namespace.rupee_transactions"
    DataGen.entities(spark, dollarTransactions, 10000, partitions = 400).save(dollarTable, Map("tblProp1" -> "1"))
    DataGen.entities(spark, rupeeTransactions, 1000, partitions = 30).save(rupeeTable)

    val dollarSource = Builders.Source.entities(
      query = Builders.Query(selects = Builders.Selects("ts", "amount_dollars"),
                             startPartition = yearAgo,
                             endPartition = dayAndMonthBefore),
      snapshotTable = dollarTable
    )

    val rupeeSource =
      Builders.Source.entities(
        query = Builders.Query(selects = Map("ts" -> "ts", "amount_dollars" -> "CAST(amount_rupees/70 as long)"),
                               startPartition = monthAgo),
        snapshotTable = rupeeTable
      )

    val groupBy = Builders.GroupBy(
      sources = Seq(dollarSource, rupeeSource),
      keyColumns = Seq("user"),
      aggregations = Seq(
        Builders.Aggregation(operation = Operation.SUM,
                             inputColumn = "amount_dollars",
                             windows = Seq(new Window(30, TimeUnit.DAYS)))),
      metaData = Builders.MetaData(name = "unit_test.user_transactions")
    )
    val queriesSchema = List(
      DataGen.Column("user", StringType, 100)
    )

    val queryTable = s"$namespace.queries"
    DataGen
      .events(spark, queriesSchema, 1000, partitions = 180)
      .withColumnRenamed("user", "user_name") // to test zipline renaming logic
      .save(queryTable)

    val start = Constants.Partition.minus(today, new Window(60, TimeUnit.DAYS))
    val end = Constants.Partition.minus(today, new Window(30, TimeUnit.DAYS))
    val joinConf = Builders.Join(
      left = Builders.Source.events(query = Builders.Query(startPartition = start), table = queryTable),
      joinParts = Seq(Builders.JoinPart(groupBy = groupBy, keyMapping = Map("user_name" -> "user"))),
      metaData = Builders.MetaData(name = "test.user_transaction_features")
    )

    val runner1 = new Join(joinConf, end, namespace, tableUtils)

    val computed = runner1.computeJoin(Some(3))
    println(s"join start = $start")

    val expected = spark.sql(s"""
        |WITH 
        |   queries AS (SELECT user_name, ts, ds from $queryTable where ds >= '$start' and ds <= '$end'),
        |   grouped_transactions AS (
        |      SELECT user, 
        |             ds, 
        |             SUM(IF(transactions.ts  >= (unix_timestamp(transactions.ds, 'yyyy-MM-dd') - (86400*30)) * 1000, amount_dollars, null)) AS amount_dollars_sum_30d,
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
        |        grouped_transactions.amount_dollars_sum_30d
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
      DataGen.Column("user", StringType, 1000),
      DataGen.Column("country", StringType, 100),
      DataGen.Column("weight", DoubleType, 500)
    )
    val weightTable = s"$namespace.weights"
    DataGen.entities(spark, weightSchema, 1000, partitions = 400).save(weightTable)

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
      metaData = Builders.MetaData(name = "unit_test.country_weights")
    )

    val heightSchema = List(
      DataGen.Column("user", StringType, 1000),
      DataGen.Column("country", StringType, 100),
      DataGen.Column("height", LongType, 200)
    )
    val heightTable = s"$namespace.heights"
    DataGen.entities(spark, heightSchema, 1000, partitions = 400).save(heightTable)
    val heightSource = Builders.Source.entities(
      query = Builders.Query(selects = Builders.Selects("height"), startPartition = monthAgo),
      snapshotTable = heightTable
    )

    val heightGroupBy = Builders.GroupBy(
      sources = Seq(heightSource),
      keyColumns = Seq("country"),
      aggregations = Seq(Builders.Aggregation(operation = Operation.AVERAGE, inputColumn = "height")),
      metaData = Builders.MetaData(name = "unit_test.country_heights")
    )

    // left side
    val countrySchema = List(DataGen.Column("country", StringType, 100))
    val countryTable = s"$namespace.countries"
    DataGen.entities(spark, countrySchema, 1000, partitions = 400).save(countryTable)

    val start = Constants.Partition.minus(today, new Window(60, TimeUnit.DAYS))
    val end = Constants.Partition.minus(today, new Window(15, TimeUnit.DAYS))
    val joinConf = Builders.Join(
      left = Builders.Source.entities(Builders.Query(startPartition = start), snapshotTable = countryTable),
      joinParts = Seq(Builders.JoinPart(groupBy = weightGroupBy), Builders.JoinPart(groupBy = heightGroupBy)),
      metaData = Builders.MetaData(name = "test.country_features")
    )

    val runner = new Join(joinConf, end, namespace, tableUtils)
    val computed = runner.computeJoin(Some(7))
    println(s"join start = $start")
    val expected = tableUtils.sql(s"""
    |WITH 
    |   countries AS (SELECT country, ds from $countryTable where ds >= '$start' and ds <= '$end'),
    |   grouped_weights AS (
    |      SELECT country, 
    |             ds, 
    |             avg(weight) as weight_average
    |      FROM $weightTable
    |      WHERE ds >= '$yearAgo' and ds <= '$dayAndMonthBefore'
    |      GROUP BY country, ds),
    |   grouped_heights AS (
    |      SELECT country, 
    |             ds, 
    |             avg(height) as height_average
    |      FROM $heightTable
    |      WHERE ds >= '$monthAgo'
    |      GROUP BY country, ds)
    |   SELECT countries.country,
    |        countries.ds,
    |        grouped_weights.weight_average,
    |        grouped_heights.height_average
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
      DataGen.Column("user", StringType, 10000),
      DataGen.Column("item", StringType, 100),
      DataGen.Column("time_spent_ms", LongType, 5000)
    )

    val viewsTable = s"$namespace.view"
    DataGen.events(spark, viewsSchema, count = 1000, partitions = 200).save(viewsTable)

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
      metaData = Builders.MetaData(name = "unit_test.item_views")
    )

    // left side
    val itemQueries = List(DataGen.Column("item", StringType, 100))
    val itemQueriesTable = s"$namespace.item_queries"
    DataGen
      .events(spark, itemQueries, 1000, partitions = 100)
      .save(itemQueriesTable)

    val start = Constants.Partition.minus(today, new Window(100, TimeUnit.DAYS))

    val joinConf = Builders.Join(
      left = Builders.Source.events(Builders.Query(startPartition = start), table = itemQueriesTable),
      joinParts = Seq(Builders.JoinPart(groupBy = viewsGroupBy, prefix = "user", accuracy = Accuracy.SNAPSHOT)),
      metaData = Builders.MetaData(name = "test.item_snapshot_features")
    )

    val join = new Join(joinConf = joinConf, endPartition = dayAndMonthBefore, namespace, tableUtils)
    val computed = join.computeJoin()
    computed.show()

    val expected = tableUtils.sql(s"""
                                |WITH 
                                |   queries AS (SELECT item, ts, ds from $itemQueriesTable where ds >= '$start' and ds <= '$dayAndMonthBefore')
                                | SELECT queries.item,
                                |        queries.ts,
                                |        queries.ds,
                                |        MIN(IF(CAST(queries.ts/(86400*1000) AS BIGINT) > CAST($viewsTable.ts/(86400*1000) AS BIGINT),  $viewsTable.ts, null)) as user_ts_min,
                                |        MAX(IF(CAST(queries.ts/(86400*1000) AS BIGINT) > CAST($viewsTable.ts/(86400*1000) AS BIGINT),  $viewsTable.ts, null)) as user_ts_max,
                                |        COUNT(IF(CAST(queries.ts/(86400*1000) AS BIGINT) > CAST($viewsTable.ts/(86400*1000) AS BIGINT), time_spent_ms, null)) as user_time_spent_ms_count 
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
    val viewsSchema = List(
      DataGen.Column("user", StringType, 10000),
      DataGen.Column("item", StringType, 100),
      DataGen.Column("time_spent_ms", LongType, 5000)
    )

    val viewsTable = s"$namespace.view"
    DataGen.events(spark, viewsSchema, count = 10000, partitions = 200).save(viewsTable, Map("tblProp1" -> "1"))

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
      metaData = Builders.MetaData(name = "unit_test.item_views")
    )

    // left side
    val itemQueries = List(DataGen.Column("item", StringType, 100))
    val itemQueriesTable = s"$namespace.item_queries"
    DataGen
      .events(spark, itemQueries, 10000, partitions = 100)
      .save(itemQueriesTable)

    val start = Constants.Partition.minus(today, new Window(100, TimeUnit.DAYS))

    val joinConf = Builders.Join(
      left = Builders.Source.events(Builders.Query(startPartition = start), table = itemQueriesTable),
      joinParts = Seq(Builders.JoinPart(groupBy = viewsGroupBy, prefix = "user", accuracy = Accuracy.TEMPORAL)),
      metaData = Builders.MetaData(name = "test.item_temporal_features  ")
    )

    val join = new Join(joinConf = joinConf, endPartition = dayAndMonthBefore, namespace, tableUtils)
    val computed = join.computeJoin(Some(100))
    computed.show()

    val expected = tableUtils.sql(s"""
                                     |WITH 
                                     |   queries AS (SELECT item, ts, ds from $itemQueriesTable where ds >= '$start' and ds <= '$dayAndMonthBefore')
                                     | SELECT queries.item,
                                     |        queries.ts,
                                     |        queries.ds,
                                     |        MIN(IF(queries.ts > $viewsTable.ts, $viewsTable.ts, null)) as user_ts_min,
                                     |        MAX(IF(queries.ts > $viewsTable.ts, $viewsTable.ts, null)) as user_ts_max,
                                     |        AVG(IF(queries.ts > $viewsTable.ts, time_spent_ms, null)) as user_time_spent_ms_average
                                     | FROM queries left outer join $viewsTable
                                     |  ON queries.item = $viewsTable.item
                                     | WHERE $viewsTable.ds >= '$yearAgo' AND $viewsTable.ds <= '$dayAndMonthBefore'
                                     | GROUP BY queries.item, queries.ts, queries.ds
                                     |""".stripMargin)
    expected.show()

    val diff = Comparison.sideBySide(computed, expected, List("item", "ts", "ds"))

    if (diff.count() > 0) {
      println(s"Diff count: ${diff.count()}")
      println(s"diff result rows")
      diff
        .replaceWithReadableTime(Seq("ts", "a_user_ts_max", "b_user_ts_max"), dropOriginal = true)
        .show()
    }
    assertEquals(diff.count(), 0)
  }
}
