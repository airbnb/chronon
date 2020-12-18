package ai.zipline.spark.test

import ai.zipline.aggregator.base.{DoubleType, IntType, LongType, StringType}
import ai.zipline.api.Config.DataModel.{Entities, Events}
import ai.zipline.api.Config.{
  Accuracy,
  Aggregation,
  AggregationType,
  Constants,
  DataSource,
  JoinPart,
  MetaData,
  TimeUnit,
  Window,
  GroupBy => GroupByConf,
  Join => JoinConf
}
import ai.zipline.spark.Extensions._
import ai.zipline.spark.{Comparison, Join, TableUtils}
import junit.framework.TestCase
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{from_unixtime, to_date}
import org.apache.spark.sql.types.TimestampType
import org.junit.Assert._

// main test path for query generation - including the date scan logic
class JoinTest extends TestCase {
  val spark: SparkSession = SparkSessionBuilder.buildLocal("JoinTest")

  val today = Constants.Partition.at(System.currentTimeMillis())
  val monthAgo = Constants.Partition.minus(today, Window(30, TimeUnit.Days))
  val yearAgo = Constants.Partition.minus(today, Window(365, TimeUnit.Days))
  val dayAndMonthBefore = Constants.Partition.before(monthAgo)

  val namespace = "test_namespace"
  spark.sql(s"CREATE DATABASE IF NOT EXISTS $namespace")

  val tableUtils = TableUtils(spark)

  def testEventsEntitiesSnapshot: Unit = {

    val dollarTransactions = List(
      DataGen.Column("user", StringType, 1000),
      DataGen.Column("ts", LongType, 200),
      DataGen.Column("amount_dollars", LongType, 1000)
    )

    val rupeeTransactions = List(
      DataGen.Column("user", StringType, 1000),
      DataGen.Column("ts", LongType, 200),
      DataGen.Column("amount_rupees", LongType, 70000)
    )

    val dollarTable = s"$namespace.dollar_transactions"
    val rupeeTable = s"$namespace.rupee_transactions"
    DataGen.entities(spark, dollarTransactions, 1000000, partitions = 400).save(dollarTable)
    DataGen.entities(spark, rupeeTransactions, 100000, partitions = 30).save(rupeeTable)

    val dollarSource = DataSource(
      selects = Seq("ts", "amount_dollars"),
      table = dollarTable,
      dataModel = Entities,
      startPartition = yearAgo,
      endPartition = dayAndMonthBefore
    )

    val rupeeSource =
      DataSource(selects = Seq("ts", "CAST(amount_rupees/70 as long) as amount_dollars"),
                 table = rupeeTable,
                 dataModel = Entities,
                 startPartition = monthAgo)

    val groupBy = GroupByConf(
      sources = Seq(dollarSource, rupeeSource),
      keys = Seq("user"),
      aggregations = Seq(
        Aggregation(`type` = AggregationType.Sum,
                    inputColumn = "amount_dollars",
                    windows = Seq(Window(30, TimeUnit.Days), null))),
      metadata = MetaData(name = "user_transactions", team = "unit_test")
    )
    val queriesSchema = List(
      DataGen.Column("user", StringType, 10)
    )

    val queryTable = s"$namespace.queries"
    DataGen
      .events(spark, queriesSchema, 10000, partitions = 180)
      .withColumnRenamed("user", "user_name") // to test zipline renaming logic
      .save(queryTable)

    val start = Constants.Partition.minus(today, Window(60, TimeUnit.Days))
    val end = Constants.Partition.minus(today, Window(30, TimeUnit.Days))
    val joinConf = JoinConf(
      table = queryTable,
      dataModel = Events,
      startPartition = start,
      joinParts = Seq(JoinPart(groupBy = groupBy, keyRenaming = Map("user_name" -> "user"))),
      metadata = MetaData(name = "user_transaction_features", team = "test")
    )

    val runner1 = new Join(joinConf, end, namespace, tableUtils)

    val computed = runner1.computeJoin
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
        |          WHERE ds >= '${monthAgo}'
        |          UNION 
        |          SELECT user, ts, ds, amount_dollars from $dollarTable
        |          WHERE ds >= '${yearAgo}' and ds <= '${dayAndMonthBefore}') as transactions
        |      WHERE unix_timestamp(ds, 'yyyy-MM-dd')*1000 > ts
        |      GROUP BY user, ds)
        | SELECT queries.user_name,
        |        queries.ts,
        |        queries.ds,
        |        grouped_transactions.ds as ts_ds,
        |        grouped_transactions.amount_dollars_sum,
        |        grouped_transactions.amount_dollars_sum_30d
        | FROM queries left outer join grouped_transactions
        | ON queries.user_name = grouped_transactions.user
        | AND from_unixtime(queries.ts/1000, 'yyyy-MM-dd') = grouped_transactions.ds
        |""".stripMargin)
    val queries = tableUtils.sql(
      s"SELECT user_name, from_unixtime(ts/1000, 'yyyy-MM-dd') as ts_ds, ts, ds from $queryTable where ds >= '$start'")
    println("showing left queries")
    queries.show()
    println("showing join result")
    computed.show()
    println("showing query result")
    expected.show()
    val diff = Comparison.sideBySide(computed, expected, List("user_name", "ts_ds", "ts", "ds"))
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

  def testEntitiesEntities: Unit = {
    // untimned/unwindowed entities on right
    // right side
    val weightSchema = List(
      DataGen.Column("user", StringType, 1000),
      DataGen.Column("country", StringType, 1000),
      DataGen.Column("weight", DoubleType, 500)
    )
    val weightTable = s"$namespace.weights"
    DataGen.entities(spark, weightSchema, 1000000, partitions = 400).save(weightTable)

    val weightSource = DataSource(
      selects = Seq("weight"),
      table = weightTable,
      dataModel = Entities,
      startPartition = yearAgo,
      endPartition = dayAndMonthBefore
    )
    val weightGroupBy = GroupByConf(
      sources = Seq(weightSource),
      keys = Seq("country"),
      aggregations = Seq(Aggregation(`type` = AggregationType.Average, inputColumn = "weight")),
      metadata = MetaData(name = "country_weights", team = "unit_test")
    )

    val heightSchema = List(
      DataGen.Column("user", StringType, 1000),
      DataGen.Column("country", StringType, 1000),
      DataGen.Column("height", LongType, 200)
    )
    val heightTable = s"$namespace.heights"
    DataGen.entities(spark, heightSchema, 1000000, partitions = 400).save(heightTable)
    val heightSource = DataSource(
      selects = Seq("height"),
      table = heightTable,
      dataModel = Entities,
      startPartition = monthAgo
    )
    val heightGroupBy = GroupByConf(
      sources = Seq(heightSource),
      keys = Seq("country"),
      aggregations = Seq(Aggregation(`type` = AggregationType.Average, inputColumn = "height")),
      metadata = MetaData(name = "country_heights", team = "unit_test")
    )

    // left side
    val countrySchema = List(DataGen.Column("country", StringType, 1000))
    val countryTable = s"$namespace.countries"
    DataGen.entities(spark, countrySchema, 1000000, partitions = 400).save(countryTable)

    val start = Constants.Partition.minus(today, Window(60, TimeUnit.Days))
    val end = Constants.Partition.minus(today, Window(15, TimeUnit.Days))
    val joinConf = JoinConf(
      table = countryTable,
      dataModel = Entities,
      startPartition = start,
      joinParts = Seq(JoinPart(groupBy = weightGroupBy), JoinPart(groupBy = heightGroupBy)),
      metadata = MetaData(name = "country_features", team = "test")
    )

    val runner = new Join(joinConf, end, namespace, tableUtils)
    val computed = runner.computeJoin
    println(s"join start = $start")
    val expected = tableUtils.sql(s"""
    |WITH 
    |   countries AS (SELECT country, ds from $countryTable where ds >= '$start' and ds <= '$end'),
    |   grouped_weights AS (
    |      SELECT country, 
    |             ds, 
    |             avg(weight) as weight_average
    |      FROM $weightTable
    |      WHERE ds >= '${yearAgo}' and ds <= '${dayAndMonthBefore}'
    |      GROUP BY country, ds),
    |   grouped_heights AS (
    |      SELECT country, 
    |             ds, 
    |             avg(height) as height_average
    |      FROM $heightTable
    |      WHERE ds >= '${monthAgo}'
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

  def testEventsEventsSnapshot: Unit = {
    val viewsSchema = List(
      DataGen.Column("user", StringType, 10000),
      DataGen.Column("item", StringType, 1000),
      DataGen.Column("time_spent_ms", LongType, 5000)
    )

    val viewsTable = s"$namespace.view"
    DataGen.events(spark, viewsSchema, count = 10000, partitions = 200).save(viewsTable)

    val viewsSource = DataSource(
      selects = Seq("time_spent_ms"),
      table = viewsTable,
      dataModel = Events,
      startPartition = yearAgo
    )
    val viewsGroupBy = GroupByConf(
      sources = Seq(viewsSource),
      keys = Seq("item"),
      aggregations = Seq(
        Aggregation(`type` = AggregationType.Count, inputColumn = "time_spent_ms"),
        Aggregation(`type` = AggregationType.Min, inputColumn = "ts"),
        Aggregation(`type` = AggregationType.Max, inputColumn = "ts")
      ),
      metadata = MetaData(name = "item_views", team = "unit_test")
    )

    // left side
    val itemQueries = List(DataGen.Column("item", StringType, 1000))
    val itemQueriesTable = s"$namespace.item_queries"
    DataGen
      .events(spark, itemQueries, 10000, partitions = 100)
      .save(itemQueriesTable)

    val start = Constants.Partition.minus(today, Window(100, TimeUnit.Days))

    val joinConf = JoinConf(
      query =
        s"SELECT * FROM $itemQueriesTable WHERE ds >= ${Constants.StartPartitionMacro} AND ds <= ${Constants.EndPartitionMacro}",
      dataModel = Events,
      startPartition = start,
      joinParts = Seq(JoinPart(groupBy = viewsGroupBy, prefix = "user", accuracy = Accuracy.Snapshot)),
      metadata = MetaData(name = "country_features", team = "test")
    )

    val join = new Join(joinConf = joinConf, endPartition = dayAndMonthBefore, namespace, tableUtils)
    val computed = join.computeJoin
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

  def testEventsEventsTemporal: Unit = {
    val viewsSchema = List(
      DataGen.Column("user", StringType, 10000),
      DataGen.Column("item", StringType, 1000),
      DataGen.Column("time_spent_ms", LongType, 5000)
    )

    val viewsTable = s"$namespace.view"
    DataGen.events(spark, viewsSchema, count = 10000, partitions = 200).save(viewsTable)

    val viewsSource = DataSource(
      selects = Seq("time_spent_ms"),
      table = viewsTable,
      dataModel = Events,
      startPartition = yearAgo
    )
    val viewsGroupBy = GroupByConf(
      sources = Seq(viewsSource),
      keys = Seq("item"),
      aggregations = Seq(
        Aggregation(`type` = AggregationType.Average, inputColumn = "time_spent_ms")
//        Aggregation(`type` = AggregationType.Min, inputColumn = "ts"),
//        Aggregation(`type` = AggregationType.Max, inputColumn = "ts")
      ),
      metadata = MetaData(name = "item_views", team = "unit_test")
    )

    // left side
    val itemQueries = List(DataGen.Column("item", StringType, 1000))
    val itemQueriesTable = s"$namespace.item_queries"
    DataGen
      .events(spark, itemQueries, 10000, partitions = 100)
      .save(itemQueriesTable)

    val start = Constants.Partition.minus(today, Window(100, TimeUnit.Days))

    val joinConf = JoinConf(
      query =
        s"SELECT * FROM $itemQueriesTable WHERE ds >= ${Constants.StartPartitionMacro} AND ds <= ${Constants.EndPartitionMacro}",
      dataModel = Events,
      startPartition = start,
      joinParts = Seq(JoinPart(groupBy = viewsGroupBy, prefix = "user", accuracy = Accuracy.Temporal)),
      metadata = MetaData(name = "country_features", team = "test")
    )

    val join = new Join(joinConf = joinConf, endPartition = dayAndMonthBefore, namespace, tableUtils)
    val computed = join.computeJoin
    computed.show()

    val expected = tableUtils.sql(s"""
                                     |WITH 
                                     |   queries AS (SELECT item, ts, ds from $itemQueriesTable where ds >= '$start' and ds <= '$dayAndMonthBefore')
                                     | SELECT queries.item,
                                     |        queries.ts,
                                     |        queries.ds,
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
        .replaceWithReadableTime(Seq("ts"), dropOriginal = false)
        .show()
    }
    assertEquals(diff.count(), 0)
  }
}
