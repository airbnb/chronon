package ai.zipline.spark.test

import ai.zipline.aggregator.base.{IntType, LongType, StringType}
import ai.zipline.api.Config.DataModel.{Entities, Events}
import ai.zipline.api.Config.{
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
import org.apache.spark.sql.SparkSession
import org.junit.Assert._

// main test path for query generation - including the date scan logic
class JoinTest extends TestCase {
  lazy val spark: SparkSession = SparkSessionBuilder.buildLocal("JoinTest")

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

    val namespace = SparkSessionBuilder.namespace
    val dollarTable = s"$namespace.dollar_transactions"
    val rupeeTable = s"$namespace.rupee_transactions"
    DataGen.entities(spark, dollarTransactions, 1000000, partitions = 400).save(dollarTable)
    DataGen.entities(spark, rupeeTransactions, 100000, partitions = 30).save(rupeeTable)
    val today = Constants.Partition.at(System.currentTimeMillis())
    val monthAgo = Constants.Partition.minus(today, Window(30, TimeUnit.Days))
    val yearAgo = Constants.Partition.minus(today, Window(365, TimeUnit.Days))

    val dayAndMonthBefore = Constants.Partition.before(monthAgo)
    val dollarSource = DataSource(
      selects = Seq("user", "1 as amount_dollars"),
      table = dollarTable,
      dataModel = Entities,
      startPartition = yearAgo,
      endPartition = dayAndMonthBefore
    )

    val rupeeSource =
      DataSource(selects = Seq("user", "1 as amount_dollars"),
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
      DataGen.Column("user", StringType, 10),
      DataGen.Column(Constants.TimeColumn, LongType, 180)
    )

    val queryTable = s"$namespace.queries"
    DataGen
      .events(spark, queriesSchema, 10000)
      .withColumnRenamed("user", "user_name") // to test zipline renaming logic
      .withTimestampBasedPartition(Constants.PartitionColumn)
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

    val runner1 = new Join(joinConf, end, namespace, TableUtils(spark))

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
        |         (SELECT user, ts, ds, 1 as amount_dollars from $rupeeTable
        |          WHERE ds >= '${monthAgo}'
        |          UNION 
        |          SELECT user, ts, ds, 1 as amount_dollars from $dollarTable
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
    val queries = spark.sql(
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

  def entitiesEntities: Unit = {
    val eventSchema = List(
      DataGen.Column("user", StringType, 10),
      DataGen.Column(Constants.TimeColumn, LongType, 180),
      DataGen.Column("session_length", IntType, 10000)
    )

    val eventDf = DataGen.events(spark, eventSchema, 1000000)

  }

  def eventsEventsSnapshot: Unit = {
    val eventSchema = List(
      DataGen.Column("user", StringType, 10),
      DataGen.Column(Constants.TimeColumn, LongType, 180),
      DataGen.Column("session_length", IntType, 10000)
    )

    val eventDf = DataGen.events(spark, eventSchema, 1000000)

  }

  def eventsEventsTemporal: Unit = {
    val eventSchema = List(
      DataGen.Column("user", StringType, 10),
      DataGen.Column(Constants.TimeColumn, LongType, 180),
      DataGen.Column("session_length", IntType, 10000)
    )

    val eventDf = DataGen.events(spark, eventSchema, 1000000)

  }
}
