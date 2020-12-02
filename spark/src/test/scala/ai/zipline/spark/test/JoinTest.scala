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

// main test path for query generation - including the date scan logic
class JoinTest extends TestCase {
  lazy val spark: SparkSession = SparkSessionBuilder.build("JoinTest")

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

    val dollarSource = DataSource(
      selects = null,
      table = dollarTable,
      dataModel = Entities,
      startPartition = yearAgo,
      endPartition = Constants.Partition.before(monthAgo)
    )

    val rupeeSource =
      DataSource(selects = Seq("user", "amount_rupees/70 as amount_dollars"),
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

    val computed = runner1.computeJoin.dropDuplicates("user_name", "ds", "amount_dollars_sum")

    val expected = spark.sql(s"""
        |WITH 
        |   queries AS (SELECT user_name, from_unixtime(ts/1000, 'yyyy-MM-dd') as ts_ds, ts from $queryTable),
        |   transactions AS (SELECT user, ts, ds, 0 as amount_dollars from $rupeeTable 
        |                    UNION 
        |                    SELECT user, ts, ds, amount_dollars from $dollarTable)
        | SELECT queries.user_name, 
        |        queries.ts_ds,
        |        queries.ts,
        |        queries.ts_ds as ds,
        |        SUM(IF(transactions.ts  >= (unix_timestamp(queries.ts_ds, 'yyyy-MM-dd') - (86400*30)) * 1000, amount_dollars, null)) AS amount_dollars_sum_30d,
        |        SUM(amount_dollars) AS amount_dollars_sum
        | FROM queries join transactions
        | ON queries.user_name = transactions.user
        | AND queries.ts_ds = transactions.ds
        | GROUP BY queries.user_name, queries.ts_ds, queries.ts
        |""".stripMargin)

    computed.show()
    expected.show()
    val diff = Comparison.sideBySide(computed, expected, List("user_name", "ts_ds", "ts"))
    if (diff.count() > 0) {
      diff.show()
      println("diff result rows")
    }
    assert(true)
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
