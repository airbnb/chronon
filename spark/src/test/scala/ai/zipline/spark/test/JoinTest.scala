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
import ai.zipline.spark.{Join, TableUtils}
import junit.framework.TestCase
import org.apache.spark.sql.SparkSession

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
                    windows = Seq(Window(180, TimeUnit.Days), null))),
      metadata = MetaData(name = "user_transactions", team = "unit_test")
    )
    val queriesSchema = List(
      DataGen.Column("user_name", StringType, 10),
      DataGen.Column(Constants.TimeColumn, LongType, 180)
    )

    val queryTable = s"$namespace.queries"
    DataGen.events(spark, queriesSchema, 10000).withTimestampBasedPartition(Constants.PartitionColumn).save(queryTable)

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

    runner1.computeJoin.show()
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
