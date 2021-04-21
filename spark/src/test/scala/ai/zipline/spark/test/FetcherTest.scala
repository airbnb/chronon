package ai.zipline.spark.test

import ai.zipline.aggregator.base.{IntType, LongType, StringType}
import ai.zipline.aggregator.test.{CStream, Column, RowsWithSchema}
import ai.zipline.api.Extensions.{GroupByOps, MetadataOps}
import ai.zipline.api.{Accuracy, Builders, Constants, Operation, TimeUnit, Window}
import ai.zipline.fetcher.Fetcher
import ai.zipline.spark.{GroupBy, GroupByUpload, Join, PartitionRange, SparkSessionBuilder, TableUtils}
import org.apache.spark.sql.SparkSession
import ai.zipline.spark.Extensions._
import junit.framework.TestCase
import org.apache.spark.sql.functions.avg

import java.util.concurrent.Executors
import scala.concurrent.duration.{Duration, MILLISECONDS}
import scala.concurrent.{Await, ExecutionContext}

class FetcherTest extends TestCase {
  val spark: SparkSession = SparkSessionBuilder.build("FetcherTest", local = true)

  private val namespace = "fetcher_test"
  spark.sql(s"CREATE DATABASE IF NOT EXISTS $namespace")

  def testTemporalFetch: Unit = {

    implicit val executionContext = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(1))
    implicit val tableUtils = TableUtils(spark)
    val inMemoryKvStore = new InMemoryKvStore()
    val fetcher = new Fetcher(inMemoryKvStore)

    val today = Constants.Partition.at(System.currentTimeMillis())
    val yesterday = Constants.Partition.before(today)
    val twoDaysAgo = Constants.Partition.before(yesterday)

    // temporal events
    val paymentCols =
      Seq(Column("user", StringType, 100), Column("vendor", StringType, 10), Column("payment", LongType, 100))
    val paymentsTable = "payments_table"
    DataFrameGen.events(spark, paymentCols, 100000, 60).save(paymentsTable)
    val userPaymentsGroupBy = Builders.GroupBy(
      sources = Seq(Builders.Source.events(query = Builders.Query(), table = paymentsTable)),
      keyColumns = Seq("user"),
      aggregations = Seq(
        Builders.Aggregation(operation = Operation.APPROX_UNIQUE_COUNT,
                             inputColumn = "payment",
                             windows = Seq(new Window(1, TimeUnit.HOURS), new Window(14, TimeUnit.DAYS)))),
      metaData = Builders.MetaData(name = "unit_test.user_payments", namespace = namespace)
    )
    GroupByUpload.run(userPaymentsGroupBy, yesterday, Some(tableUtils))
    inMemoryKvStore.bulkPut(userPaymentsGroupBy.kvTable, userPaymentsGroupBy.batchDataset, null)
    inMemoryKvStore.create(userPaymentsGroupBy.streamingDataset)

    // snapshot events
    val ratingCols =
      Seq(Column("user", StringType, 100), Column("vendor", StringType, 10), Column("rating", IntType, 5))
    val ratingsTable = "ratings_table"
    DataFrameGen.events(spark, ratingCols, 100000, 180).save(ratingsTable)
    val vendorRatingsGroupBy = Builders.GroupBy(
      sources = Seq(Builders.Source.events(query = Builders.Query(), table = ratingsTable)),
      keyColumns = Seq("vendor"),
      aggregations = Seq(
        Builders.Aggregation(operation = Operation.AVERAGE,
                             inputColumn = "rating",
                             windows = Seq(new Window(2, TimeUnit.DAYS), new Window(30, TimeUnit.DAYS)))),
      metaData = Builders.MetaData(name = "unit_test.vendor_ratings", namespace = namespace),
      accuracy = Accuracy.SNAPSHOT
    )
    GroupByUpload.run(vendorRatingsGroupBy, yesterday, Some(tableUtils))
    inMemoryKvStore.bulkPut(vendorRatingsGroupBy.kvTable, vendorRatingsGroupBy.batchDataset, null)

    // no-agg
    val userBalanceCols =
      Seq(Column("user", StringType, 100), Column("balance", IntType, 5000))
    val balanceTable = "balance_table"
    DataFrameGen
      .entities(spark, userBalanceCols, 100000, 180)
      .groupBy("user", "ds")
      .agg(avg("balance") as "avg_balance")
      .save(balanceTable)
    val userBalanceGroupBy = Builders.GroupBy(
      sources = Seq(Builders.Source.entities(query = Builders.Query(), snapshotTable = balanceTable)),
      keyColumns = Seq("user"),
      metaData = Builders.MetaData(name = "unit_test.user_balance", namespace = namespace)
    )
    GroupByUpload.run(userBalanceGroupBy, yesterday, Some(tableUtils))
    inMemoryKvStore.bulkPut(userBalanceGroupBy.kvTable, userBalanceGroupBy.batchDataset, null)

    // snapshot-entities
    val userVendorCreditCols =
      Seq(Column("account", StringType, 100),
          Column("vendor", StringType, 10), // will be renamed
          Column("credit", IntType, 500),
          Column("ts", LongType, 100))
    val creditTable = "credit_table"
    DataFrameGen
      .entities(spark, userVendorCreditCols, 100000, 100)
      .withColumnRenamed("vendor", "vendor_id")
      .save(creditTable)
    val creditGroupBy = Builders.GroupBy(
      sources = Seq(Builders.Source.entities(query = Builders.Query(), snapshotTable = creditTable)),
      keyColumns = Seq("vendor_id"),
      aggregations = Seq(
        Builders.Aggregation(operation = Operation.SUM,
                             inputColumn = "credit",
                             windows = Seq(new Window(2, TimeUnit.DAYS), new Window(30, TimeUnit.DAYS)))),
      metaData = Builders.MetaData(name = "unit_test.vendor_credit", namespace = namespace)
    )
    GroupByUpload.run(creditGroupBy, yesterday, Some(tableUtils))
    inMemoryKvStore.bulkPut(creditGroupBy.kvTable, creditGroupBy.batchDataset, null)

    // queries
    val queryCols =
      Seq(Column("user", StringType, 100), Column("vendor", StringType, 10))
    val queriesTable = "queries_table"
    DataFrameGen
      .events(spark, queryCols, 100000, 4)
      .withColumnRenamed("user", "user_id")
      .withColumnRenamed("vendor", "vendor_id")
      .save(queriesTable)
    val joinConf = Builders.Join(
      left = Builders.Source.events(Builders.Query(startPartition = twoDaysAgo), table = queriesTable),
      joinParts = Seq(
        Builders.JoinPart(groupBy = userPaymentsGroupBy, keyMapping = Map("user_id" -> "user")),
        Builders.JoinPart(groupBy = vendorRatingsGroupBy, keyMapping = Map("vendor_id" -> "vendor")),
        Builders.JoinPart(groupBy = userBalanceGroupBy, keyMapping = Map("user_id" -> "user")),
        Builders.JoinPart(groupBy = creditGroupBy, prefix = "b"),
        Builders.JoinPart(groupBy = creditGroupBy, prefix = "a")
      ),
      metaData = Builders.MetaData(name = "test.payments_join", namespace = namespace, team = "zipline")
    )
    val joinedDf = new Join(joinConf, today, tableUtils).computeJoin()
    joinedDf.show()

    inMemoryKvStore.bulkPut(userPaymentsGroupBy.kvTable, userPaymentsGroupBy.batchDataset, null)
    inMemoryKvStore.create(userPaymentsGroupBy.streamingDataset)
    val future =
      fetcher.fetchGroupBys(Seq(Fetcher.Request(userPaymentsGroupBy.metaData.cleanName, Map("user" -> "user58"))))
    val result = Await.result(future, Duration(100, MILLISECONDS))
    println(result)

    // create groupBy data

    // create queries
    // sawtooth aggregate batch data
    // split "streaming" data
    // bulk upload batch data

    // merge queries + streaming data and replay to inmemory store
    // streaming data gets "put", queries do "get"

  }
}
