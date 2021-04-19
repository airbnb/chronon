package ai.zipline.spark.test

import ai.zipline.aggregator.base.{IntType, LongType, StringType}
import ai.zipline.aggregator.test.{CStream, Column, RowsWithSchema}
import ai.zipline.api.Extensions.{GroupByOps, MetadataOps}
import ai.zipline.api.{Builders, Constants, Operation, TimeUnit, Window}
import ai.zipline.fetcher.Fetcher
import ai.zipline.spark.{GroupBy, GroupByUpload, PartitionRange, SparkSessionBuilder, TableUtils}
import org.apache.spark.sql.SparkSession
import ai.zipline.spark.Extensions._
import junit.framework.TestCase

import java.util.concurrent.Executors
import scala.concurrent.duration.{Duration, MILLISECONDS}
import scala.concurrent.{Await, ExecutionContext}

class FetcherTest extends TestCase {
  val spark: SparkSession = SparkSessionBuilder.build("FetcherTest", local = true)

  private val namespace = "fetcher_test"
  spark.sql(s"CREATE DATABASE IF NOT EXISTS $namespace")

  def testTemporalFetch: Unit = {

    val paymentCols =
      Seq(Column("user", StringType, 100), Column("vendor", StringType, 10), Column("payment", LongType, 100))

    val ratingCols =
      Seq(Column("user", StringType, 100), Column("vendor", StringType, 10), Column("rating", IntType, 5))

    val paymentsTable = "payments_table"
    DataFrameGen.events(spark, paymentCols, 100000, 60).save(paymentsTable)
    val ratingsTable = "ratings_table"
    DataFrameGen.events(spark, ratingCols, 100000, 180).save(ratingsTable)

    val queryCols =
      Seq(Column("user_id", StringType, 100), Column("vendor_id", StringType, 10), Column("ts", LongType, 1))

    val vendorRatingsGroupBy = Builders.GroupBy(
      sources = Seq(Builders.Source.events(query = Builders.Query(), table = ratingsTable)),
      keyColumns = Seq("vendor"),
      aggregations = Seq(
        Builders.Aggregation(operation = Operation.AVERAGE,
                             inputColumn = "rating",
                             windows = Seq(new Window(2, TimeUnit.DAYS), new Window(30, TimeUnit.DAYS)))),
      metaData = Builders.MetaData(name = "unit_test.vendor_ratings", namespace = namespace)
    )

    val userPaymentsGroupBy = Builders.GroupBy(
      sources = Seq(Builders.Source.events(query = Builders.Query(), table = paymentsTable)),
      keyColumns = Seq("user"),
      aggregations = Seq(
        Builders.Aggregation(operation = Operation.APPROX_UNIQUE_COUNT,
                             inputColumn = "payment",
                             windows = Seq(new Window(1, TimeUnit.HOURS), new Window(14, TimeUnit.DAYS)))),
      metaData = Builders.MetaData(name = "unit_test.user_payments", namespace = namespace)
    )

    implicit val tableUtils = TableUtils(spark)
    val today = Constants.Partition.at(System.currentTimeMillis())
    println(today)
    val yesterday = Constants.Partition.before(today)
    GroupByUpload
      .run(userPaymentsGroupBy, today, Some(TableUtils(spark)))

    implicit val executionContext = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(1))
    val inMemoryKvStore = new InMemoryKvStore()

    inMemoryKvStore.bulkPut(userPaymentsGroupBy.kvTable, userPaymentsGroupBy.batchDataset, null)

    inMemoryKvStore.create(userPaymentsGroupBy.streamingDataset)
    val fetcher = new Fetcher(inMemoryKvStore)
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
