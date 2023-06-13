package ai.chronon.spark.test

import ai.chronon.aggregator.test.Column
import ai.chronon.api
import ai.chronon.api.{Accuracy, Builders, Constants, Operation, TimeUnit, Window}
import ai.chronon.api.Constants.ChrononMetadataKey
import ai.chronon.api.Extensions._
import ai.chronon.online.Fetcher.{SeriesStatsResponse, StatsRequest}

import scala.compat.java8.FutureConverters
import ai.chronon.online.{JavaStatsRequest, MetadataStore}
import ai.chronon.spark.Extensions._
import ai.chronon.spark.{Analyzer, Join, SparkSessionBuilder, TableUtils}
import com.google.gson.GsonBuilder
import junit.framework.TestCase
import org.apache.spark.sql.SparkSession

import java.util.TimeZone
import java.util.concurrent.Executors
import scala.concurrent.duration.{Duration, SECONDS}
import scala.collection.JavaConverters._
import scala.concurrent.{Await, ExecutionContext}

/**
  * For testing of the consumption side of Stats end to end.
  *
  * Start by creating a join. Building the output table
  * Compute and serve stats. (OnlineUtils)
  * Fetch stats.
  */
class FetchStatsTest extends TestCase {

  val spark: SparkSession = SparkSessionBuilder.build("FetchStatsTest", local = true)
  val tableUtils = TableUtils(spark)
  val namespace = "fetch_stats"
  TimeZone.setDefault(TimeZone.getTimeZone("UTC"))
  private val today = Constants.Partition.at(System.currentTimeMillis())
  private val yesterday = Constants.Partition.before(today)

  def testFetchStats(): Unit = {
    // Part 1: Build the assets. Join definition, compute and serve stats.
    tableUtils.sql(s"CREATE DATABASE IF NOT EXISTS $namespace")
    val nameSuffix = "_fetch_stats_test"

    // LeftDf ->  item, value, ts, ds
    val itemQueries = List(
      Column("item", api.StringType, 100),
      Column("value", api.LongType, 100)
    )
    val itemQueriesTable = s"$namespace.item_queries_$nameSuffix"
    val itemQueriesDf = DataFrameGen
      .events(spark, itemQueries, 10000, partitions = 30)
    itemQueriesDf.save(s"${itemQueriesTable}_tmp")
    val leftDf = tableUtils.sql(s"SELECT item, value, ts, ds FROM ${itemQueriesTable}_tmp")
    leftDf.save(itemQueriesTable)
    val start = Constants.Partition.minus(today, new Window(10, TimeUnit.DAYS))

    // RightDf -> user, item, value
    val viewsSchema = List(
      Column("user", api.StringType, 10000),
      Column("item", api.StringType, 100),
      Column("value", api.LongType, 100)
    )
    val viewsTable = s"$namespace.view_$nameSuffix"
    val rightDf = DataFrameGen.events(spark, viewsSchema, count = 10000, partitions = 30)
    rightDf.save(viewsTable)

    // Build Group By
    val gb = Builders.GroupBy(
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
      metaData =
        Builders.MetaData(name = s"ut_fetch_stats.item_views_$nameSuffix", namespace = namespace, team = "item_team"),
      accuracy = Accuracy.SNAPSHOT
    )

    val joinConf = Builders.Join(
      left = Builders.Source.events(Builders.Query(startPartition = start), table = itemQueriesTable),
      joinParts = Seq(Builders.JoinPart(groupBy = gb, prefix = "user")),
      metaData = Builders.MetaData(name = s"ut_fetch_stats.item_temporal_features.$nameSuffix",
                                   namespace = namespace,
                                   team = "item_team",
                                   online = true)
    )

    // Compute daily join.
    val joinJob = new Join(joinConf, today, tableUtils)
    joinJob.computeJoin()
    // Load some data.
    implicit val executionContext: ExecutionContext = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(1))
    val kvStoreFunc = () => OnlineUtils.buildInMemoryKVStore("FetchStatsTest")
    val inMemoryKvStore = kvStoreFunc()
    val metadataStore = new MetadataStore(inMemoryKvStore, timeoutMillis = 10000)
    inMemoryKvStore.create(ChrononMetadataKey)
    metadataStore.putJoinConf(joinConf)
    OnlineUtils.serveStats(tableUtils, inMemoryKvStore, yesterday, joinConf)
    joinConf.joinParts.asScala.foreach(jp =>
      OnlineUtils.serve(tableUtils, inMemoryKvStore, kvStoreFunc, namespace, yesterday, jp.groupBy))

    // Part 2: Fetch. Build requests, and fetch.
    val request = StatsRequest(joinConf.metaData.nameToFilePath, None, None)
    val mockApi = new MockApi(kvStoreFunc, namespace)
    val gson = new GsonBuilder().setPrettyPrinting().serializeNulls().create()
    val javaFetchedSeries = fetchStatsSeries(request, mockApi, false)
    val fetchedSeries = fetchStatsSeries(request, mockApi, true)
    println(gson.toJson(fetchedSeries.values.get))

    // Appendix: Incremental run to check incremental updates for summary job.
    OnlineUtils.serveStats(tableUtils, inMemoryKvStore, today, joinConf)

    // Appendix: Test Analyzer output.
    val analyzer = new Analyzer(tableUtils,joinConf)
    analyzer.analyzeJoin(joinConf)
  }

  def fetchStatsSeries(request: StatsRequest,
                       mockApi: MockApi,
                       useJavaFetcher: Boolean = false,
                       debug: Boolean = false)(implicit ec: ExecutionContext): SeriesStatsResponse = {
    @transient lazy val fetcher = mockApi.buildFetcher(debug)
    @transient lazy val javaFetcher = mockApi.buildJavaFetcher()
    val results = if (useJavaFetcher) {
      val javaRequest = new JavaStatsRequest(request)
      val javaResponse = javaFetcher.fetchStatsTimeseries(javaRequest)
      FutureConverters.toScala(javaResponse).map(_.toScala)
    } else {
      fetcher.fetchStatsTimeseries(request)
    }
    Await.result(results, Duration(10000, SECONDS))
  }
}
