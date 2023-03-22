package ai.chronon.spark.test

import ai.chronon.aggregator.test.Column
import ai.chronon.api
import ai.chronon.api.{Accuracy, Builders, Constants, Operation, TimeUnit, Window}
import ai.chronon.api.Constants.ChrononMetadataKey
import ai.chronon.api.Extensions._
import ai.chronon.online.Fetcher.Request
import ai.chronon.spark.test.StreamingTest.buildInMemoryKvStore
import ai.chronon.online.MetadataStore
import ai.chronon.spark.Extensions._
import ai.chronon.spark.{Join, SparkSessionBuilder, TableUtils}
import com.fasterxml.jackson.databind.ObjectMapper
import com.google.gson.Gson
import junit.framework.TestCase
import org.apache.spark.sql.SparkSession

import java.util.TimeZone
import scala.concurrent.Await
import scala.concurrent.duration.{Duration, SECONDS}
import scala.collection.JavaConverters._
import com.fasterxml.jackson.module.scala.DefaultScalaModule

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
    /* Part 1: Build the assets. Join definition, compute and serve stats. */
    tableUtils.sql(s"CREATE DATABASE IF NOT EXISTS $namespace")
    val inMemoryKvStore = buildInMemoryKvStore()
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
      metaData = Builders.MetaData(name = s"unit_test_fetcher_stats.item_views_$nameSuffix",
                                   namespace = namespace,
                                   team = "item_team"),
      accuracy = Accuracy.SNAPSHOT
    )

    val joinConf = Builders.Join(
      left = Builders.Source.events(Builders.Query(startPartition = start), table = itemQueriesTable),
      joinParts = Seq(Builders.JoinPart(groupBy = gb, prefix = "user")),
      metaData = Builders.MetaData(name = s"test.item_temporal_features.$nameSuffix",
                                   namespace = namespace,
                                   team = "item_team",
                                   online = true)
    )

    // Compute daily join.
    val joinJob = new Join(joinConf, today, tableUtils)
    joinJob.computeJoin()
    // Load some data.
    val metadataStore = new MetadataStore(inMemoryKvStore, timeoutMillis = 10000)
    inMemoryKvStore.create(ChrononMetadataKey)
    metadataStore.putJoinConf(joinConf)
    joinConf.joinParts.asScala.foreach(jp =>
      OnlineUtils.serve(tableUtils, inMemoryKvStore, buildInMemoryKvStore, namespace, yesterday, jp.groupBy))
    OnlineUtils.serveStats(tableUtils, inMemoryKvStore, buildInMemoryKvStore, namespace, yesterday, joinConf)

    /* Part 2: Fetch. Build requests, and fetch. Compare with output table. */
    val mockApi = new MockApi(buildInMemoryKvStore, namespace)
    val fetcher = mockApi.buildFetcher(debug = true)
    val fetchStartTs = Constants.Partition.epochMillis(start)
    println(s"Fetch Start ts: $fetchStartTs")
    val futures =
      fetcher.fetchStats(joinConf.metaData.nameToFilePath, Some(fetchStartTs), Some(System.currentTimeMillis()))
    val result = Await.result(futures, Duration(10000, SECONDS))
    println(s"Test fetch: ")
    println(result)
    val statsMergedFutures = fetcher.fetchMergedStatsBetween(joinConf.metaData.nameToFilePath,
                                                             Some(fetchStartTs),
                                                             Some(Constants.Partition.epochMillis(yesterday)))
    val gson = new Gson()
    val statsMerged = Await.result(statsMergedFutures, Duration(10000, SECONDS))
    val mapper = new ObjectMapper()
    mapper.registerModule(DefaultScalaModule)
    val writer = mapper.writerWithDefaultPrettyPrinter
    println(s"Stats Merged: ${writer.writeValueAsString(statsMerged.values.get)}")
    println("Done!")
  }
}
