/*
 *    Copyright (C) 2023 The Chronon Authors.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package ai.chronon.spark.test

import org.slf4j.LoggerFactory
import ai.chronon.aggregator.windowing.TsUtils
import ai.chronon.api
import ai.chronon.api.Constants.ChrononMetadataKey
import ai.chronon.api.Extensions.{JoinOps, MetadataOps}
import ai.chronon.api._
import ai.chronon.online.Fetcher.{Request}
import ai.chronon.online.{MetadataStore, SparkConversions}
import ai.chronon.spark.Extensions._
import ai.chronon.spark.{Join => _, _}
import junit.framework.TestCase
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.junit.Assert.{assertEquals, assertTrue}

import java.lang
import java.util.TimeZone
import java.util.concurrent.Executors
import scala.collection.Seq
import scala.concurrent.ExecutionContext
import scala.util.ScalaJavaConversions._

class ChainingFetcherTest extends TestCase {
  @transient lazy val logger = LoggerFactory.getLogger(getClass)
  val sessionName = "ChainingFetcherTest"
  val spark: SparkSession = SparkSessionBuilder.build(sessionName, local = true)
  private val tableUtils = TableUtils(spark)
  TimeZone.setDefault(TimeZone.getTimeZone("UTC"))
  private val today = tableUtils.partitionSpec.at(System.currentTimeMillis())
  def toTs(arg: String): Long = TsUtils.datetimeToTs(arg)

  /**
    * This test group by is trying to get the latest rating of listings a user viewed in the last 7 days.
    * Parent Join: lasted price a certain user viewed
    * Chained Join: latest rating of the listings the user viewed in the last 7 days
    */
  def generateMutationData(namespace: String, accuracy: Accuracy): api.Join = {
    tableUtils.createDatabase(namespace)
    // left user views table
    // {listing, user, ts, ds}
    val viewsSchema = StructType(
      "listing_events_fetcher",
      Array(
        StructField("user", LongType),
        StructField("listing", LongType),
        StructField("ts", LongType),
        StructField("ds", StringType)
      )
    )
    val viewsData = Seq(
      Row(12L, 59L, toTs("2021-04-15 10:00:00"), "2021-04-15"),
      Row(12L, 1L, toTs("2021-04-15 08:00:00"), "2021-04-15"),
      Row(12L, 123L, toTs("2021-04-15 12:00:00"), "2021-04-15"),
      Row(88L, 1L, toTs("2021-04-15 11:00:00"), "2021-04-15"),
      Row(88L, 59L, toTs("2021-04-15 01:10:00"), "2021-04-15"),
      Row(88L, 456L, toTs("2021-04-15 12:00:00"), "2021-04-15")
    )
    // {listing, ts, rating, ds}
    val ratingSchema = StructType(
      "listing_ratings_fetcher",
      Array(StructField("listing", LongType),
            StructField("ts", LongType),
            StructField("rating", IntType),
            StructField("ds", StringType))
    )
    val ratingData = Seq(
      Row(1L, toTs("2021-04-13 00:30:00"), 3, "2021-04-13"),
      Row(1L, toTs("2021-04-15 09:30:00"), 4, "2021-04-15"),
      Row(59L, toTs("2021-04-13 00:30:00"), 5, "2021-04-13"),
      Row(59L, toTs("2021-04-15 02:30:00"), 6, "2021-04-15"),
      Row(123L, toTs("2021-04-13 01:40:00"), 7, "2021-04-13"),
      Row(123L, toTs("2021-04-14 03:40:00"), 8, "2021-04-14"),
      Row(456L, toTs("2021-04-15 20:45:00"), 10, "2021-04-15")
    )

    val sourceData: Map[StructType, Seq[Row]] = Map(
      viewsSchema -> viewsData,
      ratingSchema -> ratingData
    )

    sourceData.foreach {
      case (schema, rows) =>
        spark
          .createDataFrame(rows.toJava, SparkConversions.fromChrononSchema(schema))
          .save(s"$namespace.${schema.name}")

    }
    logger.info("saved all data hand written for fetcher test")

    val startPartition = "2021-04-13"
    val endPartition = "2021-04-16"
    val rightSource = Builders.Source.events(
      query = Builders.Query(
        selects = Map("listing" -> "listing", "ts" -> "ts", "rating" -> "rating"),
        startPartition = startPartition,
        endPartition = endPartition
      ),
      table = s"$namespace.${ratingSchema.name}",
      topic = "rating_test_topic"
    )

    val leftSource =
      Builders.Source.events(
        query = Builders.Query(
          selects = Builders.Selects("user", "listing", "ts"),
          startPartition = startPartition
        ),
        table = s"$namespace.${viewsSchema.name}"
      )

    val groupBy = Builders.GroupBy(
      sources = Seq(rightSource),
      keyColumns = Seq("listing"),
      aggregations = Seq(
        Builders.Aggregation(
          operation = Operation.LAST,
          inputColumn = "rating",
          windows = null
        )
      ),
      accuracy = accuracy,
      metaData = Builders.MetaData(name = "fetcher_parent_gb", namespace = namespace, team = "chronon")
    )

    val joinConf = Builders.Join(
      left = leftSource,
      joinParts = Seq(Builders.JoinPart(groupBy = groupBy)),
      metaData = Builders.MetaData(name = "fetcher_parent_join", namespace = namespace, team = "chronon")
    )
    joinConf
  }

  def generateChainingJoinData(namespace: String, accuracy: Accuracy): api.Join = {
    tableUtils.createDatabase(namespace)
    // User search listing event. Schema: user, listing, ts, ds
    val searchSchema = StructType(
      "user_search_listing_event",
      Array(StructField("user", LongType),
            StructField("listing", LongType),
            StructField("ts", LongType),
            StructField("ds", StringType))
    )

    val searchData = Seq(
      Row(12L, 59L, toTs("2021-04-18 10:00:00"), "2021-04-18"),
      Row(12L, 123L, toTs("2021-04-18 13:45:00"), "2021-04-18"),
      Row(88L, 1L, toTs("2021-04-18 00:10:00"), "2021-04-18"),
      Row(88L, 59L, toTs("2021-04-18 23:10:00"), "2021-04-18"),
      Row(88L, 456L, toTs("2021-04-18 03:10:00"), "2021-04-18"),
      Row(68L, 123L, toTs("2021-04-17 23:55:00"), "2021-04-18")
    ).toList

    TestUtils.makeDf(spark, searchSchema, searchData).save(s"$namespace.${searchSchema.name}")
    logger.info("Created user search table.")

    // construct chaining join
    val startPartition = "2021-04-14"
    val endPartition = "2021-04-18"

    val joinSource = generateMutationData(namespace, accuracy)
    val query = Builders.Query(startPartition = startPartition, endPartition = endPartition)
    val chainingGroupby = Builders.GroupBy(
      sources = Seq(Builders.Source.joinSource(joinSource, query)),
      keyColumns = Seq("user"),
      aggregations = Seq(
        Builders.Aggregation(operation = Operation.LAST_K,
                             argMap = Map("k" -> "10"),
                             inputColumn = "fetcher_parent_gb_rating_last",
                             windows = Seq(new Window(7, TimeUnit.DAYS)))
      ),
      metaData = Builders.MetaData(name = "chaining_gb", namespace = namespace),
      accuracy = accuracy
    )

    val leftSource =
      Builders.Source.events(
        query = Builders.Query(
          selects = Builders.Selects("user", "listing", "ts"),
          startPartition = startPartition,
          endPartition = endPartition
        ),
        table = s"$namespace.${searchSchema.name}"
      )

    Builders.Join(
      left = leftSource,
      joinParts = Seq(Builders.JoinPart(groupBy = chainingGroupby)),
      metaData = Builders.MetaData(name = "chaining_join", namespace = namespace, team = "chronon")
    )
  }

  def executeFetch(joinConf: api.Join, endDs: String, namespace: String): (DataFrame, Seq[Row]) = {
    implicit val executionContext: ExecutionContext = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(1))
    implicit val tableUtils: TableUtils = TableUtils(spark)
    val kvStoreFunc = () => OnlineUtils.buildInMemoryKVStore("ChainingFetcherTest")
    val inMemoryKvStore = kvStoreFunc()
    val mockApi = new MockApi(kvStoreFunc, namespace)

    val joinedDf = new ai.chronon.spark.Join(joinConf, endDs, tableUtils).computeJoin()
    val joinTable = s"$namespace.join_test_expected_${joinConf.metaData.cleanName}"
    joinedDf.save(joinTable)
    logger.info("=== Expected join table computed: === " + joinTable)
    joinedDf.show()

    val endDsExpected = tableUtils.sql(s"SELECT * FROM $joinTable WHERE ds='$endDs'")
    joinConf.joinParts.toScala.foreach(jp =>
      OnlineUtils.serve(tableUtils, inMemoryKvStore, kvStoreFunc, namespace, endDs, jp.groupBy))

    // Extract queries for the EndDs from the computedJoin results and eliminating computed aggregation values
    val endDsEvents = {
      tableUtils.sql(
        s"SELECT * FROM $joinTable WHERE ts >= unix_timestamp('$endDs', '${tableUtils.partitionSpec.format}')")
    }
    val endDsQueries = endDsEvents.drop(endDsEvents.schema.fieldNames.filter(_.contains("fetcher")): _*)
    logger.info("Queries:")
    endDsQueries.show()

    val keys = joinConf.leftKeyCols
    val keyIndices = keys.map(endDsQueries.schema.fieldIndex)
    val tsIndex = endDsQueries.schema.fieldIndex(Constants.TimeColumn)
    val metadataStore = new MetadataStore(inMemoryKvStore, timeoutMillis = 10000)
    inMemoryKvStore.create(ChrononMetadataKey)
    metadataStore.putJoinConf(joinConf)

    def buildRequests(lagMs: Int = 0): Array[Request] =
      endDsQueries.rdd
        .map { row =>
          val keyMap = keyIndices.indices.map { idx =>
            keys(idx) -> row.get(keyIndices(idx)).asInstanceOf[AnyRef]
          }.toMap
          val ts = row.get(tsIndex).asInstanceOf[Long]
          Request(joinConf.metaData.nameToFilePath, keyMap, Some(ts - lagMs))
        }
        .collect()

    val requests = buildRequests()

    //fetch
    val columns = endDsExpected.schema.fields.map(_.name)
    val responseRows: Seq[Row] =
      FetcherTestUtil.joinResponses(spark, requests, mockApi)._1.map { res =>
        val all: Map[String, AnyRef] =
          res.request.keys ++
            res.values.get ++
            Map(tableUtils.partitionColumn -> today) ++
            Map(Constants.TimeColumn -> new lang.Long(res.request.atMillis.get))
        val values: Array[Any] = columns.map(all.get(_).orNull)
        SparkConversions
          .toSparkRow(values, StructType.from("record", SparkConversions.toChrononSchema(endDsExpected.schema)))
          .asInstanceOf[GenericRow]
      }

    logger.info(endDsExpected.schema.pretty)
    (endDsExpected, responseRows)
  }

  // compare the result of fetched response with the expected result
  def compareTemporalFetch(joinConf: api.Join,
                           endDs: String,
                           expectedDf: DataFrame,
                           responseRows: Seq[Row],
                           ignoreCol: String): Unit = {
    // comparison
    val keys = joinConf.leftKeyCols
    val keyishColumns = keys.toList ++ List(tableUtils.partitionColumn, Constants.TimeColumn)
    val responseRdd = tableUtils.sparkSession.sparkContext.parallelize(responseRows.toSeq)
    var responseDf = tableUtils.sparkSession.createDataFrame(responseRdd, expectedDf.schema)
    if (endDs != today) {
      responseDf = responseDf.drop("ds").withColumn("ds", lit(endDs))
    }
    logger.info("expected:")
    expectedDf.show()
    logger.info("response:")
    responseDf.show()

    // remove user during comparison since `user` is not the key
    val diff = Comparison.sideBySide(responseDf.drop(ignoreCol),
                                     expectedDf.drop(ignoreCol),
                                     keyishColumns,
                                     aName = "online",
                                     bName = "offline")
    assertEquals(expectedDf.count(), responseDf.count())
    if (diff.count() > 0) {
      logger.info(s"Total count: ${responseDf.count()}")
      logger.info(s"Diff count: ${diff.count()}")
      logger.info(s"diff result rows:")
      diff
        .withTimeBasedColumn("ts_string", "ts", "yy-MM-dd HH:mm")
        .select("ts_string", diff.schema.fieldNames: _*)
        .show()
    }
    assertEquals(0, diff.count())
  }

  def testFetchParentJoin(): Unit = {
    val namespace = "parent_join_fetch"
    val joinConf = generateMutationData(namespace, Accuracy.TEMPORAL)
    val (expected, fetcherResponse) = executeFetch(joinConf, "2021-04-15", namespace)
    compareTemporalFetch(joinConf, "2021-04-15", expected, fetcherResponse, "user")
  }

  def testFetchChainingDeterministic(): Unit = {
    val namespace = "chaining_fetch"
    val chainingJoinConf = generateChainingJoinData(namespace, Accuracy.TEMPORAL)
    assertTrue(chainingJoinConf.joinParts.get(0).groupBy.sources.get(0).isSetJoinSource)

    val (expected, fetcherResponse) = executeFetch(chainingJoinConf, "2021-04-18", namespace)
    compareTemporalFetch(chainingJoinConf, "2021-04-18", expected, fetcherResponse, "listing")
  }
}
