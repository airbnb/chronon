package ai.chronon.spark.test

import ai.chronon.aggregator.windowing.TsUtils
import ai.chronon.api
import ai.chronon.api.Constants.ChrononMetadataKey
import ai.chronon.api.Extensions.{JoinOps, MetadataOps}
import ai.chronon.api._
import ai.chronon.online.Fetcher.{Request, Response}
import ai.chronon.online.{JavaRequest, LoggableResponseBase64, MetadataStore, SparkConversions}
import ai.chronon.spark.Extensions._
import ai.chronon.spark.stats.ConsistencyJob
import ai.chronon.spark.{Join => _, _}
import junit.framework.TestCase
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.junit.Assert.{assertEquals, assertTrue}
import org.junit.Ignore

import java.lang
import java.util.TimeZone
import java.util.concurrent.Executors
import scala.collection.Seq
import scala.Console.println
import scala.compat.java8.FutureConverters
import scala.concurrent.duration.{Duration, SECONDS}
import scala.concurrent.{Await, ExecutionContext}
import scala.util.ScalaJavaConversions._

class ChainingFetcherTest extends TestCase {
  val sessionName = "FetcherTest"
  val spark: SparkSession = SparkSessionBuilder.build(sessionName, local = true)
  private val tableUtils = TableUtils(spark)
  TimeZone.setDefault(TimeZone.getTimeZone("UTC"))
  private val today = tableUtils.partitionSpec.at(System.currentTimeMillis())
  private val yesterday = tableUtils.partitionSpec.before(today)
  def toTs(arg: String): Long = TsUtils.datetimeToTs(arg)

  /**
    * This test group by is trying to get the latest rating of listings a user viewed in the last 7 days.
    */
  def generateMutationData(namespace: String, accuracy: Accuracy): api.Join = {
    spark.sql(s"CREATE DATABASE IF NOT EXISTS $namespace")
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
    println("saved all data hand written for fetcher test")

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
    spark.sql(s"CREATE DATABASE IF NOT EXISTS $namespace")
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
      Row(88L, 1L, toTs("2021-04-16 00:10:00"), "2021-04-16"),
      Row(88L, 59L, toTs("2021-04-18 23:10:00"), "2021-04-18"),
      Row(68L, 1L, toTs("2021-04-17 03:10:00"), "2021-04-17"),
      Row(68L, 123L, toTs("2021-04-18 23:55:00"), "2021-04-18")
    ).toList

    TestUtils.makeDf(spark, searchSchema, searchData).save(s"$namespace.${searchSchema.name}")
    println("Created user search table.")

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

  def joinResponses(requests: Array[Request],
                    mockApi: MockApi,
                    useJavaFetcher: Boolean = false,
                    runCount: Int = 1,
                    samplePercent: Double = -1,
                    logToHive: Boolean = false,
                    debug: Boolean = false)(implicit ec: ExecutionContext): (List[Response], DataFrame) = {
    val chunkSize = 100
    @transient lazy val fetcher = mockApi.buildFetcher(debug)
    @transient lazy val javaFetcher = mockApi.buildJavaFetcher()

    def fetchOnce = {
      var latencySum: Long = 0
      var latencyCount = 0
      val blockStart = System.currentTimeMillis()
      val result = requests.iterator
        .grouped(chunkSize)
        .map { r =>
          val responses = if (useJavaFetcher) {
            // Converting to java request and using the toScalaRequest functionality to test conversion
            val convertedJavaRequests = r.map(new JavaRequest(_)).toJava
            val javaResponse = javaFetcher.fetchJoin(convertedJavaRequests)
            FutureConverters
              .toScala(javaResponse)
              .map(
                _.toScala.map(jres =>
                  Response(
                    Request(jres.request.name, jres.request.keys.toScala.toMap, Option(jres.request.atMillis)),
                    jres.values.toScala.map(_.toScala)
                  )))
          } else {
            fetcher.fetchJoin(r)
          }
          System.currentTimeMillis() -> responses
        }
        .flatMap {
          case (start, future) =>
            val result = Await.result(future, Duration(10000, SECONDS)) // todo: change back to millis
            val latency = System.currentTimeMillis() - start
            latencySum += latency
            latencyCount += 1
            result
        }
        .toList
      val latencyMillis = latencySum.toFloat / latencyCount.toFloat
      val qps = (requests.length * 1000.0) / (System.currentTimeMillis() - blockStart).toFloat
      (latencyMillis, qps, result)
    }

    // to overwhelm the profiler with fetching code path
    // so as to make it prominent in the flamegraph & collect enough stats

    var latencySum = 0.0
    var qpsSum = 0.0
    var loggedValues: Seq[LoggableResponseBase64] = null
    var result: List[Response] = null
    (0 until runCount).foreach { _ =>
      val (latency, qps, resultVal) = fetchOnce
      result = resultVal
      loggedValues = mockApi.flushLoggedValues
      latencySum += latency
      qpsSum += qps
    }
    val fetcherNameString = if (useJavaFetcher) "Java" else "Scala"

    println(s"""
               |Averaging fetching stats for $fetcherNameString Fetcher over ${requests.length} requests $runCount times
               |with batch size: $chunkSize
               |average qps: ${qpsSum / runCount}
               |average latency: ${latencySum / runCount}
               |""".stripMargin)
    val loggedDf = mockApi.loggedValuesToDf(loggedValues, spark)
    if (logToHive) {
      TableUtils(spark).insertPartitions(
        loggedDf,
        mockApi.logTable,
        partitionColumns = Seq("ds", "name")
      )
    }
    if (samplePercent > 0) {
      println(s"logged count: ${loggedDf.count()}")
      loggedDf.show()
    }
    result -> loggedDf
  }

  def executeFetch(joinConf: api.Join,
                   endDs: String,
                   namespace: String): (DataFrame, Seq[Row]) = {
    implicit val executionContext: ExecutionContext = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(1))
    implicit val tableUtils: TableUtils = TableUtils(spark)
    val kvStoreFunc = () => OnlineUtils.buildInMemoryKVStore("ChainingFetcherTest")
    val inMemoryKvStore = kvStoreFunc()
    val mockApi = new MockApi(kvStoreFunc, namespace)

    val joinedDf = new ai.chronon.spark.Join(joinConf, endDs, tableUtils).computeJoin()
    val joinTable = s"$namespace.join_test_expected_${joinConf.metaData.cleanName}"
    joinedDf.save(joinTable)
    println("=== Expected join table computed: === " + joinTable)
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
    println("Queries:")
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
      joinResponses(requests, mockApi, debug = true)._1.map { res =>
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

    println(endDsExpected.schema.pretty)
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
    println("expected:")
    expectedDf.show()
    println("response:")
    responseDf.show()

    // remove user during comparison since `user` is not the key
    val diff = Comparison.sideBySide(responseDf.drop(ignoreCol),
                                     expectedDf.drop(ignoreCol),
                                     keyishColumns,
                                     aName = "online",
                                     bName = "offline")
    assertEquals(expectedDf.count(), responseDf.count())
    if (diff.count() > 0) {
      println(s"Total count: ${responseDf.count()}")
      println(s"Diff count: ${diff.count()}")
      println(s"diff result rows:")
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

  @Ignore
  //todo: test currently failing fetcher responses have queries != endDs
  def testFetchChainingDeterministic(): Unit = {
    val namespace = "chaining_fetch"
    val chainingJoinConf = generateChainingJoinData(namespace, Accuracy.TEMPORAL)
    assertTrue(chainingJoinConf.joinParts.get(0).groupBy.sources.get(0).isSetJoinSource)

    val (expected, fetcherResponse) = executeFetch(chainingJoinConf, "2021-04-18", namespace)
    compareTemporalFetch(chainingJoinConf, "2021-04-18", expected, fetcherResponse, "listing")
  }
}
