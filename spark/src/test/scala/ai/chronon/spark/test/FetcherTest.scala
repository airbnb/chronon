package ai.chronon.spark.test

import ai.chronon.aggregator.test.Column
import ai.chronon.aggregator.windowing.TsUtils
import ai.chronon.api
import ai.chronon.api.Constants.ChrononMetadataKey
import ai.chronon.api.Extensions.{JoinOps, MetadataOps}
import ai.chronon.api._
import ai.chronon.online.Fetcher.{Request, Response}
import ai.chronon.online.KVStore.GetRequest
import ai.chronon.online.{Api, JavaRequest, LoggableResponseBase64, MetadataStore, SparkConversions}
import ai.chronon.spark.Extensions._
import ai.chronon.spark.stats.ConsistencyJob
import ai.chronon.spark.test.TestUtils.generateRandomData
import ai.chronon.spark.{Join => _, _}
import junit.framework.TestCase
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.apache.spark.sql.functions.{avg, col, lit}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.junit.Assert.{assertEquals, assertFalse, assertTrue}

import java.lang
import java.util.TimeZone
import java.util.concurrent.Executors
import scala.collection.JavaConverters._
import scala.collection.Seq
import scala.compat.java8.FutureConverters
import scala.concurrent.duration.{Duration, SECONDS}
import scala.concurrent.{Await, ExecutionContext}
import scala.io.Source
import scala.util.ScalaVersionSpecificCollectionsConverter

class FetcherTest extends TestCase {
  val sessionName = "FetcherTest"
  val spark: SparkSession = SparkSessionBuilder.build(sessionName, local = true)
  private val tableUtils = TableUtils(spark)
  private val topic = "test_topic"
  TimeZone.setDefault(TimeZone.getTimeZone("UTC"))
  private val today = tableUtils.partitionSpec.at(System.currentTimeMillis())
  private val yesterday = tableUtils.partitionSpec.before(today)

  def testMetadataStore(): Unit = {
    implicit val executionContext: ExecutionContext = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(1))
    implicit val tableUtils: TableUtils = TableUtils(spark)

    val joinPath = "joins/team/example_join.v1"
    val confResource = getClass.getResource(s"/$joinPath")
    val src = Source.fromFile(confResource.getPath)

    val expected = {
      try src.mkString
      finally src.close()
    }.replaceAll("\\s+", "")

    val inMemoryKvStore = OnlineUtils.buildInMemoryKVStore("FetcherTest")
    val singleFileDataSet = ChrononMetadataKey + "_single_file_test"
    val singleFileMetadataStore = new MetadataStore(inMemoryKvStore, singleFileDataSet, timeoutMillis = 10000)
    inMemoryKvStore.create(singleFileDataSet)
    // set the working directory to /chronon instead of $MODULE_DIR in configuration if Intellij fails testing
    val singleFilePut = singleFileMetadataStore.putConf(confResource.getPath)
    Await.result(singleFilePut, Duration.Inf)
    val response = inMemoryKvStore.get(GetRequest(joinPath.getBytes(), singleFileDataSet))
    val res = Await.result(response, Duration.Inf)
    assertTrue(res.latest.isSuccess)
    val actual = new String(res.values.get.head.bytes)

    assertEquals(expected, actual.replaceAll("\\s+", ""))

    val directoryDataSetDataSet = ChrononMetadataKey + "_directory_test"
    val directoryMetadataStore = new MetadataStore(inMemoryKvStore, directoryDataSetDataSet, timeoutMillis = 10000)
    inMemoryKvStore.create(directoryDataSetDataSet)
    val directoryPut = directoryMetadataStore.putConf(confResource.getPath.replace(s"/$joinPath", ""))
    Await.result(directoryPut, Duration.Inf)
    val dirResponse =
      inMemoryKvStore.get(GetRequest(joinPath.getBytes(), directoryDataSetDataSet))
    val dirRes = Await.result(dirResponse, Duration.Inf)
    assertTrue(dirRes.latest.isSuccess)
    val dirActual = new String(dirRes.values.get.head.bytes)

    assertEquals(expected, dirActual.replaceAll("\\s+", ""))

    val emptyResponse =
      inMemoryKvStore.get(GetRequest("NoneExistKey".getBytes(), "NonExistDataSetName"))
    val emptyRes = Await.result(emptyResponse, Duration.Inf)
    assertFalse(emptyRes.latest.isSuccess)
  }

  /**
    * Generate deterministic data for testing and checkpointing IRs and streaming data.
    */
  def generateMutationData(namespace: String): api.Join = {
    spark.sql(s"CREATE DATABASE IF NOT EXISTS $namespace")
    def toTs(arg: String): Long = TsUtils.datetimeToTs(arg)
    val eventData = Seq(
      Row(595125622443733822L, toTs("2021-04-10 09:00:00"), "2021-04-10"),
      Row(595125622443733822L, toTs("2021-04-10 23:00:00"), "2021-04-10"), // Query for added event
      Row(595125622443733822L, toTs("2021-04-10 23:45:00"), "2021-04-10"), // Query for mutated event
      Row(1L, toTs("2021-04-10 00:10:00"), "2021-04-10"), // query for added event
      Row(1L, toTs("2021-04-10 03:10:00"), "2021-04-10") // query for mutated event
    )
    val snapshotData = Seq(
      Row(1L, toTs("2021-04-04 00:30:00"), 4, "2021-04-08"),
      Row(1L, toTs("2021-04-04 12:30:00"), 4, "2021-04-08"),
      Row(1L, toTs("2021-04-05 00:30:00"), 4, "2021-04-08"),
      Row(1L, toTs("2021-04-08 02:30:00"), 4, "2021-04-08"),
      Row(595125622443733822L, toTs("2021-04-04 01:40:00"), 3, "2021-04-08"),
      Row(595125622443733822L, toTs("2021-04-05 03:40:00"), 3, "2021-04-08"),
      Row(595125622443733822L, toTs("2021-04-06 03:45:00"), 4, "2021-04-08"),
      // {listing_id, ts, rating, ds}
      Row(1L, toTs("2021-04-04 00:30:00"), 4, "2021-04-09"),
      Row(1L, toTs("2021-04-04 12:30:00"), 4, "2021-04-09"),
      Row(1L, toTs("2021-04-05 00:30:00"), 4, "2021-04-09"),
      Row(1L, toTs("2021-04-08 02:30:00"), 4, "2021-04-09"),
      Row(595125622443733822L, toTs("2021-04-04 01:40:00"), 3, "2021-04-09"),
      Row(595125622443733822L, toTs("2021-04-05 03:40:00"), 3, "2021-04-09"),
      Row(595125622443733822L, toTs("2021-04-06 03:45:00"), 4, "2021-04-09"),
      Row(595125622443733822L, toTs("2021-04-09 05:45:00"), 5, "2021-04-09")
    )
    val mutationData = Seq(
      Row(595125622443733822L, toTs("2021-04-09 05:45:00"), 2, "2021-04-09", toTs("2021-04-09 05:45:00"), false),
      Row(595125622443733822L, toTs("2021-04-09 05:45:00"), 2, "2021-04-09", toTs("2021-04-09 07:00:00"), true),
      Row(595125622443733822L, toTs("2021-04-09 05:45:00"), 5, "2021-04-09", toTs("2021-04-09 07:00:00"), false),
      // {listing_id, ts, rating, ds, mutation_ts, is_before}
      Row(1L, toTs("2021-04-10 00:30:00"), 5, "2021-04-10", toTs("2021-04-10 00:30:00"), false),
      Row(1L, toTs("2021-04-10 00:30:00"), 5, "2021-04-10", toTs("2021-04-10 02:30:00"), true), // mutation delete event
      Row(595125622443733822L, toTs("2021-04-10 10:00:00"), 4, "2021-04-10", toTs("2021-04-10 10:00:00"), false),
      Row(595125622443733822L, toTs("2021-04-10 10:00:00"), 4, "2021-04-10", toTs("2021-04-10 23:30:00"), true),
      Row(595125622443733822L, toTs("2021-04-10 10:00:00"), 3, "2021-04-10", toTs("2021-04-10 23:30:00"), false)
    )
    // Schemas
    val snapshotSchema = StructType(
      "listing_ratings_snapshot_fetcher",
      Array(StructField("listing", LongType),
            StructField("ts", LongType),
            StructField("rating", IntType),
            StructField("ds", StringType))
    )

    // {..., mutation_ts (timestamp of mutation), is_before (previous value or the updated value),...}
    // Change the names to make sure mappings work properly
    val mutationSchema = StructType(
      "listing_ratings_mutations_fetcher",
      snapshotSchema.fields ++ Seq(
        StructField("mutation_time", LongType),
        StructField("is_before_reversal", BooleanType)
      )
    )

    // {..., event (generic event column), ...}
    val eventSchema = StructType("listing_events_fetcher",
                                 Array(
                                   StructField("listing_id", LongType),
                                   StructField("ts", LongType),
                                   StructField("ds", StringType)
                                 ))

    val sourceData: Map[StructType, Seq[Row]] = Map(
      eventSchema -> eventData,
      mutationSchema -> mutationData,
      snapshotSchema -> snapshotData
    )

    sourceData.foreach {
      case (schema, rows) =>
        spark
          .createDataFrame(rows.asJava, SparkConversions.fromChrononSchema(schema))
          .save(s"$namespace.${schema.name}")

    }
    println("saved all data hand written for fetcher test")

    val startPartition = "2021-04-08"
    val endPartition = "2021-04-10"
    val rightSource = Builders.Source.entities(
      query = Builders.Query(
        selects = Map("listing_id" -> "listing", "ts" -> "ts", "rating" -> "rating"),
        startPartition = startPartition,
        endPartition = endPartition,
        mutationTimeColumn = "mutation_time",
        reversalColumn = "is_before_reversal"
      ),
      snapshotTable = s"$namespace.${snapshotSchema.name}",
      mutationTable = s"$namespace.${mutationSchema.name}",
      mutationTopic = "blank"
    )

    val leftSource =
      Builders.Source.events(
        query = Builders.Query(
          selects = Builders.Selects("listing_id", "ts"),
          startPartition = startPartition
        ),
        table = s"$namespace.${eventSchema.name}"
      )

    val groupBy = Builders.GroupBy(
      sources = Seq(rightSource),
      keyColumns = Seq("listing_id"),
      aggregations = Seq(
        Builders.Aggregation(
          operation = Operation.SUM,
          inputColumn = "rating",
          windows = null
        ),
        Builders.Aggregation(
          operation = Operation.AVERAGE,
          inputColumn = "rating",
          windows = Seq(new Window(1, TimeUnit.DAYS))
        ),
        Builders.Aggregation(
          operation = Operation.APPROX_HISTOGRAM_K,
          inputColumn = "rating",
          windows = Seq(new Window(1, TimeUnit.DAYS))
        )
      ),
      accuracy = Accuracy.TEMPORAL,
      metaData = Builders.MetaData(name = "unit_test/fetcher_mutations_gb", namespace = namespace, team = "chronon")
    )

    val joinConf = Builders.Join(
      left = leftSource,
      joinParts = Seq(Builders.JoinPart(groupBy = groupBy)),
      metaData = Builders.MetaData(name = "unit_test/fetcher_mutations_join", namespace = namespace, team = "chronon")
    )
    joinConf
  }

  def generateEventOnlyData(namespace: String, groupByCustomJson: Option[String] = None): api.Join = {
    spark.sql(s"CREATE DATABASE IF NOT EXISTS $namespace")

    def toTs(arg: String): Long = TsUtils.datetimeToTs(arg)

    val listingEventData = Seq(
      Row(1L, toTs("2021-04-10 03:10:00"), "2021-04-10"),
      Row(2L, toTs("2021-04-10 03:10:00"), "2021-04-10")
    )
    val ratingEventData = Seq(
      // 1L listing id event data
      Row(1L, toTs("2021-04-08 00:30:00"), 2, "2021-04-08"),
      Row(1L, toTs("2021-04-09 05:35:00"), 4, "2021-04-09"),
      Row(1L, toTs("2021-04-10 02:30:00"), 4, "2021-04-10"),
      Row(1L, toTs("2021-04-10 02:30:00"), 6, "2021-04-10"),
      Row(1L, toTs("2021-04-10 02:30:00"), 7, "2021-04-10"),
      Row(1L, toTs("2021-04-10 02:30:00"), 10, "2021-04-10"),
      // 2L listing id event data
      Row(2L, toTs("2021-04-06 00:30:00"), 10, "2021-04-06"), // excluded from all aggs with start partition 4/7
      Row(2L, toTs("2021-04-06 00:30:00"), 10, "2021-04-06"), // excluded from all aggs with start partition 4/7
      Row(2L, toTs("2021-04-07 00:30:00"), 10, "2021-04-07"), // excluded from avg agg
      Row(2L, toTs("2021-04-07 00:30:00"), 10, "2021-04-07"), // excluded from avg agg
      Row(2L, toTs("2021-04-08 00:30:00"), 2, "2021-04-08"),
      Row(2L, toTs("2021-04-09 05:35:00"), 4, "2021-04-09"),
      Row(2L, toTs("2021-04-10 02:30:00"), 5, "2021-04-10"),
      Row(2L, toTs("2021-04-10 02:30:00"), 5, "2021-04-10"),
      Row(2L, toTs("2021-04-10 02:30:00"), 8, "2021-04-10"),
      Row(2L, toTs("2021-04-10 02:30:00"), 8, "2021-04-10"),
      Row(2L, toTs("2021-04-07 00:30:00"), 10, "2021-04-10") // dated 4/10 but excluded from avg agg based on ts
    )
    // Schemas
    // {..., event (generic event column), ...}
    val listingsSchema = StructType("listing_events_fetcher",
                                    Array(
                                      StructField("listing_id", LongType),
                                      StructField("ts", LongType),
                                      StructField("ds", StringType)
                                    ))

    val ratingsSchema = StructType(
      "listing_ratings_fetcher",
      Array(StructField("listing_id", LongType),
            StructField("ts", LongType),
            StructField("rating", IntType),
            StructField("ds", StringType))
    )

    val sourceData: Map[StructType, Seq[Row]] = Map(
      listingsSchema -> listingEventData,
      ratingsSchema -> ratingEventData
    )

    sourceData.foreach {
      case (schema, rows) =>
        val tableName = s"$namespace.${schema.name}"

        spark.sql(s"DROP TABLE IF EXISTS $tableName")

        spark
          .createDataFrame(rows.asJava, SparkConversions.fromChrononSchema(schema))
          .save(tableName)
    }
    println("saved all data hand written for fetcher test")

    val startPartition = "2021-04-07"
    val endPartition = "2021-04-10"

    val leftSource =
      Builders.Source.events(
        query = Builders.Query(
          selects = Builders.Selects("listing_id", "ts"),
          startPartition = startPartition
        ),
        table = s"$namespace.${listingsSchema.name}"
      )

    val rightSource =
      Builders.Source.events(
        query = Builders.Query(
          selects = Builders.Selects("listing_id", "ts", "rating"),
          startPartition = startPartition
        ),
        table = s"$namespace.${ratingsSchema.name}",
        topic = "fake_topic2"
      )

    val groupBy = Builders.GroupBy(
      sources = Seq(rightSource),
      keyColumns = Seq("listing_id"),
      aggregations = Seq(
        Builders.Aggregation(
          operation = Operation.SUM,
          inputColumn = "rating",
          windows = null
        ),
        Builders.Aggregation(
          operation = Operation.AVERAGE,
          inputColumn = "rating",
          windows = Seq(new Window(2, TimeUnit.DAYS))
        ),
        Builders.Aggregation(
          operation = Operation.APPROX_HISTOGRAM_K,
          inputColumn = "rating",
          windows = Seq(new Window(2, TimeUnit.DAYS))
        )
      ),
      accuracy = Accuracy.TEMPORAL,
      metaData = Builders.MetaData(name = "unit_test/fetcher_tiled_gb",
                                   namespace = namespace,
                                   team = "chronon",
                                   customJson = groupByCustomJson.orNull)
    )

    val joinConf = Builders.Join(
      left = leftSource,
      joinParts = Seq(Builders.JoinPart(groupBy = groupBy)),
      metaData = Builders.MetaData(name = "unit_test/fetcher_tiled_join", namespace = namespace, team = "chronon")
    )
    joinConf
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
        .map { oldReqs =>
          // deliberately mis-type a few keys
          val r = oldReqs
            .map(r =>
              r.copy(keys = r.keys.mapValues { v =>
                if (v.isInstanceOf[java.lang.Long]) v.toString else v
              }))
          val responses = if (useJavaFetcher) {
            // Converting to java request and using the toScalaRequest functionality to test conversion
            val convertedJavaRequests = r.map(new JavaRequest(_)).asJava
            val javaResponse = javaFetcher.fetchJoin(convertedJavaRequests)
            FutureConverters
              .toScala(javaResponse)
              .map(_.asScala.map(jres =>
                Response(
                  Request(jres.request.name, jres.request.keys.asScala.toMap, Option(jres.request.atMillis)),
                  jres.values.toScala.map(ScalaVersionSpecificCollectionsConverter.convertJavaMapToScala)
                )))
          } else {
            fetcher.fetchJoin(r)
          }
          // fix mis-typed keys in the request
          val fixedResponses =
            responses.map(resps => resps.zip(oldReqs).map { case (resp, req) => resp.copy(request = req) })
          System.currentTimeMillis() -> fixedResponses
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

  // Compute a join until endDs and compare the result of fetching the aggregations with the computed join values.
  def compareTemporalFetch(joinConf: api.Join, endDs: String, namespace: String, consistencyCheck: Boolean): Unit = {
    implicit val executionContext: ExecutionContext = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(1))
    implicit val tableUtils: TableUtils = TableUtils(spark)
    val kvStoreFunc = () => OnlineUtils.buildInMemoryKVStore("FetcherTest")
    val inMemoryKvStore = kvStoreFunc()
    val mockApi = new MockApi(kvStoreFunc, namespace)

    val joinedDf = new ai.chronon.spark.Join(joinConf, endDs, tableUtils).computeJoin()
    val joinTable = s"$namespace.join_test_expected_${joinConf.metaData.cleanName}"
    joinedDf.save(joinTable)
    val endDsExpected = tableUtils.sql(s"SELECT * FROM $joinTable WHERE ds='$endDs'")

    joinConf.joinParts.asScala.foreach(jp =>
      OnlineUtils.serve(tableUtils, inMemoryKvStore, kvStoreFunc, namespace, endDs, jp.groupBy))

    // Extract queries for the EndDs from the computedJoin results and eliminating computed aggregation values
    val endDsEvents = {
      tableUtils.sql(
        s"SELECT * FROM $joinTable WHERE ts >= unix_timestamp('$endDs', '${tableUtils.partitionSpec.format}')")
    }
    val endDsQueries = endDsEvents.drop(endDsEvents.schema.fieldNames.filter(_.contains("unit_test")): _*)
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

    if (consistencyCheck) {
      val lagMs = -100000
      val laggedRequests = buildRequests(lagMs)
      val laggedResponseDf = joinResponses(laggedRequests, mockApi, samplePercent = 5, logToHive = true)._2
      val correctedLaggedResponse = laggedResponseDf
        .withColumn("ts_lagged", laggedResponseDf.col("ts_millis") + lagMs)
        .withColumn("ts_millis", col("ts_lagged"))
        .drop("ts_lagged")
      println("corrected lagged response")
      correctedLaggedResponse.show()
      correctedLaggedResponse.save(mockApi.logTable, partitionColumns = Seq(tableUtils.partitionColumn, "name"))

      // build flattened log table
      SchemaEvolutionUtils.runLogSchemaGroupBy(mockApi, today, today)
      val flattenerJob = new LogFlattenerJob(spark, joinConf, today, mockApi.logTable, mockApi.schemaTable)
      flattenerJob.buildLogTable()

      // build consistency metrics
      val consistencyJob = new ConsistencyJob(spark, joinConf, today)
      val metrics = consistencyJob.buildConsistencyMetrics()
      println(s"ooc metrics: $metrics".stripMargin)
    }
    // benchmark
    joinResponses(requests, mockApi, runCount = 10, useJavaFetcher = true)
    joinResponses(requests, mockApi, runCount = 10)

    // comparison
    val columns = endDsExpected.schema.fields.map(_.name)
    val responseRows: Seq[Row] =
      joinResponses(requests, mockApi, useJavaFetcher = true, debug = true)._1.map { res =>
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

    val keyishColumns = keys.toList ++ List(tableUtils.partitionColumn, Constants.TimeColumn)
    val responseRdd = tableUtils.sparkSession.sparkContext.parallelize(responseRows.toSeq)
    var responseDf = tableUtils.sparkSession.createDataFrame(responseRdd, endDsExpected.schema)
    if (endDs != today) {
      responseDf = responseDf.drop("ds").withColumn("ds", lit(endDs))
    }
    println("expected:")
    endDsExpected.show()
    println("response:")
    responseDf.show()

    val diff = Comparison.sideBySide(responseDf, endDsExpected, keyishColumns, aName = "online", bName = "offline")
    assertEquals(endDsQueries.count(), responseDf.count())
    if (diff.count() > 0) {
      println("queries:")
      endDsQueries.show()
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

  def testTemporalFetchJoinDeterministic(): Unit = {
    val namespace = "deterministic_fetch"
    val joinConf = generateMutationData(namespace)
    compareTemporalFetch(joinConf, "2021-04-10", namespace, consistencyCheck = false)
  }

  def testTemporalFetchJoinGenerated(): Unit = {
    val namespace = "generated_fetch"
    val joinConf = generateRandomData(spark = spark, namespace = namespace)
    compareTemporalFetch(joinConf,
                         tableUtils.partitionSpec.at(System.currentTimeMillis()),
                         namespace,
                         consistencyCheck = true)
  }

  def testTemporalTiledFetchJoinDeterministic(): Unit = {
    val namespace = "deterministic_tiled_fetch"
    val joinConf = generateEventOnlyData(namespace, groupByCustomJson = Some("{\"enable_tiling\": true}"))
    compareTemporalFetch(joinConf, "2021-04-10", namespace, consistencyCheck = false)
  }

  // test soft-fail on missing keys
  def testEmptyRequest(): Unit = {
    val namespace = "empty_request"
    val joinConf = generateRandomData(spark = spark, namespace = namespace, keyCount = 5, cardinality = 5)
    implicit val executionContext: ExecutionContext = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(1))
    implicit val tableUtils: TableUtils = TableUtils(spark)
    val kvStoreFunc = () => OnlineUtils.buildInMemoryKVStore("FetcherTest")
    val inMemoryKvStore = kvStoreFunc()
    val mockApi = new MockApi(kvStoreFunc, namespace)

    val metadataStore = new MetadataStore(inMemoryKvStore, timeoutMillis = 10000)
    inMemoryKvStore.create(ChrononMetadataKey)
    metadataStore.putJoinConf(joinConf)

    val request = Request(joinConf.metaData.nameToFilePath, Map.empty)
    val (responses, _) = joinResponses(Array(request), mockApi)
    val responseMap = responses.head.values.get

    println("====== Empty request response map ======")
    println(responseMap)
    assertEquals(joinConf.joinParts.size() + joinConf.derivationsWithoutStar.size, responseMap.size)
    assertEquals(responseMap.keys.count(_.endsWith("_exception")), joinConf.joinParts.size())
  }

  def testRetrieveSchema(): Unit = {
    val namespace: String = "test_retrieve_schema"
    val generatedJoin: Join =
      TestUtils.generateRandomData(spark = spark, namespace = namespace, keyCount = 10, cardinality = 10)
    val mockApi: Api = TestUtils.setupFetcherWithJoin(spark, generatedJoin, namespace)

    // validates the schema for all features in the join
    val joinResult: Map[String, DataType] = mockApi.fetcher.retrieveJoinSchema(generatedJoin.metaData.getName)
    assertEquals(TestUtils.expectedSchemaForTestPaymentsJoinWithCtxFeats, joinResult)

    // validates the keys (both entity and external) schema for the join
    val keySchemaJoinResult: Map[String, DataType] = mockApi.fetcher.retrieveJoinKeys(generatedJoin.metaData.getName)
    assertEquals(TestUtils.expectedJoinKeySchema, keySchemaJoinResult)

    // validates the entity keys schema for the join
    val entityKeysJoinResult: Map[String, DataType] =
      mockApi.fetcher.retrieveEntityJoinKeys(generatedJoin.metaData.getName)
    assertEquals(TestUtils.expectedEntityJoinKeySchema, entityKeysJoinResult)

    // validates the external keys schema for the join
    val externalKeysJoinResult: Map[String, DataType] =
      mockApi.fetcher.retrieveExternalJoinKeys(generatedJoin.metaData.getName)
    assertEquals(TestUtils.expectedExternalJoinKeySchema, externalKeysJoinResult)

    // validates the schema for all features in the given GroupBy
    val groupByResult: Map[String, DataType] = mockApi.fetcher.retrieveGroupBySchema(TestUtils.vendorRatingsGroupByName)
    assertEquals(TestUtils.expectedSchemaForVendorRatingsGroupBy, groupByResult)
  }
}
