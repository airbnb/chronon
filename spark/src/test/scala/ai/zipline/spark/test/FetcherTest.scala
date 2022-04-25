package ai.zipline.spark.test

import ai.zipline.aggregator.test.Column
import ai.zipline.aggregator.windowing.TsUtils
import ai.zipline.api.Constants.ZiplineMetadataKey

import ai.zipline.api.Extensions.{GroupByOps, MetadataOps, SourceOps}

import ai.zipline.api.Extensions._

import ai.zipline.api.{
  Accuracy,
  BooleanType,
  Builders,
  Constants,
  DataModel,
  DoubleType,
  IntType,
  ListType,
  LongType,
  Operation,
  StringType,
  StructField,
  StructType,
  TimeUnit,
  Window,
  GroupBy => GroupByConf,
  Join => JoinConf
}
import ai.zipline.online.Fetcher.{Request, Response}
import ai.zipline.online.KVStore.GetRequest
import ai.zipline.online._
import ai.zipline.spark.Extensions._
import ai.zipline.spark._
import ai.zipline.spark.consistency.ConsistencyJob
import ai.zipline.spark.test.FetcherTest.buildInMemoryKVStore
import junit.framework.TestCase
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.apache.spark.sql.functions.{avg, lit}
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.junit.Assert.{assertEquals, assertFalse, assertTrue}

import java.lang
import java.util.TimeZone
import java.util.concurrent.Executors
import scala.collection.JavaConverters.{asScalaBufferConverter, _}
import scala.compat.java8.FutureConverters
import scala.concurrent.duration.{Duration, SECONDS}
import scala.concurrent.{Await, ExecutionContext}
import scala.io.Source
import scala.util.Try

object FetcherTest {

  def buildInMemoryKVStore(): InMemoryKvStore = {
    InMemoryKvStore.build("FetcherTest", { () => TableUtils(SparkSessionBuilder.build("FetcherTest", local = true)) })
  }
}

class FetcherTest extends TestCase {
  val spark: SparkSession = SparkSessionBuilder.build("FetcherTest", local = true)

  private val topic = "test_topic"
  TimeZone.setDefault(TimeZone.getTimeZone("UTC"))
  private val today = Constants.Partition.at(System.currentTimeMillis())
  private val yesterday = Constants.Partition.before(today)

  // TODO: Pull the code here into what streaming can use.
  def putStreaming(groupByConf: GroupByConf,
                   kvStore: () => KVStore,
                   tableUtils: TableUtils,
                   ds: String,
                   previousDs: String,
                   namespace: String): Unit = {
    val inputStreamDf = groupByConf.dataModel match {
      case DataModel.Entities =>
        val entity = groupByConf.streamingSource.get
        val df = tableUtils.sql(s"SELECT * FROM ${entity.getEntities.mutationTable} WHERE ds = '$ds'")
        df.withColumnRenamed(entity.query.reversalColumn, Constants.ReversalColumn)
          .withColumnRenamed(entity.query.mutationTimeColumn, Constants.MutationTimeColumn)
      case DataModel.Events =>
        val table = groupByConf.streamingSource.get.table
        tableUtils.sql(s"SELECT * FROM $table WHERE ds >= '$ds'")
    }
    val inputStream = new InMemoryStream
    val mockApi = new MockApi(kvStore, namespace)
    mockApi.streamSchema = StructType.from("Stream", Conversions.toZiplineSchema(inputStreamDf.schema))
    val groupByStreaming =
      new streaming.GroupBy(inputStream.getInMemoryStreamDF(spark, inputStreamDf), spark, groupByConf, mockApi)
    // We modify the arguments for running to make sure all data gets into the KV Store before fetching.
    val dataStream = groupByStreaming.buildDataStream()
    val query = dataStream.trigger(Trigger.Once()).start()
    query.awaitTermination()
  }

  def testMetadataStore(): Unit = {
    implicit val executionContext: ExecutionContext = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(1))
    implicit val tableUtils: TableUtils = TableUtils(spark)

    val src =
      Source.fromFile("./spark/src/test/scala/ai/zipline/spark/test/resources/joins/team/example_join.v1")
    val expected = {
      try src.mkString
      finally src.close()
    }.replaceAll("\\s+", "")

    val inMemoryKvStore = buildInMemoryKVStore()
    val singleFileDataSet = ZiplineMetadataKey + "_single_file_test"
    val singleFileMetadataStore = new MetadataStore(inMemoryKvStore, singleFileDataSet, timeoutMillis = 10000)
    inMemoryKvStore.create(singleFileDataSet)
    // set the working directory to /zipline instead of $MODULE_DIR in configuration if Intellij fails testing
    val singleFilePut = singleFileMetadataStore.putConf(
      "./spark/src/test/scala/ai/zipline/spark/test/resources/joins/team/example_join.v1")
    Await.result(singleFilePut, Duration.Inf)
    val response = inMemoryKvStore.get(GetRequest("joins/team/example_join.v1".getBytes(), singleFileDataSet))
    val res = Await.result(response, Duration.Inf)
    assertTrue(res.latest.isSuccess)
    val actual = new String(res.values.get.head.bytes)

    assertEquals(expected, actual.replaceAll("\\s+", ""))

    val directoryDataSetDataSet = ZiplineMetadataKey + "_directory_test"
    val directoryMetadataStore = new MetadataStore(inMemoryKvStore, directoryDataSetDataSet, timeoutMillis = 10000)
    inMemoryKvStore.create(directoryDataSetDataSet)
    val directoryPut = directoryMetadataStore.putConf("./spark/src/test/scala/ai/zipline/spark/test/resources")
    Await.result(directoryPut, Duration.Inf)
    val dirResponse =
      inMemoryKvStore.get(GetRequest("joins/team/example_join.v1".getBytes(), directoryDataSetDataSet))
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
  def generateMutationData(namespace: String): ai.zipline.api.Join = {
    spark.sql(s"CREATE DATABASE IF NOT EXISTS $namespace")
    def toTs(arg: String): Long = TsUtils.datetimeToTs(arg)
    val eventData = Seq(
      Row(595125622443733822L, toTs("2021-04-10 09:00:00"), "2021-04-10"),
      Row(595125622443733822L, toTs("2021-04-10 23:00:00"), "2021-04-10"), // Query for added event
      Row(595125622443733822L, toTs("2021-04-10 23:45:00"), "2021-04-10") // Query for mutated event
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
        spark.createDataFrame(rows.asJava, Conversions.fromZiplineSchema(schema)).save(s"$namespace.${schema.name}")

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
        )
      ),
      accuracy = Accuracy.TEMPORAL,
      metaData = Builders.MetaData(name = "unit_test/fetcher_mutations_gb", namespace = namespace, team = "zipline")
    )

    val joinConf = Builders.Join(
      left = leftSource,
      joinParts = Seq(Builders.JoinPart(groupBy = groupBy)),
      metaData = Builders.MetaData(name = "unit_test/fetcher_mutations_join", namespace = namespace, team = "zipline")
    )
    joinConf
  }

  def generateRandomData(namespace: String): JoinConf = {
    spark.sql(s"CREATE DATABASE IF NOT EXISTS $namespace")
    val keyCount = 100
    val rowCount = 1000 * keyCount
    val userCol = Column("user", StringType, keyCount)
    val vendorCol = Column("vendor", StringType, keyCount)
    // temporal events
    val paymentCols = Seq(userCol, vendorCol, Column("payment", LongType, 100), Column("notes", StringType, 20))
    val paymentsTable = s"$namespace.payments_table"
    val paymentsDf = DataFrameGen.events(spark, paymentCols, rowCount, 60)
    val tsColString = "ts_string"

    paymentsDf.withTimeBasedColumn(tsColString, format = "yyyy-MM-dd HH:mm:ss").save(paymentsTable)
    val userPaymentsGroupBy = Builders.GroupBy(
      sources = Seq(Builders.Source.events(query = Builders.Query(), table = paymentsTable, topic = topic)),
      keyColumns = Seq("user"),
      aggregations = Seq(
        Builders.Aggregation(operation = Operation.COUNT,
                             inputColumn = "payment",
                             windows = Seq(new Window(6, TimeUnit.HOURS), new Window(14, TimeUnit.DAYS))),
        Builders.Aggregation(operation = Operation.LAST, inputColumn = "payment"),
        Builders.Aggregation(operation = Operation.LAST_K, argMap = Map("k" -> "5"), inputColumn = "notes"),
        Builders.Aggregation(operation = Operation.VARIANCE, inputColumn = "payment"),
        Builders.Aggregation(operation = Operation.FIRST, inputColumn = "notes"),
        Builders.Aggregation(operation = Operation.FIRST, inputColumn = tsColString),
        Builders.Aggregation(operation = Operation.LAST, inputColumn = tsColString)
      ),
      metaData = Builders.MetaData(name = "unit_test/user_payments", namespace = namespace)
    )

    // snapshot events
    val ratingCols =
      Seq(
        userCol,
        vendorCol,
        Column("rating", IntType, 5),
        Column("bucket", StringType, 5),
        Column("sub_rating", ListType(DoubleType), 5),
        Column("txn_types", ListType(StringType), 5)
      )
    val ratingsTable = s"$namespace.ratings_table"
    DataFrameGen.events(spark, ratingCols, rowCount, 180).save(ratingsTable)
    val vendorRatingsGroupBy = Builders.GroupBy(
      sources = Seq(Builders.Source.events(query = Builders.Query(), table = ratingsTable)),
      keyColumns = Seq("vendor"),
      aggregations = Seq(
        Builders.Aggregation(operation = Operation.AVERAGE,
                             inputColumn = "rating",
                             windows = Seq(new Window(2, TimeUnit.DAYS), new Window(30, TimeUnit.DAYS)),
                             buckets = Seq("bucket")),
        Builders.Aggregation(operation = Operation.HISTOGRAM,
                             inputColumn = "txn_types",
                             windows = Seq(new Window(3, TimeUnit.DAYS))),
        Builders.Aggregation(operation = Operation.LAST_K,
                             argMap = Map("k" -> "300"),
                             inputColumn = "user",
                             windows = Seq(new Window(2, TimeUnit.DAYS), new Window(30, TimeUnit.DAYS)))
      ),
      metaData = Builders.MetaData(name = "unit_test/vendor_ratings", namespace = namespace),
      accuracy = Accuracy.SNAPSHOT
    )

    // no-agg
    val userBalanceCols = Seq(userCol, Column("balance", IntType, 5000))
    val balanceTable = s"$namespace.balance_table"
    DataFrameGen
      .entities(spark, userBalanceCols, rowCount, 180)
      .groupBy("user", "ds")
      .agg(avg("balance") as "avg_balance")
      .save(balanceTable)
    val userBalanceGroupBy = Builders.GroupBy(
      sources = Seq(Builders.Source.entities(query = Builders.Query(), snapshotTable = balanceTable)),
      keyColumns = Seq("user"),
      metaData = Builders.MetaData(name = "unit_test/user_balance", namespace = namespace)
    )

    // snapshot-entities
    val userVendorCreditCols =
      Seq(Column("account", StringType, 100),
          vendorCol, // will be renamed
          Column("credit", IntType, 500),
          Column("ts", LongType, 100))
    val creditTable = s"$namespace.credit_table"
    DataFrameGen
      .entities(spark, userVendorCreditCols, rowCount, 100)
      .withColumnRenamed("vendor", "vendor_id")
      .save(creditTable)
    val creditGroupBy = Builders.GroupBy(
      sources = Seq(Builders.Source.entities(query = Builders.Query(), snapshotTable = creditTable)),
      keyColumns = Seq("vendor_id"),
      aggregations = Seq(
        Builders.Aggregation(operation = Operation.SUM,
                             inputColumn = "credit",
                             windows = Seq(new Window(2, TimeUnit.DAYS), new Window(30, TimeUnit.DAYS)))),
      metaData = Builders.MetaData(name = "unit_test/vendor_credit", namespace = namespace)
    )

    // temporal-entities
    val vendorReviewCols =
      Seq(Column("vendor", StringType, 10), // will be renamed
          Column("review", LongType, 10))
    val snapshotTable = s"$namespace.reviews_table_snapshot"
    val mutationTable = s"$namespace.reviews_table_mutations"
    val mutationTopic = "reviews_mutation_topic"
    val (snapshotDf, mutationsDf) =
      DataFrameGen.mutations(spark, vendorReviewCols, 10000, 35, 0.2, 1, keyColumnName = "vendor")
    snapshotDf.withColumnRenamed("vendor", "vendor_id").save(snapshotTable)
    mutationsDf.withColumnRenamed("vendor", "vendor_id").save(mutationTable)
    val reviewGroupBy = Builders.GroupBy(
      sources = Seq(
        Builders.Source
          .entities(
            query = Builders.Query(
              startPartition = Constants.Partition.before(yesterday)
            ),
            snapshotTable = snapshotTable,
            mutationTable = mutationTable,
            mutationTopic = mutationTopic
          )),
      keyColumns = Seq("vendor_id"),
      aggregations = Seq(
        Builders.Aggregation(operation = Operation.SUM,
                             inputColumn = "review",
                             windows = Seq(new Window(2, TimeUnit.DAYS), new Window(30, TimeUnit.DAYS)))),
      metaData = Builders.MetaData(name = "unit_test/vendor_review", namespace = namespace),
      accuracy = Accuracy.TEMPORAL
    )

    // queries
    val queryCols = Seq(userCol, vendorCol)
    val queriesTable = s"$namespace.queries_table"
    val queriesDf = DataFrameGen
      .events(spark, queryCols, rowCount, 4)
      .withColumnRenamed("user", "user_id")
      .withColumnRenamed("vendor", "vendor_id")
    queriesDf.show()
    queriesDf.save(queriesTable)

    val joinConf = Builders.Join(
      left = Builders.Source.events(Builders.Query(startPartition = today), table = queriesTable),
      joinParts = Seq(
        Builders.JoinPart(groupBy = vendorRatingsGroupBy, keyMapping = Map("vendor_id" -> "vendor")),
        Builders.JoinPart(groupBy = userPaymentsGroupBy, keyMapping = Map("user_id" -> "user")),
        Builders.JoinPart(groupBy = userBalanceGroupBy, keyMapping = Map("user_id" -> "user")),
        Builders.JoinPart(groupBy = reviewGroupBy),
        Builders.JoinPart(groupBy = creditGroupBy, prefix = "b"),
        Builders.JoinPart(groupBy = creditGroupBy, prefix = "a")
      ),
      metaData =
        Builders.MetaData(name = "test/payments_join", namespace = namespace, team = "zipline", samplePercent = 30)
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
        .map { r =>
          val responses = if (useJavaFetcher) {
            // Converting to java request and using the toScalaRequest functionality to test conversion
            val convertedJavaRequests = r.map(new JavaRequest(_)).asJava
            val javaResponse = javaFetcher.fetchJoin(convertedJavaRequests)
            FutureConverters
              .toScala(javaResponse)
              .map(_.asScala.map(jres =>
                Response(Request(jres.request.name, jres.request.keys.asScala.toMap, Option(jres.request.atMillis)),
                         Try(jres.values.asScala.toMap))))
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
    var loggedValues: Seq[LoggableResponse] = null
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
        partitionColumns = Seq("join_name", "ds")
      )
    }
    if (samplePercent > 0) {
      println(s"logged count: ${loggedDf.count()}")
      loggedDf.show()
    }

    result -> loggedDf
  }

  // Compute a join until endDs and compare the result of fetching the aggregations with the computed join values.
  def compareTemporalFetch(joinConf: ai.zipline.api.Join,
                           endDs: String,
                           namespace: String,
                           consistencyCheck: Boolean): Unit = {
    implicit val executionContext: ExecutionContext = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(1))
    implicit val tableUtils: TableUtils = TableUtils(spark)
    val inMemoryKvStore = buildInMemoryKVStore()
    val mockApi = new MockApi(buildInMemoryKVStore, namespace)

    val joinedDf = new Join(joinConf, endDs, tableUtils).computeJoin()
    val joinTable = s"$namespace.join_test_expected_${joinConf.metaData.cleanName}"
    joinedDf.save(joinTable)
    val endDsExpected = tableUtils.sql(s"SELECT * FROM $joinTable WHERE ds='$endDs'")
    val prevDs = Constants.Partition.before(endDs)

    def serve(groupByConf: GroupByConf): Unit = {
      GroupByUpload.run(groupByConf, prevDs, Some(tableUtils))
      inMemoryKvStore.bulkPut(groupByConf.metaData.uploadTable, groupByConf.batchDataset, null)
      if (groupByConf.inferredAccuracy == Accuracy.TEMPORAL && groupByConf.streamingSource.isDefined) {
        inMemoryKvStore.create(groupByConf.streamingDataset)
        putStreaming(groupByConf, buildInMemoryKVStore, tableUtils, endDs, prevDs, namespace)
      }
    }
    joinConf.joinParts.asScala.foreach(jp => serve(jp.groupBy))

    // Extract queries for the EndDs from the computedJoin results and eliminating computed aggregation values
    val endDsEvents = {
      tableUtils.sql(s"SELECT * FROM $joinTable WHERE ts >= unix_timestamp('$endDs', '${Constants.Partition.format}')")
    }
    val endDsQueries = endDsEvents.drop(endDsEvents.schema.fieldNames.filter(_.contains("unit_test")): _*)
    val keys = endDsQueries.schema.fieldNames.filterNot(Constants.ReservedColumns.contains)
    val keyIndices = keys.map(endDsQueries.schema.fieldIndex)
    val tsIndex = endDsQueries.schema.fieldIndex(Constants.TimeColumn)
    val metadataStore = new MetadataStore(inMemoryKvStore, timeoutMillis = 10000)
    inMemoryKvStore.create(ZiplineMetadataKey)
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
        .drop("ts_millis")
        .withColumnRenamed("ts_lagged", "ts_millis")
      println("corrected lagged response")
      correctedLaggedResponse.show()
      correctedLaggedResponse.save(mockApi.logTable)

      // build consistency metrics
      val consistencyJob = new ConsistencyJob(spark, joinConf, today, mockApi)
      val metrics = consistencyJob.buildConsistencyMetrics()
      println(s"ooc metrics: $metrics".stripMargin)
    }
    // benchmark
    joinResponses(requests, mockApi, runCount = 1000, useJavaFetcher = true)
    joinResponses(requests, mockApi, runCount = 10)

    // comparison
    val columns = endDsExpected.schema.fields.map(_.name)
    val responseRows: Seq[Row] =
      joinResponses(requests, mockApi, useJavaFetcher = true, debug = true)._1.map { res =>
        val all: Map[String, AnyRef] =
          res.request.keys ++
            res.values.get ++
            Map(Constants.PartitionColumn -> today) ++
            Map(Constants.TimeColumn -> new lang.Long(res.request.atMillis.get))
        val values: Array[Any] = columns.map(all.get(_).orNull)
        Conversions
          .toSparkRow(values, StructType.from("record", Conversions.toZiplineSchema(endDsExpected.schema)))
          .asInstanceOf[GenericRow]
      }

    println(endDsExpected.schema.pretty)
    val keyishColumns = keys.toList ++ List(Constants.PartitionColumn, Constants.TimeColumn)
    val responseRdd = tableUtils.sparkSession.sparkContext.parallelize(responseRows)
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
    val joinConf = generateRandomData(namespace)
    compareTemporalFetch(joinConf,
                         Constants.Partition.at(System.currentTimeMillis()),
                         namespace,
                         consistencyCheck = true)
  }
}
