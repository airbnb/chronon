package ai.zipline.spark.test

import ai.zipline.aggregator.test.Column
import ai.zipline.aggregator.windowing.TsUtils
import ai.zipline.api.Constants.ZiplineMetadataKey
import ai.zipline.api.Extensions.{GroupByOps, MetadataOps}

import ai.zipline.api.{
  Accuracy,
  BooleanType,
  Builders,
  Constants,
  DataModel,
  DoubleType,
  IntType,
  LongType,
  Operation,
  StringType,
  StructField,
  StructType,
  TimeUnit,
  Window,
  GroupBy => GroupByConf
}
import ai.zipline.api.Extensions.{GroupByOps, MetadataOps}
import ai.zipline.online.KVStore.GetRequest
import ai.zipline.online.Fetcher.{Request, Response}
import ai.zipline.online.KVStore.GetRequest
import ai.zipline.online._
import ai.zipline.spark.Extensions._
import ai.zipline.spark._
import ai.zipline.spark.test.FetcherTest.buildInMemoryKVStore
import junit.framework.TestCase
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.apache.spark.sql.functions.avg
import org.apache.spark.sql.{Row, SparkSession}
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
    InMemoryKvStore("FetcherTest", { () => TableUtils(SparkSessionBuilder.build("FetcherTest", local = true)) })
  }
}

class FetcherTest extends TestCase {
  val spark: SparkSession = SparkSessionBuilder.build("FetcherTest", local = true)

  private val namespace = "fetcher_test"
  private val topic = "test_topic"
  TimeZone.setDefault(TimeZone.getTimeZone("UTC"))
  spark.sql(s"CREATE DATABASE IF NOT EXISTS $namespace")

  // TODO: Pull the code here into what streaming can use.
  def putStreaming(groupByConf: GroupByConf,
                   kvStore: () => KVStore,
                   tableUtils: TableUtils,
                   ds: String,
                   previous_ds: String): Unit = {
    val groupBy = GroupBy.from(groupByConf, PartitionRange(previous_ds, ds), tableUtils)
    // for events this will select ds-1 <= ts < ds

    val selected = groupByConf.dataModel match {
      case DataModel.Entities => groupBy.mutationDf.filter(s"ds='$ds'")
      case DataModel.Events   => groupBy.inputDf.filter(s"ds>='$ds'")
    }
    val inputStream = new InMemoryStream
    val groupByStreaming =
      new streaming.GroupBy(inputStream.getInMemoryStreamDF(spark, selected), spark, groupByConf, new MockApi(kvStore))
    groupByStreaming.run()
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
    assertTrue(res.get.latest.isSuccess)
    val actual = new String(res.get.values.get.head.bytes)

    assertEquals(expected, actual.replaceAll("\\s+", ""))

    val directoryDataSetDataSet = ZiplineMetadataKey + "_directory_test"
    val directoryMetadataStore = new MetadataStore(inMemoryKvStore, directoryDataSetDataSet, timeoutMillis = 10000)
    inMemoryKvStore.create(directoryDataSetDataSet)
    val directoryPut = directoryMetadataStore.putConf("./spark/src/test/scala/ai/zipline/spark/test/resources")
    Await.result(directoryPut, Duration.Inf)
    val dirResponse =
      inMemoryKvStore.get(GetRequest("joins/team/example_join.v1".getBytes(), directoryDataSetDataSet))
    val dirRes = Await.result(dirResponse, Duration.Inf)
    assertTrue(dirRes.get.latest.isSuccess)
    val dirActual = new String(dirRes.get.values.get.head.bytes)

    assertEquals(expected, dirActual.replaceAll("\\s+", ""))

    val emptyResponse =
      inMemoryKvStore.get(GetRequest("NoneExistKey".getBytes(), "NonExistDataSetName"))
    val emptyRes = Await.result(emptyResponse, Duration.Inf)
    assertFalse(emptyRes.get.latest.isSuccess)
  }

  /**
    * Generate deterministic data for testing and checkpointing IRs and streaming data.
    */
  def generateMutationData(): ai.zipline.api.Join = {
    def toTs(arg: String): Long = TsUtils.datetimeToTs(arg)
    val eventData = Seq(
      Row(2, toTs("2021-04-10 09:00:00"), "2021-04-10"),
      Row(2, toTs("2021-04-10 23:00:00"), "2021-04-10"), // Query for added event
      Row(2, toTs("2021-04-10 23:45:00"), "2021-04-10") // Query for mutated event
    )
    val snapshotData = Seq(
      Row(1, toTs("2021-04-04 00:30:00"), 4, "2021-04-08"),
      Row(1, toTs("2021-04-04 12:30:00"), 4, "2021-04-08"),
      Row(1, toTs("2021-04-05 00:30:00"), 4, "2021-04-08"),
      Row(1, toTs("2021-04-08 02:30:00"), 4, "2021-04-08"),
      Row(2, toTs("2021-04-04 01:40:00"), 3, "2021-04-08"),
      Row(2, toTs("2021-04-05 03:40:00"), 3, "2021-04-08"),
      Row(2, toTs("2021-04-06 03:45:00"), 4, "2021-04-08"),
      // {listing_id, ts, rating, ds}
      Row(1, toTs("2021-04-04 00:30:00"), 4, "2021-04-09"),
      Row(1, toTs("2021-04-04 12:30:00"), 4, "2021-04-09"),
      Row(1, toTs("2021-04-05 00:30:00"), 4, "2021-04-09"),
      Row(1, toTs("2021-04-08 02:30:00"), 4, "2021-04-09"),
      Row(2, toTs("2021-04-04 01:40:00"), 3, "2021-04-09"),
      Row(2, toTs("2021-04-05 03:40:00"), 3, "2021-04-09"),
      Row(2, toTs("2021-04-06 03:45:00"), 4, "2021-04-09"),
      Row(2, toTs("2021-04-09 05:45:00"), 5, "2021-04-09")
    )
    val mutationData = Seq(
      Row(2, toTs("2021-04-09 05:45:00"), 2, "2021-04-09", toTs("2021-04-09 05:45:00"), false),
      Row(2, toTs("2021-04-09 05:45:00"), 2, "2021-04-09", toTs("2021-04-09 07:00:00"), true),
      Row(2, toTs("2021-04-09 05:45:00"), 5, "2021-04-09", toTs("2021-04-09 07:00:00"), false),
      // {listing_id, ts, rating, ds, mutation_ts, is_before}
      Row(1, toTs("2021-04-10 00:30:00"), 5, "2021-04-10", toTs("2021-04-10 00:30:00"), false),
      Row(2, toTs("2021-04-10 10:00:00"), 4, "2021-04-10", toTs("2021-04-10 10:00:00"), false),
      Row(2, toTs("2021-04-10 10:00:00"), 4, "2021-04-10", toTs("2021-04-10 23:30:00"), true),
      Row(2, toTs("2021-04-10 10:00:00"), 3, "2021-04-10", toTs("2021-04-10 23:30:00"), false)
    )
    // Schemas
    val snapshotSchema = StructType(
      "listing_ratings_snapshot_fetcher",
      Array(StructField("listing_id", IntType),
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
                                   StructField("listing_id", IntType),
                                   StructField("ts", LongType),
                                   StructField("ds", StringType)
                                 ))

    val sourceData: Map[StructType, Seq[Row]] = Map(
      eventSchema -> eventData,
      mutationSchema -> mutationData,
      snapshotSchema -> snapshotData
    )

    sourceData.foreach {
      case (schema, rows) => {
        spark.createDataFrame(rows.asJava, Conversions.fromZiplineSchema(schema)).save(s"$namespace.${schema.name}")
      }
    }

    val startPartition = "2021-04-08"
    val endPartition = "2021-04-10"
    val rightSource = Builders.Source.entities(
      query = Builders.Query(
        selects = Builders.Selects("listing_id", "ts", "rating"),
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
      metaData = Builders.MetaData(name = "unit_test.fetcher_mutations_gb", namespace = namespace, team = "zipline")
    )

    val joinConf = Builders.Join(
      left = leftSource,
      joinParts = Seq(Builders.JoinPart(groupBy = groupBy)),
      metaData = Builders.MetaData(name = "unit_test.fetcher_mutations_join", namespace = namespace, team = "zipline")
    )
    joinConf
  }

  def generateRandomData(): ai.zipline.api.Join = {
    val today = Constants.Partition.at(System.currentTimeMillis())
    val yesterday = Constants.Partition.before(today)
    val rowCount = 100000
    val userCol = Column("user", StringType, 10)
    val vendorCol = Column("vendor", StringType, 10)
    // temporal events
    val paymentCols =
      Seq(userCol, vendorCol, Column("payment", LongType, 100), Column("notes", StringType, 20))
    val paymentsTable = s"$namespace.payments_table"
    DataFrameGen.events(spark, paymentCols, rowCount, 60).save(paymentsTable)
    val userPaymentsGroupBy = Builders.GroupBy(
      sources = Seq(Builders.Source.events(query = Builders.Query(), table = paymentsTable, topic = topic)),
      keyColumns = Seq("user"),
      aggregations = Seq(
        Builders.Aggregation(operation = Operation.COUNT,
                             inputColumn = "payment",
                             windows = Seq(new Window(6, TimeUnit.HOURS), new Window(14, TimeUnit.DAYS))),
        Builders.Aggregation(operation = Operation.LAST, inputColumn = "payment"),
        Builders.Aggregation(operation = Operation.LAST, inputColumn = "notes"),
        Builders.Aggregation(operation = Operation.FIRST, inputColumn = "notes")
      ),
      metaData = Builders.MetaData(name = "unit_test.user_payments", namespace = namespace)
    )

    // snapshot events
    val ratingCols =
      Seq(userCol, vendorCol, Column("rating", IntType, 5), Column("bucket", StringType, 5))
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
        Builders.Aggregation(operation = Operation.LAST_K,
                             argMap = Map("k" -> "300"),
                             inputColumn = "user",
                             windows = Seq(new Window(2, TimeUnit.DAYS), new Window(30, TimeUnit.DAYS)))
      ),
      metaData = Builders.MetaData(name = "unit_test.vendor_ratings", namespace = namespace),
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
      metaData = Builders.MetaData(name = "unit_test.user_balance", namespace = namespace)
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
      metaData = Builders.MetaData(name = "unit_test.vendor_credit", namespace = namespace)
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
      metaData = Builders.MetaData(name = "unit_test.vendor_review", namespace = namespace),
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
      metaData = Builders.MetaData(name = "test.payments_join", namespace = namespace, team = "zipline")
    )
    joinConf
  }

  /**
    * Compute a join until endDs and compare the result of fetching the aggregations with the computed join values.
    */
  def compareTemporalFetch(joinConf: ai.zipline.api.Join, endDs: String): Unit = {

    implicit val executionContext: ExecutionContext = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(1))
    implicit val tableUtils: TableUtils = TableUtils(spark)
    val inMemoryKvStore = buildInMemoryKVStore()
    @transient lazy val fetcher = new Fetcher(inMemoryKvStore)
    @transient lazy val javaFetcher = new JavaFetcher(inMemoryKvStore)
    val joinedDf = new Join(joinConf, endDs, tableUtils).computeJoin()
    val joinTable = s"$namespace.join_test_expected_${joinConf.metaData.cleanName}"
    joinedDf.save(joinTable)
    val endDsExpected = tableUtils.sql(s"SELECT * FROM $joinTable WHERE ds='$endDs'")
    val prevDs = Constants.Partition.before(endDs)

    def serve(groupByConf: GroupByConf): Unit = {
      GroupByUpload.run(groupByConf, prevDs, Some(tableUtils))
      buildInMemoryKVStore().bulkPut(groupByConf.kvTable, groupByConf.batchDataset, null)
      if (groupByConf.inferredAccuracy == Accuracy.TEMPORAL && groupByConf.streamingSource.isDefined) {
        inMemoryKvStore.create(groupByConf.streamingDataset)
        putStreaming(groupByConf, buildInMemoryKVStore, tableUtils, endDs, prevDs)
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

    val requests = endDsQueries.rdd
      .map { row =>
        val keyMap = keys.zipWithIndex.map {
          case (keyName, idx) =>
            keyName -> row.get(keyIndices(idx)).asInstanceOf[AnyRef]
        }.toMap
        val ts = row.get(tsIndex).asInstanceOf[Long]
        Request(joinConf.metaData.nameToFilePath, keyMap, Some(ts))
      }
      .collect()
    val chunkSize = 100

    def printFetcherStats(useJavaFetcher: Boolean,
                          requests: Array[Request],
                          count: Int,
                          chunkSize: Int,
                          qpsSum: Double,
                          latencySum: Double): Unit = {

      val fetcherNameString = if (useJavaFetcher) "Java Fetcher" else "Scala Fetcher"
      println(s"""
                 |Averaging fetching stats for $fetcherNameString over ${requests.length} requests $count times
                 |with batch size: $chunkSize
                 |average qps: ${qpsSum / count}
                 |average latency: ${latencySum / count}
                 |""".stripMargin)
    }

    def joinResponses(useJavaFetcher: Boolean = false) = {
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
    val count = 10
    var latencySum = 0.0
    var qpsSum = 0.0
    (0 until count).foreach { _ =>
      val (latency, qps, _) = joinResponses()
      latencySum += latency
      qpsSum += qps
    }
    printFetcherStats(false, requests, count, chunkSize, qpsSum, latencySum)

    latencySum = 0.0
    qpsSum = 0.0
    (0 until count).foreach { _ =>
      val (latency, qps, _) = joinResponses(true)
      latencySum += latency
      qpsSum += qps
    }
    printFetcherStats(true, requests, count, chunkSize, qpsSum, latencySum)

    val columns = endDsExpected.schema.fields.map(_.name)
    val responseRows: Seq[Row] = joinResponses(true)._3.map { res =>
      val all: Map[String, AnyRef] =
        res.request.keys ++
          res.values.get ++
          Map(Constants.PartitionColumn -> endDs) ++
          Map(Constants.TimeColumn -> new lang.Long(res.request.atMillis.get))
      val values: Array[Any] = columns.map(all.get(_).orNull)
      KvRdd
        .toSparkRow(
          values,
          StructType.from(s"record_${joinConf.metaData.cleanName}", Conversions.toZiplineSchema(endDsExpected.schema)))
        .asInstanceOf[GenericRow]
    }

    println(endDsExpected.schema.pretty)
    val keyishColumns = keys.toList ++ List(Constants.PartitionColumn, Constants.TimeColumn)
    val responseRdd = tableUtils.sparkSession.sparkContext.parallelize(responseRows)
    val responseDf = tableUtils.sparkSession.createDataFrame(responseRdd, endDsExpected.schema)

    val diff = Comparison.sideBySide(responseDf, endDsExpected, keyishColumns, aName = "online", bName = "offline")
    assertEquals(endDsQueries.count(), responseDf.count())
    if (diff.count() > 0) {
      println("queries:")
      endDsQueries.show()
      println("expected:")
      endDsExpected.show()
      println("response:")
      responseDf.show()
      println(s"Total count: ${responseDf.count()}")
      println(s"Diff count: ${diff.count()}")
      println(s"diff result rows:")
      diff.show()
    }
    assertEquals(0, diff.count())
  }

  def testTemporalFetchJoinDeterministic(): Unit = {
    val joinConf = generateMutationData()
    compareTemporalFetch(joinConf, "2021-04-10")
  }

  def testTemporalFetchJoinGenerated(): Unit = {
    val joinConf = generateRandomData()
    compareTemporalFetch(joinConf, Constants.Partition.at(System.currentTimeMillis()))
  }
}
