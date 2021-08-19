package ai.zipline.spark.test

import java.lang
import java.util.concurrent.Executors

import scala.collection.JavaConverters.{asScalaBufferConverter, _}
import scala.compat.java8.FutureConverters
import scala.concurrent.duration.{Duration, MILLISECONDS}
import scala.concurrent.{Await, ExecutionContext}

import ai.zipline.aggregator.test.Column
import ai.zipline.api.Extensions.{GroupByOps, MetadataOps}
import ai.zipline.api.KVStore.PutRequest
import ai.zipline.api.{Accuracy, Builders, Constants, IntType, KVStore, LongType, Operation, StringType, StructType, TimeUnit, Window, GroupBy => GroupByConf}
import ai.zipline.fetcher.Fetcher.Request
import ai.zipline.fetcher.{Fetcher, GroupByServingInfoParsed, JavaFetcher, MetadataStore}
import ai.zipline.spark.Extensions._
import ai.zipline.spark._
import junit.framework.TestCase
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.apache.spark.sql.functions.avg
import org.apache.spark.sql.{Row, SparkSession}
import org.junit.Assert.assertEquals

class FetcherTest extends TestCase {
  val spark: SparkSession = SparkSessionBuilder.build("FetcherTest", local = true)

  private val namespace = "fetcher_test"
  spark.sql(s"CREATE DATABASE IF NOT EXISTS $namespace")

  // TODO: Pull the code here into what streaming can use.
  def putStreaming(groupByConf: GroupByConf,
                   fetcher: Fetcher,
                   kvStore: KVStore,
                   tableUtils: TableUtils,
                   ds: String): Unit = {
    val servingInfo: GroupByServingInfoParsed = fetcher.getGroupByServingInfo(groupByConf.metaData.cleanName)
    val groupBy = GroupBy.from(groupByConf, PartitionRange(ds, ds), tableUtils)
    // for events this will select ds-1 <= ts < ds
    val selected = groupBy.inputDf.filter(s"ds='$ds'")
    println("streaming inserts")
    selected.show()
    val keys = groupByConf.keyColumns.asScala.toArray
    val values = groupBy.preAggSchema.fields.map(_.name)
    val keyIndices = keys.map(selected.schema.fieldIndex)
    val valueIndices = values.map(selected.schema.fieldIndex)
    val tsIndex = selected.schema.fieldIndex(Constants.TimeColumn)
    val keyValueTs =
      selected.rdd
        .map { row =>
          val keys = keyIndices.map(row.get)
          val keyBytes = servingInfo.keyCodec.encodeArray(keys)
          val values = valueIndices.map(row.get)
          val valueBytes = servingInfo.selectedCodec.encodeArray(values)
          val ts = row.get(tsIndex).asInstanceOf[Long]
          (keyBytes, valueBytes, ts)
        }
        .collect()
    val puts = keyValueTs.map {
      case (keyBytes, valueBytes, ts) =>
        PutRequest(keyBytes, valueBytes, groupByConf.streamingDataset, Some(ts))
    }
    kvStore.multiPut(puts)
  }

  def getInMemKVStore() = {
    implicit val executionContext: ExecutionContext = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(1))
    implicit val tableUtils: TableUtils = TableUtils(spark)
    new InMemoryKvStore()
  }

  def testTemporalFetch(): Unit = {

    implicit val executionContext: ExecutionContext = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(1))
    implicit val tableUtils: TableUtils = TableUtils(spark)
    val inMemoryKvStore = new InMemoryKvStore()
    @transient lazy val fetcher = new Fetcher(inMemoryKvStore)
    @transient lazy val javaFetcher = new JavaFetcher(inMemoryKvStore)

    val today = Constants.Partition.at(System.currentTimeMillis())
    val yesterday = Constants.Partition.before(today)

    // temporal events
    val paymentCols =
      Seq(Column("user", StringType, 10), Column("vendor", StringType, 10), Column("payment", LongType, 100))
    val paymentsTable = "payments_table"
    DataFrameGen.events(spark, paymentCols, 100000, 60).save(paymentsTable)
    val userPaymentsGroupBy = Builders.GroupBy(
      sources = Seq(Builders.Source.events(query = Builders.Query(), table = paymentsTable)),
      keyColumns = Seq("user"),
      aggregations = Seq(
        Builders.Aggregation(operation = Operation.COUNT,
                             inputColumn = "payment",
                             windows = Seq(new Window(6, TimeUnit.HOURS), new Window(14, TimeUnit.DAYS)))),
      metaData = Builders.MetaData(name = "unit_test.user_payments", namespace = namespace)
    )

    // snapshot events
    val ratingCols =
      Seq(Column("user", StringType, 10),
          Column("vendor", StringType, 10),
          Column("rating", IntType, 5),
          Column("bucket", StringType, 5))
    val ratingsTable = "ratings_table"
    DataFrameGen.events(spark, ratingCols, 100000, 180).save(ratingsTable)
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
    val userBalanceCols =
      Seq(Column("user", StringType, 10), Column("balance", IntType, 5000))
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

    // queries
    val queryCols =
      Seq(Column("user", StringType, 10), Column("vendor", StringType, 10))
    val queriesTable = s"$namespace.queries_table"
    val queriesDf = DataFrameGen
      .events(spark, queryCols, 100000, 4)
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
        Builders.JoinPart(groupBy = creditGroupBy, prefix = "b"),
        Builders.JoinPart(groupBy = creditGroupBy, prefix = "a")
      ),
      metaData = Builders.MetaData(name = "test.payments_join", namespace = namespace, team = "zipline")
    )
    val joinedDf = new Join(joinConf, today, tableUtils).computeJoin()
    val joinTable = s"$namespace.join_test_expected"
    joinedDf.save(joinTable)
    val todaysExpected = tableUtils.sql(s"SELECT * FROM $joinTable WHERE ds='$today'")

    def serve(groupByConf: GroupByConf): Unit = {

      GroupByUpload.run(groupByConf, yesterday, Some(tableUtils))
      inMemoryKvStore.bulkPut(groupByConf.kvTable, groupByConf.batchDataset, null)
      if (groupByConf.inferredAccuracy == Accuracy.TEMPORAL) {
        inMemoryKvStore.create(groupByConf.streamingDataset)
        putStreaming(groupByConf, fetcher, inMemoryKvStore, tableUtils, today)
      }
    }
    joinConf.joinParts.asScala.foreach(jp => serve(jp.groupBy))

    val todaysQueries = tableUtils.sql(s"SELECT * FROM $queriesTable WHERE ds='$today'")
    val keys = todaysQueries.schema.fieldNames.filterNot(Constants.ReservedColumns.contains)
    val keyIndices = keys.map(todaysQueries.schema.fieldIndex)
    val tsIndex = todaysQueries.schema.fieldIndex(Constants.TimeColumn)
    val metadataStore = new MetadataStore(inMemoryKvStore, timeoutMillis = 10000)
    inMemoryKvStore.create("ZIPLINE_METADATA")
    metadataStore.putJoinConf(joinConf)

    val requests = todaysQueries.rdd
      .map { row =>
        val keyMap = keyIndices.map { idx => keys(idx) -> row.get(idx).asInstanceOf[AnyRef] }.toMap
        val ts = row.get(tsIndex).asInstanceOf[Long]
        Request(joinConf.metaData.name, keyMap, Some(ts))
      }
      .collect()

    val chunkSize = 100
    def joinResponses(useJavaFetcher: Boolean = false) = {
      var latencySum: Long = 0
      var latencyCount = 0
      val blockStart = System.currentTimeMillis()
      val result = requests.iterator
        .grouped(chunkSize)
        .map { r =>
          val responses = if (useJavaFetcher){
            val javaResponse = javaFetcher.fetchJoin(r.asJava)
            FutureConverters.toScala(javaResponse).map(_.asScala.toSeq)
          } else {
            fetcher.fetchJoin(r)
          }
          System.currentTimeMillis() -> responses }
        .flatMap {
          case (start, future) =>
            val result = Await.result(future, Duration(10000, MILLISECONDS))
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
    var count = 10
    var latencySum = 0.0
    var qpsSum = 0.0
    (0 until count).foreach { _ =>
      val (latency, qps, _) = joinResponses()
      latencySum += latency
      qpsSum += qps
    }
    println(s"""
         |Averaging fetching stats (Scala Fetcher) over ${requests.length} requests $count times
         |with batch size: $chunkSize
         |average qps: ${qpsSum / count}
         |average latency: ${latencySum / count}
         |""".stripMargin)

    latencySum = 0.0
    qpsSum = 0.0
    (0 until count).foreach { _ =>
      val (latency, qps, _) = joinResponses(true)
      latencySum += latency
      qpsSum += qps
    }
    println(s"""
               |Averaging fetching stats (Java Fetcher) over ${requests.length} requests $count times
               |with batch size: $chunkSize
               |average qps: ${qpsSum / count}
               |average latency: ${latencySum / count}
               |""".stripMargin)

    val columns = todaysExpected.schema.fields.map(_.name)
    val responseRows: Seq[Row] = joinResponses(true)._3.map { res =>
      val all: Map[String, AnyRef] =
        res.request.keys ++
          res.values ++
          Map(Constants.PartitionColumn -> today) ++
          Map(Constants.TimeColumn -> new lang.Long(res.request.atMillis.get))
      val values: Array[Any] = columns.map(all.get(_).orNull)
      KvRdd
        .toSparkRow(values, StructType.from("record", Conversions.toZiplineSchema(todaysExpected.schema)))
        .asInstanceOf[GenericRow]
    }

    println(todaysExpected.schema.pretty)
    val keyishColumns = List("vendor_id", "user_id", "ts", "ds")
    val responseRdd = tableUtils.sparkSession.sparkContext.parallelize(responseRows)
    val responseDf = tableUtils.sparkSession.createDataFrame(responseRdd, todaysExpected.schema)
    println("queries:")
    todaysQueries.order(keyishColumns).show()
    println("expected:")
    todaysExpected.order(keyishColumns).show()
    println("response:")
    responseDf.order(keyishColumns).show()
    val diff = Comparison.sideBySide(responseDf, todaysExpected, keyishColumns)
    assertEquals(todaysQueries.count(), responseDf.count())
    if (diff.count() > 0) {
      println(s"Diff count: ${diff.count()}")
      println(s"diff result rows:")
      diff.show()
    }
    assertEquals(diff.count(), 0)
  }
}
