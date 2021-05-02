package ai.zipline.spark.test

import ai.zipline.aggregator.base.{IntType, LongType, StringType}
import ai.zipline.aggregator.test.Column
import ai.zipline.aggregator.windowing.TsUtils
import ai.zipline.api.Extensions.{GroupByOps, JoinPartOps, MetadataOps}
import ai.zipline.api.{Accuracy, Builders, Constants, Operation, TimeUnit, Window, GroupBy => GroupByConf}
import ai.zipline.fetcher.Fetcher.Request
import ai.zipline.fetcher.KVStore.PutRequest
import ai.zipline.fetcher.{Fetcher, GroupByServingInfoParsed, KVStore, MetadataStore}
import ai.zipline.spark.Extensions._
import ai.zipline.spark._
import com.google.gson.Gson
import junit.framework.TestCase
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.apache.spark.sql.functions.avg
import org.apache.spark.sql.types.StructType
import org.junit.Assert.assertEquals

import java.lang
import java.util.concurrent.Executors
import scala.collection.JavaConverters.{asJavaIterableConverter, asScalaBufferConverter}
import scala.concurrent.duration.{Duration, MILLISECONDS}
import scala.concurrent.{Await, ExecutionContext}

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
    val selected = groupBy.inputDf
    val keys = groupByConf.keyColumns.asScala.toArray
    val values = groupBy.preAggSchema.fields.map(_.name)
    val keyIndices = keys.map(selected.schema.fieldIndex)
    val valueIndices = values.map(selected.schema.fieldIndex)
    val tsIndex = selected.schema.fieldIndex(Constants.TimeColumn)
    val keyValueTs =
      selected
        .filter(s"ds='$ds'")
        .rdd
        .map { row =>
          val keys = keyIndices.map(row.get)
          val keyBytes = servingInfo.keyCodec.encodeArray(keys)
          val values = valueIndices.map(row.get)
          val valueBytes = servingInfo.selectedCodec.encodeArray(values)
          val ts = row.get(tsIndex).asInstanceOf[Long]
          val gson = new Gson()
//          println(s"""
//               |keys: ${gson.toJson(keys)}
//               |values: ${gson.toJson(values)}
//               |ts: ${TsUtils.toStr(ts)}
//               |""".stripMargin)
          (keyBytes, valueBytes, ts)
        }
        .collect()
    val puts = keyValueTs.map {
      case (keyBytes, valueBytes, ts) =>
        PutRequest(keyBytes, valueBytes, groupByConf.streamingDataset, Some(ts))
    }
    kvStore.multiPut(puts)
  }

  def testTemporalFetch: Unit = {

    implicit val executionContext: ExecutionContext = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(1))
    implicit val tableUtils: TableUtils = TableUtils(spark)
    val inMemoryKvStore = new InMemoryKvStore()
    @transient lazy val fetcher = new Fetcher(inMemoryKvStore)

    val today = Constants.Partition.at(System.currentTimeMillis())
    val yesterday = Constants.Partition.before(today)
    val twoDaysAgo = Constants.Partition.before(yesterday)
    val tomorrow = Constants.Partition.after(today)

    // temporal events
    val paymentCols =
      Seq(Column("user", StringType, 10), Column("vendor", StringType, 10), Column("payment", LongType, 100))
    val paymentsTable = "payments_table"
    DataFrameGen.events(spark, paymentCols, 100000, 60).save(paymentsTable)
    val userPaymentsGroupBy = Builders.GroupBy(
      sources = Seq(Builders.Source.events(query = Builders.Query(), table = paymentsTable)),
      keyColumns = Seq("user"),
      aggregations = Seq(
        Builders.Aggregation(operation = Operation.AVERAGE,
                             inputColumn = "payment",
                             windows = Seq(new Window(6, TimeUnit.HOURS), new Window(14, TimeUnit.DAYS)))),
      metaData = Builders.MetaData(name = "unit_test.user_payments", namespace = namespace)
    )
    GroupByUpload.run(userPaymentsGroupBy, yesterday, Some(tableUtils))
    inMemoryKvStore.bulkPut(userPaymentsGroupBy.kvTable, userPaymentsGroupBy.batchDataset, null)
    inMemoryKvStore.create(userPaymentsGroupBy.streamingDataset)
    putStreaming(userPaymentsGroupBy, fetcher, inMemoryKvStore, tableUtils, today)
    val result = Await.result(
      fetcher.fetchGroupBys(Seq(Fetcher.Request(userPaymentsGroupBy.metaData.cleanName, Map("user" -> "user5")))),
      Duration(1000, MILLISECONDS))
    println(result)

    // snapshot events
    val ratingCols =
      Seq(Column("user", StringType, 10), Column("vendor", StringType, 10), Column("rating", IntType, 5))
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
      Seq(Column("user", StringType, 10), Column("vendor", StringType, 10))
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
    val joinTable = s"$namespace.join_test_expected"
    joinedDf.save(joinTable)
    val todaysExpected = tableUtils.sql(s"SELECT * FROM $joinTable WHERE ds='$yesterday'")

    val todaysQueries = tableUtils.sql(s"SELECT * from $queriesTable WHERE ds='$yesterday'")
    val keys = joinConf.joinParts.asScala.flatMap(_.leftToRight.keys).distinct.toArray
    val keyIndices = keys.map(todaysQueries.schema.fieldIndex)
    val tsIndex = todaysQueries.schema.fieldIndex(Constants.TimeColumn)
    val metadataStore = new MetadataStore(inMemoryKvStore)
    inMemoryKvStore.create("ZIPLINE_METADATA")
    metadataStore.putJoinConf(joinConf)

    val requests = todaysQueries.rdd
      .map { row =>
        val keyMap = keyIndices.map { idx => keys(idx) -> row.get(idx).asInstanceOf[AnyRef] }.toMap
        val ts = row.get(tsIndex).asInstanceOf[Long]
        Request(joinConf.metaData.name, keyMap, Some(ts))
      }
      .collect()
    val joinResponseFuture = fetcher.fetchJoin(requests)
    val joinResponses = Await.result(joinResponseFuture, Duration(10000, MILLISECONDS))

    val columns = todaysExpected.schema.fields.map(_.name)
    val responseRows: Seq[Row] = joinResponses.map { res =>
      val all: Map[String, AnyRef] =
        res.request.keys ++
          res.values ++
          Map(Constants.PartitionColumn -> today) ++
          Map(Constants.TimeColumn -> new lang.Long(res.request.atMillis.get))
      val values: Array[Any] = columns.map(all.get(_).orNull)
      new GenericRow(values)
    }
    println(todaysExpected.schema.pretty)
    val responseRdd = tableUtils.sparkSession.sparkContext.parallelize(responseRows)
    val responseDf = tableUtils.sparkSession.createDataFrame(responseRdd, todaysExpected.schema)
    todaysExpected.show()
    responseDf.show()
    val diff = Comparison.sideBySide(responseDf, todaysExpected, List("vendor_id", "user_id", "ts", "ds"))
    assertEquals(todaysQueries.count(), responseDf.count())
//    assertEquals(todaysQueries.count(), todaysExpected.count())
    if (diff.count() > 0) {
      println(s"Diff count: ${diff.count()}")
      println(s"diff result rows")
      diff.show()
    }
    assertEquals(diff.count(), 0) // create groupBy data

    // create queries
    // sawtooth aggregate batch data
    // split "streaming" data
    // bulk upload batch data

    // merge queries + streaming data and replay to inmemory store
    // streaming data gets "put", queries do "get"

  }
}
