package ai.chronon.spark.test.bootstrap

import ai.chronon.api.Extensions._
import ai.chronon.api._
import ai.chronon.online.Fetcher.Request
import ai.chronon.online.MetadataStore
import ai.chronon.spark.Extensions._
import ai.chronon.spark.test.{MockApi, OnlineUtils, SchemaEvolutionUtils}
import ai.chronon.spark.{Comparison, LogFlattenerJob, SparkSessionBuilder, TableUtils}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{coalesce, col, max}
import org.junit.Assert.assertEquals
import org.junit.Test

import scala.concurrent.Await
import scala.concurrent.duration.{Duration, SECONDS}
import scala.util.ScalaVersionSpecificCollectionsConverter

class LogBootstrapTest {

  val spark: SparkSession = SparkSessionBuilder.build("BootstrapTest", local = true)
  val namespace = "test_log_bootstrap"
  spark.sql(s"CREATE DATABASE IF NOT EXISTS $namespace")
  private val tableUtils = TableUtils(spark)
  private val today = Constants.Partition.at(System.currentTimeMillis())

  @Test
  def testBootstrap(): Unit = {

    // group by
    val groupBy = Utils.buildGroupBy(namespace, spark)

    // query
    val queryTable = Utils.buildQuery(namespace, spark)

    val baseJoin = Builders.Join(
      left = Builders.Source.events(
        table = queryTable,
        query = Builders.Query()
      ),
      joinParts = Seq(Builders.JoinPart(groupBy = groupBy)),
      externalParts = Seq(
        Builders.ExternalPart(
          Builders.ContextualSource(
            Array(StructField("request_id", StringType), StructField("request_id_2", StringType))
          )
        )),
      rowIds = Seq("request_id"),
      metaData = Builders.MetaData(name = "test.user_transaction_features",
                                   namespace = namespace,
                                   team = "chronon",
                                   samplePercent = 100.0)
    )

    val join = baseJoin.deepCopy()
    join.getMetaData.setName("test.user_transaction_features.bootstrap")
    join.setBootstrapParts(
      ScalaVersionSpecificCollectionsConverter.convertScalaSeqToJava(
        Seq(
          Builders.BootstrapPart(
            table = join.metaData.loggedTable
          )
        )
      ))

    val kvStore = OnlineUtils.buildInMemoryKVStore(namespace)
    val mockApi = new MockApi(() => kvStore, namespace)
    val endDs = spark.table(queryTable).select(max(Constants.PartitionColumn)).head().getString(0)
    OnlineUtils.serve(tableUtils, kvStore, () => kvStore, namespace, endDs, groupBy)
    val fetcher = mockApi.buildFetcher(debug = true)

    val metadataStore = new MetadataStore(kvStore, timeoutMillis = 10000)
    kvStore.create(Constants.ChrononMetadataKey)
    metadataStore.putJoinConf(join)

    val requests = spark
      .table(queryTable)
      .where(col(Constants.PartitionColumn) === endDs)
      .where(col("user").isNotNull and col("request_id").isNotNull)
      .select("user", "request_id", "ts")
      .collect()
      .map { row =>
        val (user, requestId, ts) = (row.getLong(0), row.getString(1), row.getLong(2))
        Request(join.metaData.nameToFilePath,
                Map(
                  "user" -> user,
                  "request_id" -> requestId,
                  "request_id_2" -> requestId
                ).asInstanceOf[Map[String, AnyRef]],
                atMillis = Some(ts))
      }
    val future = fetcher.fetchJoin(requests)
    val responses = Await.result(future, Duration.Inf).toArray // force through logResponse iterator

    // build flattened log table
    val logs = mockApi.flushLoggedValues
    assertEquals(responses.length, requests.length)
    assertEquals(logs.length, 1 + requests.length)
    mockApi
      .loggedValuesToDf(logs, spark)
      .save(mockApi.logTable, partitionColumns = Seq(Constants.PartitionColumn, "name"))
    SchemaEvolutionUtils.runLogSchemaGroupBy(mockApi, today, endDs)
    val flattenerJob = new LogFlattenerJob(spark, join, endDs, mockApi.logTable, mockApi.schemaTable, None)
    flattenerJob.buildLogTable()

    val logDf = spark.table(join.metaData.loggedTable)
    assertEquals(logDf.count(), responses.length)

    val baseJoinJob = new ai.chronon.spark.Join(baseJoin, endDs, tableUtils)
    val baseOutput = baseJoinJob.computeJoin()

    val expected = baseOutput
      .join(logDf, baseOutput("request_id") <=> logDf("request_id") and baseOutput("ds") <=> logDf("ds"), "left")
      .select(
        baseOutput("user"),
        baseOutput("request_id"),
        baseOutput("ts"),
        coalesce(logDf("unit_test_user_transactions_amount_dollars_sum_30d"),
                 baseOutput("unit_test_user_transactions_amount_dollars_sum_30d"))
          .as("unit_test_user_transactions_amount_dollars_sum_30d"),
        logDf("request_id_2"),
        baseOutput("ds")
      )

    val joinJob = new ai.chronon.spark.Join(join, endDs, tableUtils)
    val computed = joinJob.computeJoin()

    val diff = Comparison.sideBySide(computed, expected, List("request_id", "user", "ts", "ds"))
    if (diff.count() > 0) {
      println(s"Actual count: ${computed.count()}")
      println(s"Expected count: ${expected.count()}")
      println(s"Diff count: ${diff.count()}")
      println(s"diff result rows")
      diff.show()
    }

    assertEquals(0, diff.count())
  }
}
