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

package ai.chronon.spark.test.bootstrap

import org.slf4j.LoggerFactory
import ai.chronon.api.Extensions._
import ai.chronon.api._
import ai.chronon.online.Fetcher.Request
import ai.chronon.online.MetadataStore
import ai.chronon.spark.Extensions._
import ai.chronon.spark.test.{MockApi, OnlineUtils, SchemaEvolutionUtils}
import ai.chronon.spark.{Comparison, LogFlattenerJob, SparkSessionBuilder, TableUtils}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.junit.Assert.assertEquals
import org.junit.Test

import scala.concurrent.Await
import scala.concurrent.duration.Duration
import scala.util.ScalaJavaConversions._

class LogBootstrapTest {
  @transient lazy val logger = LoggerFactory.getLogger(getClass)

  val spark: SparkSession = SparkSessionBuilder.build("BootstrapTest", local = true)
  val namespace = "test_log_bootstrap"
  private implicit val tableUtils: TableUtils = TableUtils(spark)
  tableUtils.createDatabase(namespace)
  private val today = tableUtils.partitionSpec.at(System.currentTimeMillis())

  @Test
  def testBootstrap(): Unit = {

    // group by
    val groupBy = BootstrapUtils.buildGroupBy(namespace, spark)
    val groupBy2 = groupBy
      .deepCopy()
      .setAggregations(
        Seq(Builders.Aggregation(operation = Operation.SUM, inputColumn = "amount_dollars")).toJava
      )
      .setMetaData(Builders.MetaData(name = "unit_test.user_transactions_v2", namespace = namespace, team = "chronon"))

    // query
    val queryTable = BootstrapUtils.buildQuery(namespace, spark)

    // create Join V1 => V2 to simulate feature evolution and version upgrade
    val baseJoinV1 = Builders.Join(
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
    val baseJoinV2 = baseJoinV1
      .deepCopy()
      .setJoinParts(
        Seq(
          Builders.JoinPart(groupBy = groupBy),
          Builders.JoinPart(groupBy = groupBy2)
        ).toJava
      )

    def createBootstrapJoin(baseJoin: Join): Join = {
      val join = baseJoin.deepCopy()
      join.getMetaData.setName("test.user_transaction_features.bootstrap")
      join.setBootstrapParts(
        Seq(
          Builders.BootstrapPart(
            table = join.metaData.loggedTable
          )
        ).toJava
      )
      join
    }

    // Create corresponding Log-Bootstrap version for the Joins
    val joinV1 = createBootstrapJoin(baseJoinV1)
    val joinV2 = createBootstrapJoin(baseJoinV2)

    // Init artifacts to run online fetching and logging
    val kvStore = OnlineUtils.buildInMemoryKVStore(namespace)
    val mockApi = new MockApi(() => kvStore, namespace)
    val endDs = spark.table(queryTable).select(max(tableUtils.partitionColumn)).head().getString(0)
    OnlineUtils.serve(tableUtils, kvStore, () => kvStore, namespace, endDs, groupBy)
    val fetcher = mockApi.buildFetcher(debug = true)

    val metadataStore = new MetadataStore(kvStore, timeoutMillis = 10000)
    kvStore.create(Constants.ChrononMetadataKey)
    metadataStore.putJoinConf(joinV1)

    val requests = spark
      .table(queryTable)
      .where(col(tableUtils.partitionColumn) === endDs)
      .where(col("user").isNotNull and col("request_id").isNotNull)
      .select("user", "request_id", "ts")
      .collect()
      .map { row =>
        val (user, requestId, ts) = (row.getLong(0), row.getString(1), row.getLong(2))
        Request(joinV1.metaData.nameToFilePath,
                Map(
                  "user" -> user,
                  "request_id" -> requestId,
                  "request_id_2" -> requestId
                ).asInstanceOf[Map[String, AnyRef]],
                atMillis = Some(ts))
      }
    val future = fetcher.fetchJoin(requests)
    val responses = Await.result(future, Duration.Inf).toArray // force through logResponse iterator

    // Populate log table
    val logs = mockApi.flushLoggedValues
    assertEquals(responses.length, requests.length)
    assertEquals(logs.length, 1 + requests.length)
    mockApi
      .loggedValuesToDf(logs, spark)
      .save(mockApi.logTable, partitionColumns = Seq(tableUtils.partitionColumn, "name"))
    SchemaEvolutionUtils.runLogSchemaGroupBy(mockApi, today, endDs)
    val flattenerJob = new LogFlattenerJob(spark, joinV1, endDs, mockApi.logTable, mockApi.schemaTable)
    flattenerJob.buildLogTable()

    val logDf = spark.table(joinV1.metaData.loggedTable)
    assertEquals(logDf.count(), responses.length)

    val baseJoinJob = new ai.chronon.spark.Join(baseJoinV2, endDs, tableUtils)
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
        coalesce(logDf("unit_test_user_transactions_amount_dollars_sum_15d"),
                 baseOutput("unit_test_user_transactions_amount_dollars_sum_15d"))
          .as("unit_test_user_transactions_amount_dollars_sum_15d"),
        baseOutput("unit_test_user_transactions_v2_amount_dollars_sum"), // not covered by logging
        logDf("request_id_2"),
        baseOutput("request_id").as("ext_contextual_request_id"),
        logDf("request_id_2").as("ext_contextual_request_id_2"),
        baseOutput("ds")
      )

    val joinJob = new ai.chronon.spark.Join(joinV2, endDs, tableUtils)
    val computed = joinJob.computeJoin()

    val overlapCount = baseOutput.join(logDf, Seq("request_id", "ds")).count()
    logger.info(s"""Debugging information:
         |base count: ${baseOutput.count()}
         |overlap keys between base and log: ${overlapCount}
         |""".stripMargin)

    val diff = Comparison.sideBySide(computed, expected, List("request_id", "user", "ts", "ds"))
    if (diff.count() > 0) {
      logger.info(s"Actual count: ${computed.count()}")
      logger.info(s"Expected count: ${expected.count()}")
      logger.info(s"Diff count: ${diff.count()}")
      logger.info(s"diff result rows")
      diff.show()
    }

    assertEquals(0, diff.count())
  }
}
