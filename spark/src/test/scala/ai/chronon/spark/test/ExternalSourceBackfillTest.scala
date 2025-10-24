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

import ai.chronon.aggregator.test.Column
import ai.chronon.api.{Accuracy, Builders, DoubleType, LongType, Operation, StringType, StructField, StructType, TimeUnit, Window}
import ai.chronon.spark.Extensions._
import ai.chronon.spark._
import ai.chronon.spark.catalog.TableUtils
import org.apache.spark.sql.SparkSession
import org.junit.Assert._
import org.junit.Test

import scala.util.Random

class ExternalSourceBackfillTest {
  val spark: SparkSession = SparkSessionBuilder.build("ExternalSourceBackfillTest", local = true)
  private val tableUtils = TableUtils(spark)
  private val today = tableUtils.partitionSpec.at(System.currentTimeMillis())
  private val monthAgo = tableUtils.partitionSpec.minus(today, new Window(30, TimeUnit.DAYS))

  @Test
  def testExternalSourceBackfillComputeJoin(): Unit = {
    val spark: SparkSession =
      SparkSessionBuilder.build("ExternalSourceBackfillTest" + "_" + Random.alphanumeric.take(6).mkString, local = true)
    val tableUtils = TableUtils(spark)
    val namespace = "test_namespace_" + Random.alphanumeric.take(6).mkString
    tableUtils.createDatabase(namespace)

    // Create user transaction data for offline GroupBy
    // IMPORTANT: Use same cardinality as left source to ensure key overlap
    val transactionColumns = List(
      Column("user_id", StringType, 100),  // Same cardinality as userEventColumns
      Column("amount", LongType, 1000),
      Column("transaction_type", StringType, 5)
    )

    val transactionTable = s"$namespace.user_transactions"
    spark.sql(s"DROP TABLE IF EXISTS $transactionTable")
    DataFrameGen.events(spark, transactionColumns, 5000, partitions = 100).save(transactionTable)

    // Create offline GroupBy for external source
    val offlineGroupBy = Builders.GroupBy(
      sources = Seq(
        Builders.Source.events(
          query = Builders.Query(
            selects = Map("user_id" -> "user_id", "amount" -> "amount"),
            timeColumn = "ts"
          ),
          table = transactionTable
        )
      ),
      keyColumns = Seq("user_id"),
      aggregations = Seq(
        Builders.Aggregation(
          operation = Operation.SUM,
          inputColumn = "amount",
          windows = Seq(new Window(30, TimeUnit.DAYS))
        )
      ),
      derivations = Seq(
        Builders.Derivation.star(), // Keep all base aggregation columns
        Builders.Derivation(
          name = s"es_amount",
          expression = "amount_sum_30d"
        )
      ),
      metaData = Builders.MetaData(name = s"gb_amount", namespace = namespace),
      accuracy = Accuracy.SNAPSHOT
    )

    // Create ExternalSource with offline GroupBy
    val externalSource = Builders.ExternalSource(
      metadata = Builders.MetaData(name = s"test_external_source"),
      keySchema = StructType("external_keys", Array(StructField("user_id", StringType))),
      valueSchema = StructType("external_values", Array(StructField("es_amount", LongType)))
    )
    externalSource.setOfflineGroupBy(offlineGroupBy)

    // Create left source (user events to join against)
    val userEventColumns = List(
      Column("user_id", StringType, 100),
      Column("event_type", StringType, 10),
      Column("session_id", StringType, 200)
    )

    val userEventTable = s"$namespace.user_events"
    spark.sql(s"DROP TABLE IF EXISTS $userEventTable")
    DataFrameGen.events(spark, userEventColumns, 1000, partitions = 50).save(userEventTable)

    // Create Join configuration with ExternalPart
    val joinConf = Builders.Join(
      left = Builders.Source.events(
        query = Builders.Query(
          selects = Map("user_id" -> "user_id", "event_type" -> "event_type", "session_id" -> "session_id"),
          timeColumn = "ts"
        ),
        table = userEventTable
      ),
      joinParts = Seq(),
      externalParts = Seq(
        Builders.ExternalPart(
          externalSource,
          prefix = "txn"
        )
      ),
      metaData = Builders.MetaData(name = s"test_external_part", namespace = namespace)
    )

    // Run analyzer to ensure GroupBy tables are created
    val analyzer = new Analyzer(tableUtils, joinConf, monthAgo, today)
    analyzer.run()

    // Create Join and compute
    val endPartition = monthAgo
    val join = new Join(joinConf = joinConf, endPartition = endPartition, tableUtils)
    val computed = join.computeJoin(Some(10))

    // Verify results
    assertNotNull("Computed result should not be null", computed)
    assertTrue("Result should have rows", computed.count() > 0)

    // Create expected data frame based on SQL query
    // The offlineGroupBy has a 30-day window with SUM aggregation
    val start = tableUtils.partitionSpec.minus(monthAgo, new Window(60, TimeUnit.DAYS))
    val windowDays = 30

    val expected = tableUtils.sql(s"""
                                     |WITH
                                     |   user_events AS (
                                     |      SELECT user_id, event_type, session_id, ts, ds
                                     |      FROM $userEventTable
                                     |      WHERE ds >= '$start' and ds <= '$endPartition'),
                                     |   distinct_keys AS (
                                     |      SELECT DISTINCT user_id, ds FROM user_events),
                                     |   amount_windowed AS (
                                     |      SELECT dk.user_id,
                                     |             dk.ds,
                                     |             sum(t.amount) as ext_txn_test_external_source_es_amount
                                     |      FROM distinct_keys dk
                                     |      LEFT JOIN $transactionTable t
                                     |      ON dk.user_id = t.user_id
                                     |      AND t.ds >= date_sub(dk.ds, $windowDays)
                                     |      AND t.ds < dk.ds
                                     |      GROUP BY dk.user_id, dk.ds)
                                     |   SELECT user_events.user_id,
                                     |          user_events.ts,
                                     |          user_events.event_type,
                                     |          user_events.session_id,
                                     |          amount_windowed.ext_txn_test_external_source_es_amount,
                                     |          user_events.ds
                                     | FROM user_events
                                     | LEFT OUTER JOIN amount_windowed
                                     | ON user_events.user_id = amount_windowed.user_id
                                     | AND user_events.ds = amount_windowed.ds
    """.stripMargin)

    // Compare computed and expected data frames
    val diff = Comparison.sideBySide(computed, expected, List("user_id", "ds", "ts"))
    val diffCount = diff.count()
    if (diffCount > 0) {
      diff.show(10)
    }
    assertEquals(0, diffCount)
    spark.sql(s"DROP DATABASE IF EXISTS $namespace CASCADE")
  }

  @Test
  def testMixedExternalAndJoinParts(): Unit = {
    val spark: SparkSession =
      SparkSessionBuilder.build("ExternalSourceBackfillTest_Mixed" + "_" + Random.alphanumeric.take(6).mkString, local = true)
    val tableUtils = TableUtils(spark)
    val today = tableUtils.partitionSpec.at(System.currentTimeMillis())
    val monthAgo = tableUtils.partitionSpec.minus(today, new Window(30, TimeUnit.DAYS))
    val namespace = "test_namespace_" + Random.alphanumeric.take(6).mkString
    tableUtils.createDatabase(namespace)

    // Create transaction data for external source GroupBy
    val transactionColumns = List(
      Column("user_id", StringType, 100),
      Column("purchase_amount", LongType, 5000)
    )

    val transactionTable = s"$namespace.purchase_transactions"
    spark.sql(s"DROP TABLE IF EXISTS $transactionTable")
    DataFrameGen.events(spark, transactionColumns, 1500, partitions = 80).save(transactionTable)

    // Create session data for regular JoinPart GroupBy
    val sessionColumns = List(
      Column("user_id", StringType, 100),
      Column("session_duration", LongType, 7200)
    )

    val sessionTable = s"$namespace.user_sessions"
    spark.sql(s"DROP TABLE IF EXISTS $sessionTable")
    DataFrameGen.events(spark, sessionColumns, 1200, partitions = 60).save(sessionTable)

    // Create GroupBy for external source (purchases)
    val purchaseGroupBy = Builders.GroupBy(
      sources = Seq(
        Builders.Source.events(
          query = Builders.Query(
            selects = Map("user_id" -> "user_id", "purchase_amount" -> "purchase_amount"),
            timeColumn = "ts"
          ),
          table = transactionTable
        )
      ),
      keyColumns = Seq("user_id"),
      aggregations = Seq(
        Builders.Aggregation(
          operation = Operation.AVERAGE,
          inputColumn = "purchase_amount",
          windows = Seq(new Window(7, TimeUnit.DAYS))
        )
      ),
      derivations = Seq(
        Builders.Derivation.star(), // Keep all base aggregation columns
        Builders.Derivation(
          name = s"purchase_amount",
          expression = "purchase_amount_average_7d"
        )
      ),
      metaData = Builders.MetaData(name = s"gb_purchase", namespace = namespace),
      accuracy = Accuracy.SNAPSHOT
    )

    // Create GroupBy for regular JoinPart (sessions)
    val sessionGroupBy = Builders.GroupBy(
      sources = Seq(
        Builders.Source.events(
          query = Builders.Query(
            selects = Map("user_id" -> "user_id", "session_duration" -> "session_duration"),
            timeColumn = "ts"
          ),
          table = sessionTable
        )
      ),
      keyColumns = Seq("user_id"),
      aggregations = Seq(
        Builders.Aggregation(
          operation = Operation.COUNT,
          inputColumn = "session_duration",
          windows = Seq(new Window(14, TimeUnit.DAYS))
        )
      ),
      metaData = Builders.MetaData(name = s"gb_session", namespace = namespace),
      accuracy = Accuracy.SNAPSHOT
    )

    // Create ExternalSource with offline GroupBy
    val externalSource = Builders.ExternalSource(
      metadata = Builders.MetaData(name = s"es_purchase"),
      keySchema = StructType("external_keys", Array(StructField("user_id", StringType))),
      valueSchema = StructType("external_values", Array(StructField("purchase_amount", DoubleType)))
    )
    externalSource.setOfflineGroupBy(purchaseGroupBy)

    // Create left source
    val userActivityColumns = List(
      Column("user_id", StringType, 100),
      Column("page_views", LongType, 50)
    )

    val userActivityTable = s"$namespace.user_activity"
    spark.sql(s"DROP TABLE IF EXISTS $userActivityTable")
    DataFrameGen.events(spark, userActivityColumns, 800, partitions = 40).save(userActivityTable)

    // Create Join with both ExternalPart and regular JoinPart
    val joinConf = Builders.Join(
      left = Builders.Source.events(
        query = Builders.Query(
          selects = Map("user_id" -> "user_id", "page_views" -> "page_views"),
          timeColumn = "ts"
        ),
        table = userActivityTable
      ),
      joinParts = Seq(
        Builders.JoinPart(
          groupBy = sessionGroupBy,
          prefix = "session"
        )
      ),
      externalParts = Seq(
        Builders.ExternalPart(
          externalSource,
          prefix = "purchase"
        )
      ),
      metaData = Builders.MetaData(name = s"test_mixed_join", namespace = namespace)
    )

    // Run analyzer to ensure all GroupBy tables are created
    val analyzer = new Analyzer(tableUtils, joinConf, monthAgo, today)
    analyzer.run()

    // Create Join and compute
    val endPartition = monthAgo
    val join = new Join(joinConf = joinConf, endPartition = endPartition, tableUtils)
    val computed = join.computeJoin(Some(10))

    // Verify results
    assertNotNull("Computed result should not be null", computed)
    assertTrue("Result should have rows", computed.count() > 0)

    // Create expected data frame based on SQL query
    // For windowed aggregations, we need to use window functions to aggregate over sliding time windows
    val start = tableUtils.partitionSpec.minus(monthAgo, new Window(60, TimeUnit.DAYS))

    // sessionGroupBy has a 14-day window, purchaseGroupBy has a 7-day window
    val sessionWindowDays = 14
    val purchaseWindowDays = 7

    val expected = tableUtils.sql(s"""
                                     |WITH
                                     |   user_activity AS (
                                     |      SELECT user_id, page_views, ts, ds
                                     |      FROM $userActivityTable
                                     |      WHERE ds >= '$start' and ds <= '$endPartition'),
                                     |   distinct_keys AS (
                                     |      SELECT DISTINCT user_id, ds FROM user_activity),
                                     |   session_windowed AS (
                                     |      SELECT dk.user_id,
                                     |             dk.ds,
                                     |             CASE WHEN count(s.session_duration) = 0 THEN NULL
                                     |                  ELSE count(s.session_duration)
                                     |             END as session_gb_session_session_duration_count_14d
                                     |      FROM distinct_keys dk
                                     |      LEFT JOIN $sessionTable s
                                     |      ON dk.user_id = s.user_id
                                     |      AND s.ds >= date_sub(dk.ds, $sessionWindowDays)
                                     |      AND s.ds < dk.ds
                                     |      GROUP BY dk.user_id, dk.ds),
                                     |   purchase_windowed AS (
                                     |      SELECT dk.user_id,
                                     |             dk.ds,
                                     |             avg(t.purchase_amount) as ext_purchase_es_purchase_purchase_amount
                                     |      FROM distinct_keys dk
                                     |      LEFT JOIN $transactionTable t
                                     |      ON dk.user_id = t.user_id
                                     |      AND t.ds >= date_sub(dk.ds, $purchaseWindowDays)
                                     |      AND t.ds < dk.ds
                                     |      GROUP BY dk.user_id, dk.ds)
                                     |   SELECT user_activity.user_id,
                                     |          user_activity.ts,
                                     |          user_activity.page_views,
                                     |          session_windowed.session_gb_session_session_duration_count_14d,
                                     |          purchase_windowed.ext_purchase_es_purchase_purchase_amount,
                                     |          user_activity.ds
                                     | FROM user_activity
                                     | LEFT OUTER JOIN session_windowed
                                     | ON user_activity.user_id = session_windowed.user_id
                                     | AND user_activity.ds = session_windowed.ds
                                     | LEFT OUTER JOIN purchase_windowed
                                     | ON user_activity.user_id = purchase_windowed.user_id
                                     | AND user_activity.ds = purchase_windowed.ds
    """.stripMargin)

    // Compare computed and expected data frames
    val diff = Comparison.sideBySide(computed, expected, List("user_id", "ds", "ts"))
    val diffCount = diff.count()
    if (diffCount > 0) {
        diff.show(10)
    }

    assertEquals(0, diffCount)
    spark.sql(s"DROP DATABASE IF EXISTS $namespace CASCADE")
  }

  @Test
  def testExternalSourceBackfillWithKeyMapping(): Unit = {
    val spark: SparkSession =
      SparkSessionBuilder.build("ExternalSourceBackfillTest_KeyMapping" + "_" + Random.alphanumeric.take(6).mkString, local = true)
    val tableUtils = TableUtils(spark)
    val today = tableUtils.partitionSpec.at(System.currentTimeMillis())
    val monthAgo = tableUtils.partitionSpec.minus(today, new Window(30, TimeUnit.DAYS))
    val namespace = "test_namespace_" + Random.alphanumeric.take(6).mkString
    tableUtils.createDatabase(namespace)

    // Create feature data with internal_user_id
    val featureColumns = List(
      Column("internal_user_id", StringType, 100),
      Column("feature_score", LongType, 100)
    )

    val featureTable = s"$namespace.user_features"
    spark.sql(s"DROP TABLE IF EXISTS $featureTable")
    // Generate 135 partitions to ensure we have enough data for 30-day window + 1-day shift + monthAgo (30 days) + buffer
    // Need to cover: today back to (monthAgo - 30-day window - 1-day shift) = ~91 days + buffer
    DataFrameGen.events(spark, featureColumns, 2000, partitions = 135).save(featureTable)

    // Create GroupBy using internal_user_id
    val featureGroupBy = Builders.GroupBy(
      sources = Seq(
        Builders.Source.events(
          query = Builders.Query(
            selects = Map("internal_user_id" -> "internal_user_id", "feature_score" -> "feature_score"),
            timeColumn = "ts"
          ),
          table = featureTable
        )
      ),
      keyColumns = Seq("internal_user_id"),
      aggregations = Seq(
        Builders.Aggregation(
          operation = Operation.MAX,
          inputColumn = "feature_score",
          windows = Seq(new Window(30, TimeUnit.DAYS))
        )
      ),
      derivations = Seq(
        Builders.Derivation.star(), // Keep all base aggregation columns
        Builders.Derivation(
          name = s"feature_score",
          expression = "feature_score_max_30d"
        )
      ),
      metaData = Builders.MetaData(name = "gb_feature", namespace = namespace),
      accuracy = Accuracy.SNAPSHOT
    )

    // Create ExternalSource that expects internal_user_id
    val externalSource = Builders.ExternalSource(
      metadata = Builders.MetaData(name = "es_feature"),
      keySchema = StructType("external_keys", Array(StructField("internal_user_id", StringType))),
      valueSchema = StructType("external_values", Array(StructField("feature_score", LongType)))
    )
    externalSource.setOfflineGroupBy(featureGroupBy)

    // Create left source with external_user_id
    val requestColumns = List(
      Column("external_user_id", StringType, 100),
      Column("request_type", StringType, 5)
    )

    val requestTable = s"$namespace.user_requests"
    spark.sql(s"DROP TABLE IF EXISTS $requestTable")
    // Generate 60 partitions for the left table to limit the backfill window
    // Feature table needs 135 partitions to support 60-day backfill + 30-day window + buffer
    DataFrameGen.events(spark, requestColumns, 600, partitions = 60).save(requestTable)

    // Create Join with key mapping from external_user_id to internal_user_id
    val joinConf = Builders.Join(
      left = Builders.Source.events(
        query = Builders.Query(
          selects = Map("external_user_id" -> "external_user_id", "request_type" -> "request_type"),
          timeColumn = "ts"
        ),
        table = requestTable
      ),
      externalParts = Seq(
        Builders.ExternalPart(
          externalSource,
          keyMapping = Map("external_user_id" -> "internal_user_id"),
          prefix = "mapped"
        )
      ),
      metaData = Builders.MetaData(name = s"test_keymapping_join", namespace = namespace)
    )

    // Run analyzer to ensure GroupBy tables are created (skip validation for test)
    val analyzer = new Analyzer(tableUtils, joinConf, monthAgo, today, validateTablePermission = false, skipTimestampCheck = true)
    analyzer.run()

    // Create Join and compute
    val endPartition = monthAgo
    val join = new Join(joinConf = joinConf, endPartition = endPartition, tableUtils)
    val computed = join.computeJoin(Some(10))

    // Verify results
    assertNotNull("Computed result should not be null", computed)
    assertTrue("Result should have rows", computed.count() > 0)

    // Create expected data frame based on SQL query with key mapping
    // The featureGroupBy has a 30-day window with MAX aggregation
    val start = tableUtils.partitionSpec.minus(monthAgo, new Window(60, TimeUnit.DAYS))
    val featureWindowDays = 30

    val expected = tableUtils.sql(s"""
                                     |WITH
                                     |   user_requests AS (
                                     |      SELECT external_user_id, request_type, ts, ds
                                     |      FROM $requestTable
                                     |      WHERE ds >= '$start' and ds <= '$endPartition'),
                                     |   distinct_keys AS (
                                     |      SELECT DISTINCT external_user_id, ds FROM user_requests),
                                     |   feature_windowed AS (
                                     |      SELECT dk.external_user_id,
                                     |             dk.ds,
                                     |             max(f.feature_score) as ext_mapped_es_feature_feature_score
                                     |      FROM distinct_keys dk
                                     |      LEFT JOIN $featureTable f
                                     |      ON dk.external_user_id = f.internal_user_id
                                     |      AND f.ds >= date_sub(dk.ds, $featureWindowDays)
                                     |      AND f.ds < dk.ds
                                     |      GROUP BY dk.external_user_id, dk.ds)
                                     |   SELECT user_requests.external_user_id,
                                     |          user_requests.ts,
                                     |          user_requests.request_type,
                                     |          feature_windowed.ext_mapped_es_feature_feature_score,
                                     |          user_requests.ds
                                     | FROM user_requests
                                     | LEFT OUTER JOIN feature_windowed
                                     | ON user_requests.external_user_id = feature_windowed.external_user_id
                                     | AND user_requests.ds = feature_windowed.ds
    """.stripMargin)

    // Compare computed and expected data frames
    val diff = Comparison.sideBySide(computed, expected, List("external_user_id", "ds", "ts"))
    val diffCount = diff.count()
    if (diffCount > 0) {
      diff.show(10)
    }
    assertEquals(0, diffCount)
    spark.sql(s"DROP DATABASE IF EXISTS $namespace CASCADE")
  }
}