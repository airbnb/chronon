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
import ai.chronon.api.Extensions._
import ai.chronon.api.{Accuracy, Builders, DoubleType, LongType, Operation, StringType, StructField, StructType, TimeUnit, Window}
import ai.chronon.spark.Extensions._
import ai.chronon.spark._
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

    // Verify that external source columns are present
    val columns = computed.columns.toSet
    assertTrue("Should contain left source columns", columns.contains("user_id"))
    assertTrue("Should contain left source columns", columns.contains("event_type"))
    assertTrue("Should contain left source columns", columns.contains("session_id"))
    assertTrue("Should contain external source prefixed columns",
               columns.exists(_.startsWith("ext_")))

    // Verify that external source columns have non-null data
    val externalColumns = computed.columns.filter(_.startsWith("ext_"))
    assertTrue("Should have at least one external column", externalColumns.nonEmpty)
    externalColumns.foreach { col =>
      val nonNullCount = computed.filter(s"$col IS NOT NULL").count()
      assertTrue(s"External column $col should have non-null values (found $nonNullCount non-null rows)",
                 nonNullCount > 0)
    }

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

    // Verify that both regular JoinPart and ExternalPart columns are present
    val columns = computed.columns.toSet
    assertTrue("Should contain left source columns", columns.contains("user_id"))
    assertTrue("Should contain left source columns", columns.contains("page_views"))
    assertTrue("Should contain regular JoinPart prefixed columns",
               columns.exists(_.startsWith("session_")))
    assertTrue("Should contain external source prefixed columns",
               columns.exists(_.endsWith("purchase_amount")))

    // Verify that external source columns have non-null data
    val externalColumns = computed.columns.filter(col => col.startsWith("ext_") || col.contains("purchase_amount"))
    assertTrue("Should have at least one external column", externalColumns.nonEmpty)
    externalColumns.foreach { col =>
      val nonNullCount = computed.filter(s"$col IS NOT NULL").count()
      assertTrue(s"External column $col should have non-null values (found $nonNullCount non-null rows)",
                 nonNullCount > 0)
    }

    // Verify that regular JoinPart columns have non-null data
    val joinPartColumns = computed.columns.filter(_.startsWith("session_"))
    assertTrue("Should have at least one JoinPart column", joinPartColumns.nonEmpty)
    joinPartColumns.foreach { col =>
      val nonNullCount = computed.filter(s"$col IS NOT NULL").count()
      assertTrue(s"JoinPart column $col should have non-null values (found $nonNullCount non-null rows)",
                 nonNullCount > 0)
    }

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

    // Verify column structure
    val columns = computed.columns.toSet
    assertTrue("Should contain external_user_id from left", columns.contains("external_user_id"))
    assertTrue("Should contain request_type from left", columns.contains("request_type"))
    assertTrue("Should contain mapped external columns",
               columns.exists(_.startsWith("ext_mapped_")))
    spark.sql(s"DROP DATABASE IF EXISTS $namespace CASCADE")
  }
}