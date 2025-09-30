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
import ai.chronon.api._
import ai.chronon.spark.Extensions._
import ai.chronon.spark._
import org.apache.spark.sql.SparkSession
import org.junit.Assert._
import org.junit.Test

import scala.util.Random

class BootstrapInfoTest {
  val spark: SparkSession = SparkSessionBuilder.build("BootstrapInfoTest", local = true)
  private val tableUtils = TableUtils(spark)
  private val today = tableUtils.partitionSpec.at(System.currentTimeMillis())
  private val monthAgo = tableUtils.partitionSpec.minus(today, new Window(30, TimeUnit.DAYS))

  @Test
  def testExternalSourceWithOfflineGroupByConversion(): Unit = {
    val suffix = "external_backfill_" + Random.alphanumeric.take(6).mkString
    val namespace = s"test_namespace_$suffix"
    tableUtils.createDatabase(namespace)

    // Create test data for the GroupBy source table
    val groupByColumns = List(
      Column("user_id", StringType, 100),
      Column("feature_value", LongType, 1000)
    )

    val groupByTable = s"$namespace.user_features"
    spark.sql(s"DROP TABLE IF EXISTS $groupByTable")
    DataFrameGen.events(spark, groupByColumns, 1000, partitions = 50).save(groupByTable)

    // Create the GroupBy for offline backfill
    val offlineGroupBy = Builders.GroupBy(
      sources = Seq(
        Builders.Source.events(
          query = Builders.Query(
            selects = Map("user_id" -> "user_id", "feature_value" -> "feature_value"),
            timeColumn = "ts"
          ),
          table = groupByTable
        )
      ),
      keyColumns = Seq("user_id"),
      aggregations = Seq(
        Builders.Aggregation(
          operation = Operation.SUM,
          inputColumn = "feature_value",
          windows = Seq(new Window(7, TimeUnit.DAYS))
        )
      ),
      metaData = Builders.MetaData(name = s"user_features_gb_$suffix", namespace = namespace),
      accuracy = Accuracy.TEMPORAL
    )

    // Create ExternalSource with offline GroupBy
    val externalSource = Builders.ExternalSource(
      metadata = Builders.MetaData(name = s"external_with_offline_$suffix"),
      keySchema = StructType("external_keys", Array(StructField("user_id", StringType))),
      valueSchema = StructType("external_values", Array(StructField("feature_value_sum_7d", LongType)))
    )
    externalSource.setOfflineGroupBy(offlineGroupBy)

    // Create a simple left source for the join
    val leftColumns = List(
      Column("user_id", StringType, 100),
      Column("request_id", StringType, 100)
    )

    val leftTable = s"$namespace.requests"
    spark.sql(s"DROP TABLE IF EXISTS $leftTable")
    DataFrameGen.events(spark, leftColumns, 500, partitions = 30).save(leftTable)

    // Create Join with ExternalPart that has offline GroupBy
    val join = Builders.Join(
      left = Builders.Source.events(
        query = Builders.Query(
          selects = Map("user_id" -> "user_id", "request_id" -> "request_id"),
          timeColumn = "ts"
        ),
        table = leftTable
      ),
      externalParts = Seq(
        Builders.ExternalPart(
          externalSource,
          prefix = "ext"
        )
      ),
      metaData = Builders.MetaData(name = s"test_join_$suffix", namespace = namespace)
    )

    // Test BootstrapInfo conversion logic
    val endPartition = today
    val range = PartitionRange(monthAgo, endPartition)(tableUtils)
    val bootstrapInfo = BootstrapInfo.from(
      joinConf = join,
      range = range,
      tableUtils = tableUtils,
      leftSchema = None,
      computeDependency = true
    )

    // Verify that ExternalPart with offline GroupBy was converted to JoinPart
    val totalJoinParts = bootstrapInfo.joinParts.length
    assertTrue("Should have at least one JoinPart after conversion", totalJoinParts > 0)

    // Verify that the converted JoinPart has the expected GroupBy
    val convertedJoinPart = bootstrapInfo.joinParts.find(_.joinPart.groupBy.metaData.name == offlineGroupBy.metaData.name)
    assertTrue("Should find converted JoinPart with matching GroupBy name", convertedJoinPart.isDefined)

    // Verify that online-only external parts are still tracked separately
    assertTrue("Should have no online-only external parts in this test", bootstrapInfo.externalParts.isEmpty)

    // Verify schema compatibility
    val joinPartMeta = convertedJoinPart.get
    assertEquals("Key schema should match", 1, joinPartMeta.keySchema.length)
    assertEquals("Key field should be user_id", "user_id", joinPartMeta.keySchema.head.name)

    spark.sql(s"DROP DATABASE IF EXISTS $namespace CASCADE")
  }

  @Test
  def testExternalSourceOnlineOnlyBehavior(): Unit = {
    val suffix = "online_only_" + Random.alphanumeric.take(6).mkString
    val namespace = s"test_namespace_$suffix"
    tableUtils.createDatabase(namespace)

    // Create ExternalSource without offline GroupBy (online-only)
    val onlineOnlyExternalSource = Builders.ExternalSource(
      metadata = Builders.MetaData(name = s"online_only_external_$suffix"),
      keySchema = StructType("online_keys", Array(StructField("user_id", StringType))),
      valueSchema = StructType("online_values", Array(StructField("online_feature", LongType)))
    )
    // Note: No offlineGroupBy set, so this remains online-only

    // Create a simple left source
    val leftColumns = List(
      Column("user_id", StringType, 100),
      Column("request_id", StringType, 100)
    )

    val leftTable = s"$namespace.requests"
    spark.sql(s"DROP TABLE IF EXISTS $leftTable")
    DataFrameGen.events(spark, leftColumns, 500, partitions = 30).save(leftTable)

    // Create Join with online-only ExternalPart
    val join = Builders.Join(
      left = Builders.Source.events(
        query = Builders.Query(
          selects = Map("user_id" -> "user_id", "request_id" -> "request_id"),
          timeColumn = "ts"
        ),
        table = leftTable
      ),
      externalParts = Seq(
        Builders.ExternalPart(
          onlineOnlyExternalSource,
          prefix = "online"
        )
      ),
      metaData = Builders.MetaData(name = s"online_only_join_$suffix", namespace = namespace)
    )

    // Test BootstrapInfo with online-only external part
    val endPartition = today
    val range = PartitionRange(monthAgo, endPartition)(tableUtils)
    val bootstrapInfo = BootstrapInfo.from(
      joinConf = join,
      range = range,
      tableUtils = tableUtils,
      leftSchema = None,
      computeDependency = true
    )

    // Verify that online-only ExternalPart was NOT converted to JoinPart
    assertEquals("Should have no JoinParts from conversion", 0, bootstrapInfo.joinParts.length)

    // Verify that online-only external part is tracked in externalParts
    assertEquals("Should have one online-only external part", 1, bootstrapInfo.externalParts.length)

    val externalPartMeta = bootstrapInfo.externalParts.head
    assertEquals("External part name should match", onlineOnlyExternalSource.metadata.name,
                 externalPartMeta.externalPart.source.metadata.name)

    spark.sql(s"DROP DATABASE IF EXISTS $namespace CASCADE")
  }

  @Test
  def testMixedExternalPartsConversion(): Unit = {
    val suffix = "mixed_" + Random.alphanumeric.take(6).mkString
    val namespace = s"test_namespace_$suffix"
    tableUtils.createDatabase(namespace)

    // Create test data for the GroupBy source table
    val groupByColumns = List(
      Column("user_id", StringType, 100),
      Column("feature_value", LongType, 1000)
    )

    val groupByTable = s"$namespace.user_features"
    spark.sql(s"DROP TABLE IF EXISTS $groupByTable")
    DataFrameGen.events(spark, groupByColumns, 1000, partitions = 50).save(groupByTable)

    // Create GroupBy for offline backfill
    val offlineGroupBy = Builders.GroupBy(
      sources = Seq(
        Builders.Source.events(
          query = Builders.Query(
            selects = Map("user_id" -> "user_id", "feature_value" -> "feature_value"),
            timeColumn = "ts"
          ),
          table = groupByTable
        )
      ),
      keyColumns = Seq("user_id"),
      aggregations = Seq(
        Builders.Aggregation(
          operation = Operation.SUM,
          inputColumn = "feature_value"
        )
      ),
      metaData = Builders.MetaData(name = s"offline_gb_$suffix", namespace = namespace),
      accuracy = Accuracy.TEMPORAL
    )

    // Create ExternalSource with offline GroupBy
    val externalSourceWithOffline = Builders.ExternalSource(
      metadata = Builders.MetaData(name = s"external_with_offline_$suffix"),
      keySchema = StructType("offline_keys", Array(StructField("user_id", StringType))),
      valueSchema = StructType("offline_values", Array(StructField("feature_value_sum", LongType)))
    )
    externalSourceWithOffline.setOfflineGroupBy(offlineGroupBy)

    // Create online-only ExternalSource
    val externalSourceOnlineOnly = Builders.ExternalSource(
      metadata = Builders.MetaData(name = s"external_online_only_$suffix"),
      keySchema = StructType("online_keys", Array(StructField("user_id", StringType))),
      valueSchema = StructType("online_values", Array(StructField("online_feature", LongType)))
    )
    // No offlineGroupBy set

    // Create left source
    val leftColumns = List(
      Column("user_id", StringType, 100),
      Column("request_id", StringType, 100)
    )

    val leftTable = s"$namespace.requests"
    spark.sql(s"DROP TABLE IF EXISTS $leftTable")
    DataFrameGen.events(spark, leftColumns, 500, partitions = 30).save(leftTable)

    // Create Join with both types of ExternalParts
    val join = Builders.Join(
      left = Builders.Source.events(
        query = Builders.Query(
          selects = Map("user_id" -> "user_id", "request_id" -> "request_id"),
          timeColumn = "ts"
        ),
        table = leftTable
      ),
      externalParts = Seq(
        Builders.ExternalPart(
          externalSourceWithOffline,
          prefix = "offline"
        ),
        Builders.ExternalPart(
          externalSourceOnlineOnly,
          prefix = "online"
        )
      ),
      metaData = Builders.MetaData(name = s"mixed_join_$suffix", namespace = namespace)
    )

    // Test BootstrapInfo with mixed external parts
    val endPartition = today
    val range = PartitionRange(monthAgo, endPartition)(tableUtils)
    val bootstrapInfo = BootstrapInfo.from(
      joinConf = join,
      range = range,
      tableUtils = tableUtils,
      leftSchema = None,
      computeDependency = true
    )

    // Verify that offline-capable ExternalPart was converted to JoinPart
    assertEquals("Should have one JoinPart from conversion", 1, bootstrapInfo.joinParts.length)
    val convertedJoinPart = bootstrapInfo.joinParts.head
    assertEquals("Converted JoinPart should have matching GroupBy name",
                 offlineGroupBy.metaData.name, convertedJoinPart.joinPart.groupBy.metaData.name)

    // Verify that online-only ExternalPart is tracked separately
    assertEquals("Should have one online-only external part", 1, bootstrapInfo.externalParts.length)
    val onlineExternalPart = bootstrapInfo.externalParts.head
    assertEquals("Online external part should have matching name",
                 externalSourceOnlineOnly.metadata.name, onlineExternalPart.externalPart.source.metadata.name)

    spark.sql(s"DROP DATABASE IF EXISTS $namespace CASCADE")
  }

  @Test
  def testExternalPartKeyMappingPreservation(): Unit = {
    val suffix = "keymapping_" + Random.alphanumeric.take(6).mkString
    val namespace = s"test_namespace_$suffix"
    tableUtils.createDatabase(namespace)

    // Create test data for the GroupBy source table
    val groupByColumns = List(
      Column("internal_user_id", StringType, 100),
      Column("feature_value", LongType, 1000)
    )

    val groupByTable = s"$namespace.user_features"
    spark.sql(s"DROP TABLE IF EXISTS $groupByTable")
    DataFrameGen.events(spark, groupByColumns, 1000, partitions = 50).save(groupByTable)

    // Create GroupBy with internal_user_id as key
    val offlineGroupBy = Builders.GroupBy(
      sources = Seq(
        Builders.Source.events(
          query = Builders.Query(
            selects = Map("internal_user_id" -> "internal_user_id", "feature_value" -> "feature_value"),
            timeColumn = "ts"
          ),
          table = groupByTable
        )
      ),
      keyColumns = Seq("internal_user_id"),
      aggregations = Seq(
        Builders.Aggregation(
          operation = Operation.AVERAGE,
          inputColumn = "feature_value"
        )
      ),
      metaData = Builders.MetaData(name = s"keymapping_gb_$suffix", namespace = namespace),
      accuracy = Accuracy.TEMPORAL
    )

    // Create ExternalSource with key mapping
    val externalSource = Builders.ExternalSource(
      metadata = Builders.MetaData(name = s"external_with_keymapping_$suffix"),
      keySchema = StructType("external_keys", Array(StructField("internal_user_id", StringType))),
      valueSchema = StructType("external_values", Array(StructField("feature_value_avg", DoubleType)))
    )
    externalSource.setOfflineGroupBy(offlineGroupBy)

    // Create left source with external_user_id
    val leftColumns = List(
      Column("external_user_id", StringType, 100),
      Column("request_id", StringType, 100)
    )

    val leftTable = s"$namespace.requests"
    spark.sql(s"DROP TABLE IF EXISTS $leftTable")
    DataFrameGen.events(spark, leftColumns, 500, partitions = 30).save(leftTable)

    // Create Join with key mapping from external_user_id to internal_user_id
    val join = Builders.Join(
      left = Builders.Source.events(
        query = Builders.Query(
          selects = Map("external_user_id" -> "external_user_id", "request_id" -> "request_id"),
          timeColumn = "ts"
        ),
        table = leftTable
      ),
      externalParts = Seq(
        Builders.ExternalPart(
          externalSource,
          keyMapping = Map("external_user_id" -> "internal_user_id"),
          prefix = "mapped"
        )
      ),
      metaData = Builders.MetaData(name = s"keymapping_join_$suffix", namespace = namespace)
    )

    // Test BootstrapInfo preserves key mapping
    val endPartition = today
    val range = PartitionRange(monthAgo, endPartition)(tableUtils)
    val bootstrapInfo = BootstrapInfo.from(
      joinConf = join,
      range = range,
      tableUtils = tableUtils,
      leftSchema = None,
      computeDependency = true
    )

    // Verify conversion occurred
    assertEquals("Should have one converted JoinPart", 1, bootstrapInfo.joinParts.length)

    val convertedJoinPart = bootstrapInfo.joinParts.head.joinPart
    assertNotNull("Key mapping should be preserved", convertedJoinPart.keyMapping)
    assertEquals("Key mapping should map external_user_id to internal_user_id",
                 "internal_user_id", convertedJoinPart.keyMapping.get("external_user_id"))

    // Verify prefix is preserved
    assertEquals("Prefix should be preserved", "mapped", convertedJoinPart.prefix)

    spark.sql(s"DROP DATABASE IF EXISTS $namespace CASCADE")
  }
}