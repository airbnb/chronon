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

import ai.chronon.api.{Accuracy, Builders, Operation, TimeUnit, Window}
import ai.chronon.spark.{BootstrapInfo, PartitionRange, SparkSessionBuilder, TableUtils}
import org.apache.spark.sql.SparkSession
import org.junit.Test

import scala.util.Random

class AvroValidationTest {

  @Test
  def testBootstrapInfoAvroValidationWithSupportedTypes(): Unit = {
    val spark: SparkSession =
      SparkSessionBuilder.build("BootstrapInfoTest" + "_" + Random.alphanumeric.take(6).mkString, local = true)

    val tableUtilsWithValidation = TableUtils(spark)

    val namespace = "bootstrap_test_ns" + "_" + Random.alphanumeric.take(6).mkString
    tableUtilsWithValidation.createDatabase(namespace)

    // Create test data with only Avro-supported types
    import spark.implicits._
    val currentTime = System.currentTimeMillis()
    val dateString = "2025-09-01"
    val testData = Seq(
      ("key1", 100, 1000L, "value1", true, currentTime, dateString),
      ("key2", 200, 2000L, "value2", false, currentTime, dateString),
      ("key1", 150, 1500L, "value3", true, currentTime, dateString)
    ).toDF("key", "int_col", "long_col", "string_col", "boolean_col", "ts", "ds")

    val tableName = "supported_bootstrap_table"
    val supportedTable = s"$namespace.$tableName"

    // Create partitioned table by ds column
    testData.write
      .mode("overwrite")
      .partitionBy("ds")
      .saveAsTable(supportedTable)

    // Create GroupBy with only supported types
    val groupBySource = Builders.Source.events(
      query = Builders.Query(
        selects = Builders.Selects("key", "int_col", "long_col", "string_col", "boolean_col"),
        startPartition = dateString
      ),
      table = supportedTable
    )

    val groupBy = Builders.GroupBy(
      sources = Seq(groupBySource),
      keyColumns = Seq("key"),
      aggregations = Seq(
        Builders.Aggregation(operation = Operation.SUM, inputColumn = "int_col"),
        Builders.Aggregation(operation = Operation.MAX, inputColumn = "long_col")
      ),
      metaData = Builders.MetaData(name = "bootstrap_test_groupby_supported", namespace = namespace),
      accuracy = Accuracy.TEMPORAL
    )

    // Create JoinPart that uses the GroupBy with supported types
    val joinPart = Builders.JoinPart(
      groupBy = groupBy,
      keyMapping = Map("key" -> "join_key")
    )

    // Create Join configuration
    val joinConf = Builders.Join(
      left = Builders.Source.events(
        query = Builders.Query(
          selects = Builders.Selects("join_key"),
          startPartition = dateString
        ),
        table = supportedTable
      ),
      joinParts = Seq(joinPart),
      metaData = Builders.MetaData(name = "bootstrap_test_join_supported", namespace = namespace)
    )

    val range = PartitionRange(dateString, dateString)(tableUtilsWithValidation)

    // This should succeed without throwing - all types are Avro compatible
    val bootstrapInfo = BootstrapInfo.from(
      joinConf = joinConf,
      range = range,
      tableUtils = tableUtilsWithValidation,
      leftSchema = None,
      computeDependency = false
    )

    assert(bootstrapInfo != null)
    assert(bootstrapInfo.joinParts.nonEmpty)
    assert(bootstrapInfo.joinParts.head.joinPart.groupBy.metaData.name == "bootstrap_test_groupby_supported")
  }

}
