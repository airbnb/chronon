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
import ai.chronon.api._
import ai.chronon.spark.Extensions._
import ai.chronon.spark.{Join, SparkSessionBuilder, TableUtils}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.DecimalType
import org.junit.Test

class AvroTest {
  val spark: SparkSession = SparkSessionBuilder.build("AvroTest", local = true)
  private val tableUtils = TableUtils(spark)
  private val today = tableUtils.partitionSpec.at(System.currentTimeMillis())
  private val monthAgo = tableUtils.partitionSpec.minus(today, new Window(30, TimeUnit.DAYS))
  private val twoMonthsAgo = tableUtils.partitionSpec.minus(today, new Window(60, TimeUnit.DAYS))

  @Test
  def testDecimal(): Unit = {

    val namespace = "test_decimal"
    tableUtils.createDatabase(namespace)

    /* create group by that uses a decimal field */
    val txnTable = s"$namespace.transactions"
    spark.sql(s"""CREATE TABLE $txnTable (
         |  user BIGINT,
         |  amount_dollars DECIMAL(18,6)
         |)
         |PARTITIONED BY (ds STRING)
         |""".stripMargin)

    // Note: DecimalType is not supported now, so use Double and cast to Decimal later on
    val txnSchema = List(
      Column("user", LongType, 100),
      Column("amount_dollars", DoubleType, 70000)
    )
    DataFrameGen
      .entities(spark, txnSchema, 4500, partitions = 40)
      .select(
        col("user"),
        col("amount_dollars").cast(DecimalType(18, 6)).as("amount_dollars"),
        col("ds")
      )
      .save(txnTable)

    val txnSource = Builders.Source.entities(
      query = Builders.Query(
        selects = Map(
          "user" -> "user",
          "amount_dollars" -> "amount_dollars"
        ),
        startPartition = monthAgo,
        timeColumn = "UNIX_TIMESTAMP(CONCAT(ds, ' 23:59:59.999')) * 1000"
      ),
      snapshotTable = txnTable
    )

    val groupBy = Builders.GroupBy(
      sources = Seq(txnSource),
      keyColumns = Seq("user"),
      aggregations = Seq(Builders.Aggregation(operation = Operation.LAST, inputColumn = "amount_dollars")),
      metaData = Builders.MetaData(name = "unit_test.transactions", namespace = namespace, team = "chronon")
    )

    /* create the left */
    val queriesSchema = List(
      Column("user", LongType, 100)
    )

    val queryTable = s"$namespace.queries"
    DataFrameGen.events(spark, queriesSchema, 3000, partitions = 180).save(queryTable)
    val left = Builders.Source.events(
      query = Builders.Query(startPartition = twoMonthsAgo),
      table = queryTable
    )

    /* create the join */
    val joinConf = Builders.Join(
      left = left,
      joinParts = Seq(Builders.JoinPart(groupBy = groupBy)),
      metaData = Builders.MetaData(name = "unit_test.test_decimal", namespace = namespace, team = "chronon")
    )
    val runner = new Join(joinConf, tableUtils.partitionSpec.minus(today, new Window(40, TimeUnit.DAYS)), tableUtils)
    val df = runner.computeJoin()
    df.printSchema()
  }

}
