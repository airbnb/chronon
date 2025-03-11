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

import ai.chronon.aggregator.test.{CStream, Column, RowsWithSchema}
import ai.chronon.api.{Constants, LongType, StringType}
import ai.chronon.online.SparkConversions
import ai.chronon.spark.TableUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import scala.collection.Seq

// This class generates dataframes given certain dataTypes, cardinalities and rowCounts of data
// Nulls are injected for all types
// String types are nulled at row level and also at the set level (some strings are always absent)
object DataFrameGen {
  //  The main api: that generates dataframes given certain properties of data
  def gen(spark: SparkSession, columns: Seq[Column], count: Int, partitionColOpt: Option[String] = None): DataFrame = {
    val tableUtils = TableUtils(spark)
    val partitionColumn = tableUtils.getPartitionColumn(partitionColOpt)
    val RowsWithSchema(rows, schema) = CStream.gen(columns, count, partitionColumn, tableUtils.partitionSpec)
    val genericRows = rows.map { row => new GenericRow(row.fieldsSeq.toArray) }.toArray
    val data: RDD[Row] = spark.sparkContext.parallelize(genericRows)
    val sparkSchema = SparkConversions.fromChrononSchema(schema)
    spark.createDataFrame(data, sparkSchema)
  }

  //  The main api: that generates dataframes given certain properties of data
  def events(spark: SparkSession, columns: Seq[Column], count: Int, partitions: Int, partitionColOpt: Option[String] = None): DataFrame = {
    val partitionColumn = TableUtils(spark).getPartitionColumn(partitionColOpt)
    val generated = gen(spark, columns :+ Column(Constants.TimeColumn, LongType, partitions), count, Some(partitionColumn))
    generated.withColumn(
      partitionColumn,
      from_unixtime(generated.col(Constants.TimeColumn) / 1000, TableUtils(spark).partitionSpec.format))
  }

  //  Generates Entity data
  def entities(spark: SparkSession, columns: Seq[Column], count: Int, partitions: Int, partitionColOpt: Option[String] = None): DataFrame = {
    val partitionColumn = TableUtils(spark).getPartitionColumn(partitionColOpt)
    gen(spark, columns :+ Column(partitionColumn, StringType, partitions), count, Some(partitionColumn))
  }

  /**
    * Mutations and snapshots generation.
    * To generate data for mutations we first generate random entities events.
    * We set these as insert mutations.
    * Then we take a sample of rows and mutate them with an is_before and after at a mutation_ts after
    * the mutation and before the end of the day.
    * Finally process each snapshot with a group by to generate the snapshot data.
    * This ensures mutations end state and snapshot data is consistent.
    * @return (SnapshotDf, MutationDf)
    */
  def mutations(spark: SparkSession,
                columns: Seq[Column],
                count: Int,
                partitions: Int,
                mutationProbability: Double,
                mutationColumnIdx: Int,
                keyColumnName: String,
                partitionColOpt: Option[String] = None): (DataFrame, DataFrame) = {
    val tableUtils = TableUtils(spark)
    val partitionColumn = tableUtils.getPartitionColumn(partitionColOpt)
    val mutationColumn = columns(mutationColumnIdx)
    // Randomly generated some entity data, store them as inserts w/ mutation_ts = ts and partition = dsOf[ts].
    val generated = gen(spark, columns :+ Column(Constants.TimeColumn, LongType, partitions), count, Some(partitionColumn))
      .withColumn("created_at", col(Constants.TimeColumn))
      .withColumn("updated_at", col(Constants.TimeColumn))
    val withInserts = generated
      .withColumn(Constants.ReversalColumn, lit(false))
      .withColumn(Constants.MutationTimeColumn, col(Constants.TimeColumn))
      .withColumn(partitionColumn,
                  from_unixtime((generated.col(Constants.TimeColumn) / 1000), tableUtils.partitionSpec.format))
      .drop()

    // Sample some of the inserted data and add a mutation time.
    val randomLerp = udf((t: Long, upper: Long) => (t + math.random * (upper - t)).toLong)
    val mutatedFromDf = withInserts
      .sample(mutationProbability)
      .withColumn(Constants.ReversalColumn, lit(true))
      .withColumn(
        Constants.MutationTimeColumn,
        randomLerp(
          col(Constants.MutationTimeColumn),
          unix_timestamp(col(partitionColumn), tableUtils.partitionSpec.format) * 1000 + 86400 * 1000)
      )
    val realizedData = spark.sparkContext.parallelize(mutatedFromDf.rdd.collect())
    val realizedFrom = spark.createDataFrame(realizedData, mutatedFromDf.schema)

    // Generate mutation values then store to fix the random value.
    val randomReplace = udf(() => (math.random * mutationColumn.cardinality).round.toDouble)
    val mutatedToDf = realizedFrom
      .withColumn(Constants.ReversalColumn, lit(false))
      .withColumn(mutationColumn.name, randomReplace())
    val realizedTo =
      spark.createDataFrame(spark.sparkContext.parallelize(mutatedToDf.rdd.collect()), mutatedToDf.schema)

    val mutationsDf = withInserts
      .union(realizedFrom)
      .union(realizedTo)

    // Generate snapshot data from mutations by doing a self join
    val seedDf = mutationsDf
      .filter(s"${Constants.ReversalColumn} = false")
      .toDF(mutationsDf.columns.map(_ + "_s"): _*)
    val expandedEventsDf = seedDf
      .as("sd")
      .join(
        mutationsDf.filter(s"${Constants.ReversalColumn} = false").as("mt"),
        col(s"${partitionColumn}") <= col(s"${partitionColumn}_s")
      )
      .select("mt.*", s"sd.${partitionColumn}_s")
      .drop(partitionColumn, Constants.ReversalColumn)
      .withColumnRenamed(s"${partitionColumn}_s", partitionColumn)
      .dropDuplicates()

    // Given expanded events aggregate data such that the snapshot data is consistent with the mutations from the day.
    val aggregator =
      new SnapshotAggregator(expandedEventsDf.schema, mutationColumn.name, keyColumnName, partitionColumn)
    val snapshotRdd = expandedEventsDf.rdd
      .keyBy(aggregator.aggregatorKey(_))
      .aggregateByKey(aggregator.init)(aggregator.update, aggregator.merge)
      .mapValues(aggregator.finalize(_))
      .map {
        case (key, value) => aggregator.toRow(key, value)
      }

    val snapshotDf = spark.createDataFrame(snapshotRdd, aggregator.outputSchema)
    (snapshotDf, mutationsDf)
  }
}
