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

package ai.chronon.spark.stats

import org.slf4j.LoggerFactory
import ai.chronon.api
import ai.chronon.api.{Constants, PartitionSpec}
import ai.chronon.api.DataModel.Events
import ai.chronon.api.Extensions._
import ai.chronon.online.{DataMetrics, SparkConversions}
import ai.chronon.spark.stats.CompareJob.getJoinKeys
import ai.chronon.spark.{Analyzer, PartitionRange, StagingQuery, TableUtils, TimedKvRdd}
import org.apache.spark.sql.{DataFrame, SaveMode}

import scala.util.ScalaJavaConversions.{ListOps, MapOps}

/**
  * Compare Job for comparing data between joins, staging queries and raw queries.
  * Leverage the compare module for computation between sources.
  */
class CompareJob(
    tableUtils: TableUtils,
    joinConf: api.Join,
    stagingQueryConf: api.StagingQuery,
    startDate: String,
    endDate: String
) extends Serializable {
  @transient lazy val logger = LoggerFactory.getLogger(getClass)
  val tableProps: Map[String, String] = Option(joinConf.metaData.tableProperties)
    .map(_.toScala)
    .orNull
  val namespace = joinConf.metaData.outputNamespace
  val joinName = joinConf.metaData.cleanName
  val stagingQueryName = stagingQueryConf.metaData.cleanName
  val comparisonTableName = s"${namespace}.compare_join_query_${joinName}_${stagingQueryName}"
  val metricsTableName = s"${namespace}.compare_stats_join_query_${joinName}_${stagingQueryName}"

  def run(): (DataFrame, DataFrame, DataMetrics) = {
    assert(endDate != null, "End date for the comparison should not be null")
    // Check for schema consistency issues
    validate()

    val partitionRange = PartitionRange(startDate, endDate)(tableUtils)
    val leftDf = tableUtils.sql(s"""
        |SELECT *
        |FROM ${joinConf.metaData.outputTable}
        |WHERE ${partitionRange.betweenClauses}
        |""".stripMargin)

    // Run the staging query sql directly
    val rightDf = tableUtils.sql(
      StagingQuery.substitute(tableUtils, stagingQueryConf.query, startDate, endDate, endDate)
    )

    val (compareDf: DataFrame, metricsTimedKvRdd: TimedKvRdd, metrics: DataMetrics) =
      CompareBaseJob.compare(leftDf, rightDf, getJoinKeys(joinConf, tableUtils), tableUtils, migrationCheck = true)

    // Save the comparison table
    logger.info("Saving comparison output..")
    logger.info(
      s"Comparison schema ${compareDf.schema.fields.map(sb => (sb.name, sb.dataType)).toMap.mkString("\n - ")}")
    tableUtils.insertUnPartitioned(compareDf, comparisonTableName, tableProps, saveMode = SaveMode.Overwrite)

    // Save the metrics table
    logger.info("Saving metrics output..")
    val metricsDf = metricsTimedKvRdd.toFlatDf
    logger.info(s"Metrics schema ${metricsDf.schema.fields.map(sb => (sb.name, sb.dataType)).toMap.mkString("\n - ")}")
    tableUtils.insertUnPartitioned(metricsDf, metricsTableName, tableProps, saveMode = SaveMode.Overwrite)

    logger.info("Printing basic comparison results..")
    logger.info("(Note: This is just an estimation and not a detailed analysis of results)")
    CompareJob.printAndGetBasicMetrics(metrics, tableUtils.partitionSpec)

    logger.info("Finished compare stats.")
    (compareDf, metricsDf, metrics)
  }

  def validate(): Unit = {
    // Extract the schema of the Join, StagingQuery and the keys before calling this.
    val analyzer = new Analyzer(tableUtils, joinConf, startDate, endDate, enableHitter = false)
    val joinChrononSchema = analyzer.analyzeJoin(joinConf, false)._1
    val joinSchema = joinChrononSchema.map { case (k, v) => (k, SparkConversions.fromChrononType(v)) }.toMap
    val finalStagingQuery = StagingQuery.substitute(tableUtils, stagingQueryConf.query, startDate, endDate, endDate)
    val stagingQuerySchema =
      tableUtils.sql(s"${finalStagingQuery} LIMIT 1").schema.fields.map(sb => (sb.name, sb.dataType)).toMap

    CompareBaseJob.checkConsistency(joinSchema,
                                    stagingQuerySchema,
                                    getJoinKeys(joinConf, tableUtils),
                                    tableUtils,
                                    migrationCheck = true)
  }
}

object CompareJob {
  @transient lazy val logger = LoggerFactory.getLogger(getClass)

  /**
    * Extract the discrepancy metrics (like missing records, data mismatch) from the hourly compare metrics, consolidate
    * them into aggregations by day, which format is specified in the `partitionSpec`
    *
    * @param metrics contains hourly aggregations of compare metrics of the generated df and expected df
    * @param partitionSpec is the spec regarding the partition format
    * @return the consolidated daily data
    */
  def getConsolidatedData(metrics: DataMetrics, partitionSpec: PartitionSpec): List[(String, Long)] =
    metrics.series
      .groupBy(t => partitionSpec.at(t._1))
      .mapValues(_.map(_._2))
      .map {
        case (day, values) =>
          val aggValue = values.map { aggMetrics =>
            val leftNullSum: Long = aggMetrics
              .filterKeys(_.endsWith("left_null_sum"))
              .values
              .map(_.asInstanceOf[Long])
              .reduceOption(_ max _)
              .getOrElse(0)
            val rightNullSum: Long = aggMetrics
              .filterKeys(_.endsWith("right_null_sum"))
              .values
              .map(_.asInstanceOf[Long])
              .reduceOption(_ max _)
              .getOrElse(0)
            val mismatchSum: Long = aggMetrics
              .filterKeys(_.endsWith("mismatch_sum"))
              .values
              .map(_.asInstanceOf[Long])
              .reduceOption(_ max _)
              .getOrElse(0)
            leftNullSum + rightNullSum + mismatchSum
          }.sum
          (day, aggValue)
      }
      .toList
      .filter(_._2 > 0)
      .sortBy(_._1)

  def printAndGetBasicMetrics(metrics: DataMetrics, partitionSpec: PartitionSpec): List[(String, Long)] = {
    val consolidatedData = getConsolidatedData(metrics, partitionSpec)

    if (consolidatedData.size == 0) {
      logger.info(
        s"No discrepancies found for data mismatches and missing counts. " +
          s"It is highly recommended to explore the full metrics.")
    } else {
      consolidatedData.foreach {
        case (date, mismatchCount) =>
          logger.info(s"Found ${mismatchCount} mismatches on date '${date}'")
      }
    }
    consolidatedData
  }

  def getJoinKeys(joinConf: api.Join, tableUtils: TableUtils): Seq[String] = {
    if (joinConf.isSetRowIds) {
      joinConf.rowIds.toScala
    } else {
      val keyCols = joinConf.leftKeyCols ++ Seq(tableUtils.partitionColumn)
      if (joinConf.left.dataModel == Events) {
        keyCols ++ Seq(Constants.TimeColumn)
      } else {
        keyCols
      }
    }
  }
}
