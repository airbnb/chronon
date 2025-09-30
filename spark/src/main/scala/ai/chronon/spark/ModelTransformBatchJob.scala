/*
 *    Copyright (C) 2025 The Chronon Authors.
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

package ai.chronon.spark
import ai.chronon.api.Extensions.{JoinOps, MetadataOps, ModelTransformOps, SourceOps}
import ai.chronon.api.ModelTransform
import ai.chronon.online.{Metrics, ModelBackend}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.LoggerFactory

import scala.util.ScalaJavaConversions.MapOps
case class ModelTransformBatchJob(
    sparkSession: SparkSession,
    modelBackend: ModelBackend,
    join: ai.chronon.api.Join,
    endPartition: String,
    startPartitionOverride: Option[String] = None,
    stepDays: Int = 30,
    modelTransformOverrideName: Option[String] = None,
    jobContextJson: Option[String] = None
) {

  private lazy val metrics: Metrics.Context = {
    val joinMetric = Metrics.Context(Metrics.Environment.ModelTransformBatch, join)
    if (modelTransformOverride.isDefined) {
      joinMetric.copy(model = modelTransformOverride.get.name)
    } else {
      joinMetric
    }
  }

  private lazy val logger = LoggerFactory.getLogger(getClass)
  private lazy val tableUtils = TableUtils(sparkSession)
  private lazy val modelTransformOverride: Option[ModelTransform] = {
    modelTransformOverrideName.flatMap(modelTransformName =>
      join.modelTransformsListScala.find { modelTransform: ModelTransform =>
        modelTransform.name == modelTransformName
      })
  }

  private def writeFinalOutput(outputDf: DataFrame): Unit = {
    val tableProperties = Option(join.metaData.tableProperties)
      .map(_.toScala.toMap)
      .getOrElse(Map.empty[String, String])

    tableUtils.insertPartitions(
      outputDf,
      join.metaData.outputTable,
      tableProperties,
      autoExpand = true
    )
  }

  private def getComputeRanges(endPartition: String,
                               startPartitionOverride: Option[String],
                               stepDays: Int): Seq[PartitionRange] = {
    val rangeToFill = JoinUtils.getRangesToFill(
      join.left,
      tableUtils,
      endPartition,
      startPartitionOverride,
      join.historicalBackfill
    )
    val unfilledRanges = tableUtils
      .unfilledRanges(
        join.metaData.outputTable,
        rangeToFill,
        // Check ranges to fill based on pre-mt table
        Some(Seq(join.metaData.preModelTransformsTable)),
        skipFirstHole = true
      )
      .getOrElse(Seq.empty)

    val stepRanges = unfilledRanges.flatMap(_.steps(stepDays)).toSeq

    logger.info(s"""Compute ranges:
         |Total ranges to fill based on left table: $rangeToFill,
         |Unfilled ranges in the output table based on pre-mt table: $unfilledRanges
         |Unfilled ranges split by step days: $stepRanges
         |""".stripMargin)

    stepRanges
  }

  def run(): Unit = {
    val computeRanges = getComputeRanges(endPartition, startPartitionOverride, stepDays)
    computeRanges.foreach { range =>
      logger.info(s"Running model transform batch job for range: ${range.start} to ${range.end}")
      val startMillis = System.currentTimeMillis()
      val outputDfOpt = modelBackend.runModelInferenceBatchJob(
        sparkSession,
        join,
        range.start,
        range.end,
        modelTransformOverride,
        jobContextJson
      )
      if (outputDfOpt.isDefined) {
        logger.info(s"Writing output for range: ${range.start} to ${range.end}")
        outputDfOpt.foreach(writeFinalOutput)
      } else {
        logger.warn(
          s"No output DataFrame is provided for range: ${range.start} to ${range.end}. " +
            s"Assuming data has been written by ModelBackend.")
      }

      val elapsedMins = (System.currentTimeMillis() - startMillis) / (60 * 1000)
      metrics.gauge(Metrics.Name.LatencyMinutes, elapsedMins)
      metrics.gauge(Metrics.Name.PartitionCount, range.partitions.length)
      logger.info(s"Completed model transform batch job for range: ${range.start} to ${range.end}")
    }
  }
}
