package ai.chronon.spark
import ai.chronon.api.Extensions.{JoinOps, MetadataOps, ModelTransformOps, SourceOps}
import ai.chronon.api.ModelTransform
import ai.chronon.online.ModelBackend
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.LoggerFactory

import scala.util.ScalaJavaConversions.MapOps
case class ModelTransformBatchJob(
    sparkSession: SparkSession,
    modelBackend: ModelBackend,
    join: ai.chronon.api.Join,
    endPartition: String,
    startPartitionOverride: Option[String],
    stepDays: Int,
    modelTransformOverrideName: Option[String],
    jobContextJson: Option[String]
) {

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

    val stepRanges = unfilledRanges.flatMap(_.steps(stepDays))

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

      logger.info(s"Completed model transform batch job for range: ${range.start} to ${range.end}")
    }
  }
}
