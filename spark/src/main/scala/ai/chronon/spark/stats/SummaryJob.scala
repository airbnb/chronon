package ai.chronon.spark.stats

import ai.chronon.online.SparkConversions
import ai.chronon.aggregator.row.StatsGenerator
import ai.chronon.api.Extensions._
import ai.chronon.api._
import ai.chronon.spark.Extensions._
import ai.chronon.spark.{JoinUtils, PartitionRange, TableUtils}
import org.apache.spark.sql.SparkSession

/**
  * Summary Job for daily upload of stats.
  * Leverage the stats module for computation per range.
  * Follow pattern of staging query for dividing long ranges into reasonable chunks.
  * Follow pattern of OOC for computing offline and uploading online as well.
  */
class SummaryJob(session: SparkSession, joinConf: Join, endDate: String) extends Serializable {

  val tableUtils: TableUtils = TableUtils(session)
  private val dailyStatsTable = joinConf.metaData.dailyStatsOutputTable
  private val dailyStatsAvroTable = joinConf.metaData.dailyStatsUploadTable
  private val tableProps: Map[String, String] = tableUtils.getTableProperties(joinConf.metaData.outputTable).orNull

  def dailyRun(stepDays: Option[Int] = None, sample: Double = 0.1): Unit = {
    val backfillRequired = !JoinUtils.tablesToRecompute(joinConf, dailyStatsTable, tableUtils).isEmpty
    if (backfillRequired) Seq(dailyStatsTable, dailyStatsAvroTable).foreach(tableUtils.dropTableIfExists(_))
    val unfilledRanges = tableUtils
      .unfilledRanges(dailyStatsTable, PartitionRange(null, endDate), Some(Seq(joinConf.metaData.outputTable)))
      .getOrElse(Seq.empty)
    if (unfilledRanges.isEmpty) {
      println(s"No data to compute for $dailyStatsTable")
      return
    }
    unfilledRanges.foreach { computeRange =>
      println(s"Daily output statistics table $dailyStatsTable unfilled range: $computeRange")
      val stepRanges = stepDays.map(computeRange.steps).getOrElse(Seq(computeRange))
      println(s"Ranges to compute: ${stepRanges.map(_.toString).pretty}")
      // We are going to build the aggregator to denormalize sketches for hive.
      stepRanges.zipWithIndex.foreach {
        case (range, index) =>
          println(s"Computing range [${index + 1}/${stepRanges.size}]: $range")
          val joinOutputDf = tableUtils.sql(s"""
               |SELECT *
               |FROM ${joinConf.metaData.outputTable}
               |WHERE ds BETWEEN '${range.start}' AND '${range.end}'
               |""".stripMargin)
          val baseColumns = joinConf.leftKeyCols ++ joinConf.computedFeatureCols :+ Constants.PartitionColumn
          val inputDf = if (joinOutputDf.columns.contains(Constants.TimeColumn)) {
            joinOutputDf.select(Constants.TimeColumn, baseColumns: _*)
          } else {
            joinOutputDf.select(baseColumns.head, baseColumns.tail: _*)
          }
          val stats = new StatsCompute(inputDf, joinConf.leftKeyCols, joinConf.metaData.nameToFilePath)
          val aggregator = StatsGenerator.buildAggregator(
            stats.metrics,
            StructType.from("selected", SparkConversions.toChrononSchema(stats.selectedDf.schema)))
          val summaryKvRdd = stats.dailySummary(aggregator, sample)
          // Build upload table for stats store.
          summaryKvRdd.toAvroDf
            .withTimeBasedColumn(Constants.PartitionColumn)
            .save(dailyStatsAvroTable, tableProps)
          stats
            .addDerivedMetrics(summaryKvRdd.toFlatDf, aggregator)
            .save(dailyStatsTable, tableProps)
          println(s"Finished range [${index + 1}/${stepRanges.size}].")
      }
    }
    println("Finished writing stats.")
  }
}
