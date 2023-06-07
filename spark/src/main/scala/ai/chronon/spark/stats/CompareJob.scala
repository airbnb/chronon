package ai.chronon.spark.stats

import ai.chronon.api
import ai.chronon.api.{Constants, PartitionSpec}
import ai.chronon.api.DataModel.Events
import ai.chronon.api.Extensions._
import ai.chronon.online.{DataMetrics, SparkConversions}
import ai.chronon.spark.{Analyzer, PartitionRange, StagingQuery, TableUtils}
import org.apache.spark.sql.{DataFrame, SaveMode}

import scala.util.ScalaVersionSpecificCollectionsConverter

/**
  * Compare Job for comparing data between joins, staging queries and raw queries.
  * Leverage the compare module for computation between sources.
  */
class CompareJob(
    tableUtils: TableUtils,
    joinConf: api.Join,
    stagingQueryConf: api.StagingQuery,
    startDate: String,
    endDate: String,
) extends Serializable {
  val tableProps: Map[String, String] = Option(joinConf.metaData.tableProperties)
    .map(ScalaVersionSpecificCollectionsConverter.convertJavaMapToScala(_).toMap)
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

    val (compareDf: DataFrame, metricsDf: DataFrame, metrics: DataMetrics) =
      CompareBaseJob.compare(leftDf, rightDf, getJoinKeys(joinConf), tableUtils, migrationCheck = true)

    // Save the comparison table
    println("Saving comparison output..")
    println(s"Comparison schema ${compareDf.schema.fields.map(sb => (sb.name, sb.dataType)).toMap.mkString("\n - ")}")
    tableUtils.insertUnPartitioned(compareDf,
                                   comparisonTableName,
                                   tableProps,
                                   saveMode = SaveMode.Overwrite)

    // Save the metrics table
    println("Saving metrics output..")
    println(s"Metrics schema ${metricsDf.schema.fields.map(sb => (sb.name, sb.dataType)).toMap.mkString("\n - ")}")
    tableUtils.insertUnPartitioned(metricsDf,
                                   metricsTableName,
                                   tableProps,
                                   saveMode = SaveMode.Overwrite)


    println("Printing basic comparison results..")
    println("(Note: This is just an estimation and not a detailed analysis of results)")
    CompareJob.printAndGetBasicMetrics(metrics, tableUtils.partitionSpec)

    println("Finished compare stats.")
    (compareDf, metricsDf, metrics)
  }

  def getJoinKeys(joinConf: api.Join): Seq[String] = {
    if (joinConf.isSetRowIds) {
      ScalaVersionSpecificCollectionsConverter.convertJavaListToScala(joinConf.rowIds)
    } else {
      var keyCols = joinConf.leftKeyCols ++ Seq(tableUtils.partitionColumn)
      if (joinConf.left.dataModel == Events) {
        keyCols = keyCols ++ Seq(Constants.TimeColumn)
      }
      keyCols
    }
  }

  def validate(): Unit = {
    // Extract the schema of the Join, StagingQuery and the keys before calling this.
    val analyzer = new Analyzer(tableUtils, joinConf, startDate, endDate, enableHitter = false)
    val joinChrononSchema = analyzer.analyzeJoin(joinConf, false)._1
    val joinSchema = joinChrononSchema.map{ case(k,v) => (k, SparkConversions.fromChrononType(v)) }.toMap
    val finalStagingQuery = StagingQuery.substitute(tableUtils, stagingQueryConf.query, startDate, endDate, endDate)
    val stagingQuerySchema = tableUtils.sql(
      s"${finalStagingQuery} LIMIT 1").schema.fields.map(sb => (sb.name, sb.dataType)).toMap

    CompareBaseJob.checkConsistency(
      joinSchema,
      stagingQuerySchema,
      getJoinKeys(joinConf),
      tableUtils,
      migrationCheck = true)
  }
}

object CompareJob {
  /*
    This will take the DataMetrics object which contains hourly aggregations of compare metrics
    and this method will extract the metrics like missing records on either side of the comparison
    and the data mismatches. We then aggregate the individual counts by day and create a map of
    the partition date and the actual missing counts.
   */
  def printAndGetBasicMetrics(metrics: DataMetrics, partitionSpec: PartitionSpec): List[(String, Long)] = {
    val consolidatedData = metrics.series.groupBy(t => partitionSpec.at(t._1))
      .mapValues(_.map(_._2))
      .map { case (day, values) =>
        val aggValue = values.map { aggMetrics =>
          val leftNullSum: Long = aggMetrics.filterKeys(_.endsWith("left_null_sum"))
            .values
            .map(_.asInstanceOf[Long])
            .reduceOption(_ max _)
            .getOrElse(0)
          val rightNullSum: Long = aggMetrics.filterKeys(_.endsWith("right_null_sum"))
            .values
            .map(_.asInstanceOf[Long])
            .reduceOption(_ max _)
            .getOrElse(0)
          val mismatchSum: Long = aggMetrics.filterKeys(_.endsWith("mismatch_sum"))
            .values
            .map(_.asInstanceOf[Long])
            .reduceOption(_ max _)
            .getOrElse(0)
          leftNullSum + rightNullSum + mismatchSum
        }.sum
        (day, aggValue)
      }
      .toList
      .sortBy(_._1)
      .filter(_._2 > 0)

    if (consolidatedData.size == 0) {
      println(s"No discrepancies found for data mismatches and missing counts. " +
        s"It is highly recommended to explore the full metrics.")
    } else {
      consolidatedData.foreach { case (date, mismatchCount) =>
        println(s"Found ${mismatchCount} mismatches on date '${date}'")
      }
    }
    consolidatedData
  }
}
