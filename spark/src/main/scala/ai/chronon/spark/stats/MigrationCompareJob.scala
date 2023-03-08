package ai.chronon.spark.stats

import ai.chronon.api
import ai.chronon.api.Constants
import ai.chronon.api.DataModel.Events
import ai.chronon.api.Extensions._
import ai.chronon.online.{DataMetrics, SparkConversions}
import ai.chronon.spark.{Analyzer, PartitionRange, TableUtils}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

/**
  * Compare Job for migration to AFP.
  * Leverage the compare module for computation between sources.
  * Follow pattern of staging query for dividing long ranges into reasonable chunks.
  * Follow pattern of OOC for computing offline and uploading online as well.
  */
class MigrationCompareJob(
    session: SparkSession,
    joinConf: api.Join,
    stagingQueryConf: api.StagingQuery,
    startDate: String = null,
    endDate: String = null
) extends Serializable {
  val tableUtils: TableUtils = TableUtils(session)
  def run(): (DataFrame, DataFrame, DataMetrics) = {
    assert(endDate != null, "End date for the comparison should not be null")
    val partitionRange = if (startDate != null) {
      PartitionRange(startDate, endDate).betweenClauses
    } else {
      s"ds <= '${endDate}'"
    }
    val leftDf = tableUtils.sql(s"""
        |SELECT *
        |FROM ${joinConf.metaData.outputTable}
        |WHERE ${partitionRange}
        |""".stripMargin)
    val rightDf = tableUtils.sql(s"""
        |SELECT *
        |FROM ${stagingQueryConf.metaData.outputTable}
        |WHERE ${partitionRange}
        |""".stripMargin)
    val (compareDf, metricsDf, metrics) =
      CompareJob.compare(leftDf, rightDf, getJoinKeys(joinConf), migrationCheck = true)

    // Save the comparison table
    println("Saving output..")
    println(s"Output schema ${compareDf.schema.fields.map(sb => (sb.name, sb.dataType)).toMap.mkString("\n - ")}")
    tableUtils.insertUnPartitioned(compareDf,
                                   getCompareOutputTableName(joinConf, stagingQueryConf),
                                   saveMode = SaveMode.Overwrite)
    println("Finished compare stats.")
    (compareDf, metricsDf, metrics)
  }

  def getCompareOutputTableName(joinConf: api.Join, stagingQueryConf: api.StagingQuery): String = {
    val namespace = joinConf.metaData.outputNamespace
    val joinName = joinConf.metaData.cleanName
    val stagingQueryName = stagingQueryConf.metaData.cleanName
    s"${namespace}.migration_compare_join_${joinName}_${stagingQueryName}"
  }

  def getJoinKeys(joinConf: api.Join): Seq[String] = {
    var keyCols = joinConf.leftKeyCols ++ Seq(Constants.PartitionColumn)
    if (joinConf.left.dataModel == Events) {
      keyCols = keyCols ++ Seq(Constants.TimeColumn)
    }
    keyCols
  }

  def analyze(): Unit = {
    // Extract the schema of the Join, StagingQuery and the keys before calling this.
    val analyzer = new Analyzer(tableUtils, joinConf, enableHitter = false)
    val joinChrononSchema = analyzer.analyzeJoin(joinConf, false)._1
    val joinSchema = joinChrononSchema.map{ case(k,v) => (k, SparkConversions.fromChrononType(v)) }.toMap
    val stagingQuerySchema = tableUtils.sql(stagingQueryConf.query).schema.fields.map(sb => (sb.name, sb.dataType)).toMap

    CompareJob.checkConsistency(
      joinSchema,
      stagingQuerySchema,
      getJoinKeys(joinConf),
      migrationCheck = true)
  }
}
