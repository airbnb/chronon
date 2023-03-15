package ai.chronon.spark.stats

import ai.chronon.api
import ai.chronon.api.Constants
import ai.chronon.api.DataModel.Events
import ai.chronon.api.Extensions._
import ai.chronon.online.{DataMetrics, SparkConversions}
import ai.chronon.spark.{Analyzer, PartitionRange, TableUtils}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

/**
  * Compare Job for migrations to AFP.
  * Leverage the compare module for computation between sources.
  */
class CompareJob(
    session: SparkSession,
    joinConf: api.Join,
    stagingQueryConf: api.StagingQuery,
    startDate: String = null,
    endDate: String = null
) extends Serializable {
  val tableUtils: TableUtils = TableUtils(session)
  def run(): (DataFrame, DataFrame, DataMetrics) = {
    assert(endDate != null, "End date for the comparison should not be null")
    // Check for schema consistency issues
    validate()

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

    // Run the staging query sql directly
    val rightDf = tableUtils.sql(s"""
        |SELECT *
        |FROM (${stagingQueryConf.query})
        |WHERE ${partitionRange}
        |""".stripMargin)
    val (compareDf, metricsDf, metrics) =
      CompareBaseJob.compare(leftDf, rightDf, getJoinKeys(joinConf), migrationCheck = true)

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

  def validate(): Unit = {
    // Extract the schema of the Join, StagingQuery and the keys before calling this.
    val analyzer = new Analyzer(tableUtils, joinConf, enableHitter = false)
    val joinChrononSchema = analyzer.analyzeJoin(joinConf, false)._1
    val joinSchema = joinChrononSchema.map{ case(k,v) => (k, SparkConversions.fromChrononType(v)) }.toMap
    val stagingQuerySchema = tableUtils.sql(
      s"${stagingQueryConf.query} LIMIT 1").schema.fields.map(sb => (sb.name, sb.dataType)).toMap

    CompareBaseJob.checkConsistency(
      joinSchema,
      stagingQuerySchema,
      getJoinKeys(joinConf),
      migrationCheck = true)
  }
}
