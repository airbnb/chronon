package ai.chronon.spark.stats

import ai.chronon.api
import ai.chronon.api.Constants
import ai.chronon.api.DataModel.Events
import ai.chronon.api.Extensions._
import ai.chronon.online.{DataMetrics, SparkConversions}
import ai.chronon.spark.{Analyzer, JoinUtils, TableUtils}
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Compare Job for migration to AFP.
  * Leverage the compare module for computation between sources.
  * Follow pattern of staging query for dividing long ranges into reasonable chunks.
  * Follow pattern of OOC for computing offline and uploading online as well.
  */
class MigrationCompareJob(session: SparkSession, joinConf: api.Join, stagingConf: api.StagingQuery) extends Serializable {
  val tableUtils: TableUtils = TableUtils(session)
  def run(): (DataFrame, DataMetrics) = {
    val leftDf = tableUtils.sql(s"""
        |SELECT *
        |FROM ${joinConf.metaData.outputTable}
        |""".stripMargin)
    val rightDf = tableUtils.sql(s"""
        |SELECT *
        |FROM ${stagingConf.metaData.outputTable}
        |""".stripMargin)
    val result = CompareJob.compare(leftDf, rightDf, getJoinKeys(joinConf), migrationCheck = true)
    println("Finished compare stats.")
    result
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
    val stagingQuerySchema = tableUtils.sql(stagingConf.query).schema.fields.map(sb => (sb.name, sb.dataType)).toMap

    CompareJob.checkConsistency(
      joinSchema,
      stagingQuerySchema,
      getJoinKeys(joinConf),
      migrationCheck = true)
  }
}
