package ai.chronon.spark.stats

import ai.chronon.api.Extensions._
import ai.chronon.api._
import ai.chronon.online._
import ai.chronon.spark.Extensions._
import ai.chronon.spark.{PartitionRange, TableUtils}
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.{Row, SparkSession}

import scala.collection.JavaConverters._
import scala.util.ScalaVersionSpecificCollectionsConverter

/**
  * Summary Job for daily upload of stats.
  * Leverage the stats module for computation per range.
  * Follow pattern of staging query for dividing long ranges into reasonable chunks.
  * Follow pattern of OOC for computing offline and uploading online as well.
  */
class SummaryJob(session: SparkSession, joinConf: Join, endDate: String) extends Serializable {

  val tableUtils: TableUtils = TableUtils(session)
  private val dailyStatsTable: String = s"${joinConf.metaData.outputNamespace}.${joinConf.metaData.cleanName}_stats_daily"
  private val dailyStatsAvroTable: String = s"${joinConf.metaData.outputNamespace}.${joinConf.metaData.cleanName}_stats_daily_upload"
  private val tableProps: Map[String, String] = Option(joinConf.metaData.tableProperties)
    .map(ScalaVersionSpecificCollectionsConverter.convertJavaMapToScala(_).toMap)
    .orNull

  def dailyRun(stepDays: Option[Int] = None): Unit = {
    val unfilledRange = tableUtils.unfilledRange(dailyStatsTable, PartitionRange(null, endDate), Some(joinConf.metaData.outputTable))
    if (unfilledRange.isEmpty) {
      println(s"No data to compute for $dailyStatsTable")
      return
    }
    val computeRange = unfilledRange.get
    println(s"Daily output statistics table $dailyStatsTable unfilled range: $computeRange")
    val stepRanges = stepDays.map(computeRange.steps).getOrElse(Seq(computeRange))
    println(s"Ranges to compute: ${stepRanges.map(_.toString).pretty}")
    // We are going to build the aggregator to denormalize sketches for hive.
    stepRanges.zipWithIndex.foreach {
      case (range, index) =>
        println(s"Computing range [${index + 1}/${stepRanges.size}]: $range")
        val inputDf = tableUtils.sql(s"""SELECT *
           |FROM ${joinConf.metaData.outputTable}
           |WHERE ds BETWEEN '${range.start}' AND '${range.end}'
           |""".stripMargin)
        val stats = new StatsCompute(inputDf, joinConf.leftKeyCols)
        val aggregator = StatsGenerator.buildAggregator(stats.metrics, stats.selectedDf)
        val summaryKvRdd = stats.dailySummary(aggregator,0.1)
        if (joinConf.metaData.online) {
          // Store an Avro encoded KV Table and the schemas.
          val avroDf = summaryKvRdd.toAvroDf
          val schemas = Seq(summaryKvRdd.keyZSchema, summaryKvRdd.valueZSchema).map(AvroConversions.fromChrononSchema(_).toString(true))
          val schemaKeys = Seq(Constants.StatsKeySchemaKey, Constants.StatsValueSchemaKey)
          val metaRows = schemaKeys.zip(schemas).map{
            case (k, schema) => Row(k.getBytes(Constants.UTF8), schema.getBytes(Constants.UTF8), k, schema)
          }
          val metaRdd = tableUtils.sparkSession.sparkContext.parallelize(metaRows)
          val metaDf = tableUtils.sparkSession.createDataFrame(metaRdd, avroDf.schema)
          avroDf
            .union(metaDf).withColumn(Constants.PartitionColumn, lit(endDate))
            .save(dailyStatsAvroTable, tableProps)
        }
        stats.addDerivedMetrics(summaryKvRdd.toFlatDf, aggregator)
          .withTimeBasedColumn(Constants.PartitionColumn).save(dailyStatsTable, tableProps)
        println(s"Finished range [${index +1}/${stepRanges.size}].")
    }
    println("Finished writing stats.")
  }
}
