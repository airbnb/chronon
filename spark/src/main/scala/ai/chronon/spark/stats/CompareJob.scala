package ai.chronon.spark.stats

import ai.chronon
import ai.chronon.api
import ai.chronon.api.{Builders, Constants, EventSource, Query, Source}
import ai.chronon.api.DataModel.Events
import ai.chronon.api.Extensions._
import ai.chronon.online.{DataMetrics, JoinCodec, SparkConversions}
import ai.chronon.spark.{Analyzer, PartitionRange, StagingQuery, TableUtils}
import org.apache.spark.sql.{DataFrame, SaveMode}

import java.util
import scala.util.ScalaVersionSpecificCollectionsConverter
import scala.collection.JavaConverters._

/**
  * Compare Job for comparing data between joins, staging queries and raw queries.
  * Leverage the compare module for computation between sources.
  */
class CompareJob(
    tableUtils: TableUtils,
    joinConf: api.Join,
    stagingQueryConf: api.StagingQuery = null,
    groupByConf: api.GroupBy = null,
    modelName: String = null,
    featureNames: List[String] = null,
    startDate: String = Constants.Partition.at(System.currentTimeMillis()),
    endDate: String = Constants.Partition.at(System.currentTimeMillis())
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

    val partitionRange = PartitionRange(startDate, endDate)
    val leftDf = tableUtils.sql(s"""
        |SELECT *
        |FROM ${joinConf.metaData.outputTable}
        |WHERE ${partitionRange.betweenClauses}
        |""".stripMargin)

    // Run the staging query sql directly
    val rightDf = tableUtils.sql(
      stagingQueryConf.query
        .replaceAll(StagingQuery.StartDateRegex, startDate)
        .replaceAll(StagingQuery.EndDateRegex, endDate)
        .replaceAll(StagingQuery.LatestDateRegex, endDate)
    )
    val (compareDf: DataFrame, metricsDf: DataFrame, metrics: DataMetrics) =
      CompareBaseJob.compare(leftDf, rightDf, getJoinKeys(joinConf), migrationCheck = true)

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
    CompareJob.printAndGetBasicMetrics(metrics)

    println("Finished compare stats.")
    (compareDf, metricsDf, metrics)
  }

  def streamingRun(): (DataFrame, DataFrame, DataMetrics) = {
    // This specifically targets the streaming flow and we should compare the TransformEvent events with the AFP
    // backfilled values and generate stats around it.

    println("Building Join with left as events from the TransformEvent table")
    val modelFilter = s"model_name = ${modelName}"
    val loggedSource: Source = new Source()
    val loggedEvents: EventSource = new EventSource()
    val query = new Query()

    val selects = new util.HashMap[String, String]()
    // We need to select the keys from the GB
    ScalaVersionSpecificCollectionsConverter.convertJavaListToScala(
      groupByConf.getKeyColumns
    ).foreach { key => selects.put(key, key) }

    query.setSelects(selects)
    query.setTimeColumn(Constants.TimeColumn)
    query.setStartPartition(startDate)
    query.setEndPartition(endDate)
    val wheres = ScalaVersionSpecificCollectionsConverter.convertScalaSeqToJava(
      Seq(modelFilter)
    )
    query.setWheres(wheres)
    loggedEvents.setQuery(query)
    loggedEvents.setTable("jitney.bighead_deepthought_transformevent_v1")
    loggedSource.setEvents(loggedEvents)

    /*
    // mark OOC tables as chronon_ooc_table
    if (!join.metaData.isSetTableProperties) {
      copiedJoin.metaData.setTableProperties(new util.HashMap[String, String]())
    }
    copiedJoin.metaData.tableProperties.put(Constants.ChrononOOCTable, true.toString)
    copiedJoin
    */
    val joinConf = Builders.Join(
      left = loggedSource,
      joinParts = Seq(
        Builders.JoinPart(
          groupByConf
        )
      ),
      metaData = Builders.MetaData(name = "compare_trust", namespace = namespace, team = "chronon")
    )

    val join = new chronon.spark.Join(joinConf, endDate, tableUtils)
    println("Starting compute Join for comparison table")
    val backfillDf = join.computeJoin(Some(30))

    // Include the feature names that we are interested in
    featureNames.foreach { feature =>
      selects.put(s"element_at(element_at(features, 0), ${feature})", feature)
    }

    val keys = ScalaVersionSpecificCollectionsConverter.convertJavaListToScala(
      groupByConf.getKeyColumns
    )
    val colsToExtract = featureNames.map { feature =>
      s"element_at(element_at(features, 0), ${feature}) AS ${feature}"
    }.toSeq ++ keys ++ Seq("timestamp AS ts", "ds")

    val partitionRange = PartitionRange(startDate, endDate)
    val loggedDf = tableUtils.sql(s"""
        |SELECT ${colsToExtract.mkString(",")}
        |FROM jitney.bighead_deepthought_transformevent_v1
        |WHERE ${partitionRange.betweenClauses} AND ${modelFilter}
        |""".stripMargin)

    val (compareDf: DataFrame, metricsDf: DataFrame, metrics: DataMetrics) =
      CompareBaseJob.compare(loggedDf, backfillDf, getJoinKeys(joinConf), migrationCheck = true)

    (compareDf, metricsDf, metrics)
    /*
    val copiedJoin = joinConf.deepCopy()
    val loggedSource: Source = new Source()
    val loggedEvents: EventSource = new EventSource()
    val query = new Query()
    val mapping = joinConf.leftKeyCols.map(k => k -> k)
    val selects = new util.HashMap[String, String]()
    mapping.foreach { case (key, value) => selects.put(key, value) }
    query.setSelects(selects)
    query.setTimeColumn(Constants.TimeColumn)
    query.setStartPartition(joinConf.left.query.startPartition)
    // apply sampling logic to reduce OOC offline compute overhead
    val wheres = ScalaVersionSpecificCollectionsConverter.convertScalaSeqToJava(
      if (joinConf.metaData.consistencySamplePercent < 100) {
        Seq(s"RAND() <= ${joinConf.metaData.consistencySamplePercent / 100}")
      } else {
        Seq()
      }
    )
    query.setWheres(wheres)
    loggedEvents.setQuery(query)
    loggedEvents.setTable(joinConf.metaData.loggedTable)
    loggedSource.setEvents(loggedEvents)
    copiedJoin.setLeft(loggedSource)
    val newName = joinConf.metaData.comparisonConfName
    copiedJoin.metaData.setName(newName)
    // mark OOC tables as chronon_ooc_table
    if (!copiedJoin.metaData.isSetTableProperties) {
      copiedJoin.metaData.setTableProperties(new util.HashMap[String, String]())
    }
    copiedJoin.metaData.tableProperties.put(Constants.ChrononOOCTable, true.toString)
    copiedJoin
    */
  }

  def getJoinKeys(joinConf: api.Join): Seq[String] = {
    if (joinConf.isSetRowIds) {
      ScalaVersionSpecificCollectionsConverter.convertJavaListToScala(joinConf.rowIds)
    } else {
      var keyCols = joinConf.leftKeyCols ++ Seq(Constants.PartitionColumn)
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
    val finalStagingQuery = stagingQueryConf.query
      .replaceAll(StagingQuery.StartDateRegex, startDate)
      .replaceAll(StagingQuery.EndDateRegex, endDate)
      .replaceAll(StagingQuery.LatestDateRegex, endDate)
    val stagingQuerySchema = tableUtils.sql(
      s"${finalStagingQuery} LIMIT 1").schema.fields.map(sb => (sb.name, sb.dataType)).toMap

    CompareBaseJob.checkConsistency(
      joinSchema,
      stagingQuerySchema,
      getJoinKeys(joinConf),
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
  def printAndGetBasicMetrics(metrics: DataMetrics): List[(String, Long)] = {
    val consolidatedData = metrics.series.groupBy(t => Constants.Partition.at(t._1))
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