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
import ai.chronon
import ai.chronon.api.Extensions._
import ai.chronon.api._
import ai.chronon.online._
import ai.chronon.spark.Extensions._
import ai.chronon.spark.{PartitionRange, TableUtils}
import org.apache.spark.sql.SparkSession
import java.util

import scala.util.ScalaJavaConversions.{JListOps, ListOps, MapOps}

import ai.chronon.online.OnlineDerivationUtil.timeFields

class ConsistencyJob(session: SparkSession, joinConf: Join, endDate: String) extends Serializable {
  @transient lazy val logger = LoggerFactory.getLogger(getClass)

  val tblProperties: Map[String, String] = Option(joinConf.metaData.tableProperties)
    .map(_.toScala)
    .getOrElse(Map.empty[String, String])
  implicit val tableUtils: TableUtils = TableUtils(session)

  // Replace join's left side with the logged table events to determine offline values of the aggregations.
  private def buildComparisonJoin(): Join = {
    logger.info("Building Join With left as logged")
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
    val wheres = if (joinConf.metaData.consistencySamplePercent < 100) {
      Seq(s"RAND() <= ${joinConf.metaData.consistencySamplePercent / 100}")
    } else {
      Seq()
    }
    query.setWheres(wheres.toJava)
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
  }

  private def buildComparisonTable(): Unit = {
    val unfilledRanges = tableUtils
      .unfilledRanges(joinConf.metaData.comparisonTable,
                      PartitionRange(null, endDate),
                      Some(Seq(joinConf.metaData.loggedTable)))
      .getOrElse(Seq.empty)
    if (unfilledRanges.isEmpty) return
    val join = new chronon.spark.Join(buildComparisonJoin(), unfilledRanges.last.end, TableUtils(session))
    logger.info("Starting compute Join for comparison table")
    val compareDf = join.computeJoin(Some(30))
    logger.info("======= side-by-side comparison schema =======")
    logger.info(compareDf.schema.pretty)
  }

  def buildConsistencyMetrics(): DataMetrics = {
    // migrate legacy configs without consistencySamplePercent param
    if (!joinConf.metaData.isSetConsistencySamplePercent) {
      logger.info("consistencySamplePercent is unset and will default to 100")
      joinConf.metaData.consistencySamplePercent = 100
    }

    if (joinConf.metaData.consistencySamplePercent == 0) {
      logger.info(s"Exit ConsistencyJob because consistencySamplePercent = 0 for join conf ${joinConf.metaData.name}")
      return DataMetrics(Seq())
    }

    buildComparisonTable()
    logger.info("Determining Range between consistency table and comparison table")
    val unfilledRanges = tableUtils
      .unfilledRanges(joinConf.metaData.consistencyTable,
                      PartitionRange(null, endDate),
                      Some(Seq(joinConf.metaData.comparisonTable)))
      .getOrElse(Seq.empty)
    if (unfilledRanges.isEmpty) return null
    val allMetrics = unfilledRanges.map { unfilled =>
      val comparisonDf = unfilled.scanQueryDf(null, joinConf.metaData.comparisonTable)
      val loggedDf = unfilled.scanQueryDf(null, joinConf.metaData.loggedTable).drop(Constants.SchemaHash)
      // there could be external columns that are logged during online env, therefore they could not be used for computing OOC
      val loggedDfNoExternalCols = loggedDf.select(comparisonDf.columns.map(org.apache.spark.sql.functions.col): _*)
      logger.info("Starting compare job for stats")
      val joinKeys = if (joinConf.isSetRowIds) {
        joinConf.rowIds.toScala
      } else {
        timeFields.map(_.name).toList ++ joinConf.leftKeyCols
      }
      logger.info(s"Using ${joinKeys.mkString("[", ",", "]")} as join keys between log and backfill.")
      val (compareDf, metricsKvRdd, metrics) =
        CompareBaseJob.compare(comparisonDf,
                               loggedDfNoExternalCols,
                               keys = joinKeys,
                               tableUtils,
                               name = joinConf.metaData.nameToFilePath)
      logger.info("Saving output.")
      val outputDf = metricsKvRdd.toFlatDf.withTimeBasedColumn("ds")
      logger.info(s"output schema ${outputDf.schema.fields.map(sb => (sb.name, sb.dataType)).toMap.mkString("\n - ")}")
      tableUtils.insertPartitions(outputDf,
                                  joinConf.metaData.consistencyTable,
                                  tableProperties = tblProperties,
                                  autoExpand = true)
      metricsKvRdd.toAvroDf
        .withTimeBasedColumn(tableUtils.partitionColumn)
        .save(joinConf.metaData.consistencyUploadTable, tblProperties)
      metrics
    }
    DataMetrics(allMetrics.flatMap(_.series))
  }
}
