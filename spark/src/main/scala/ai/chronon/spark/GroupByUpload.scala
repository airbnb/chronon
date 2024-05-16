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

package ai.chronon.spark

import org.slf4j.LoggerFactory
import ai.chronon.aggregator.windowing.{FinalBatchIr, FiveMinuteResolution, Resolution, SawtoothOnlineAggregator}
import ai.chronon.api
import ai.chronon.api.{Accuracy, Constants, DataModel, GroupByServingInfo, QueryUtils, ThriftJsonCodec}
import ai.chronon.api.Extensions.{GroupByOps, MetadataOps, SourceOps}
import ai.chronon.online.Extensions.ChrononStructTypeOps
import ai.chronon.online.{GroupByServingInfoParsed, Metrics, SparkConversions}
import ai.chronon.spark.Extensions._
import org.apache.spark.SparkEnv
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.{col, lit, not}
import org.apache.spark.sql.{Row, SparkSession, types}

import scala.annotation.tailrec
import scala.collection.Seq
import scala.util.ScalaJavaConversions.{ListOps, MapOps}
import scala.util.Try

class GroupByUpload(endPartition: String, groupBy: GroupBy) extends Serializable {
  @transient lazy val logger = LoggerFactory.getLogger(getClass)
  implicit val sparkSession: SparkSession = groupBy.sparkSession
  implicit private val tableUtils: TableUtils = TableUtils(sparkSession)
  private def fromBase(rdd: RDD[(Array[Any], Array[Any])]): KvRdd = {
    KvRdd(rdd.map { case (keyAndDs, values) => keyAndDs.init -> values }, groupBy.keySchema, groupBy.postAggSchema)
  }
  def snapshotEntities: KvRdd = {
    if (groupBy.aggregations == null || groupBy.aggregations.isEmpty) {
      // pre-agg to PairRdd
      val keysAndPartition = (groupBy.keyColumns :+ tableUtils.partitionColumn).toArray
      val keyBuilder = FastHashing.generateKeyBuilder(keysAndPartition, groupBy.inputDf.schema)
      val values = groupBy.inputDf.schema.map(_.name).filterNot(keysAndPartition.contains)
      val valuesIndices = values.map(groupBy.inputDf.schema.fieldIndex).toArray
      val rdd = groupBy.inputDf.rdd
        .map { row =>
          keyBuilder(row).data.init -> valuesIndices.map(row.get)
        }
      KvRdd(rdd, groupBy.keySchema, groupBy.preAggSchema)
    } else {
      fromBase(groupBy.snapshotEntitiesBase)
    }
  }

  def snapshotEvents: KvRdd =
    fromBase(groupBy.snapshotEventsBase(PartitionRange(endPartition, endPartition)))

  // Shared between events and mutations (temporal entities).
  def temporalEvents(resolution: Resolution = FiveMinuteResolution): KvRdd = {
    val endTs = tableUtils.partitionSpec.epochMillis(endPartition)
    logger.info(s"TemporalEvents upload end ts: $endTs")
    val sawtoothOnlineAggregator = new SawtoothOnlineAggregator(
      endTs,
      groupBy.aggregations,
      SparkConversions.toChrononSchema(groupBy.inputDf.schema),
      resolution)
    val irSchema = SparkConversions.fromChrononSchema(sawtoothOnlineAggregator.batchIrSchema)
    val keyBuilder = FastHashing.generateKeyBuilder(groupBy.keyColumns.toArray, groupBy.inputDf.schema)

    logger.info(s"""
        |BatchIR Element Size: ${SparkEnv.get.serializer
      .newInstance()
      .serialize(sawtoothOnlineAggregator.init)
      .capacity()}
        |""".stripMargin)

    val outputRdd = tableUtils
      .preAggRepartition(groupBy.inputDf)
      .rdd
      .keyBy(keyBuilder)
      .aggregateByKey(sawtoothOnlineAggregator.init)( // shuffle point
        seqOp = {
          case (batchIr, row) =>
            sawtoothOnlineAggregator.update(batchIr, SparkConversions.toChrononRow(row, groupBy.tsIndex))
        },
        combOp = sawtoothOnlineAggregator.merge
      )
      .mapValues(sawtoothOnlineAggregator.normalizeBatchIr)
      .map {
        case (keyWithHash: KeyWithHash, finalBatchIr: FinalBatchIr) =>
          val irArray = new Array[Any](2)
          irArray.update(0, finalBatchIr.collapsed)
          irArray.update(1, finalBatchIr.tailHops)
          keyWithHash.data -> irArray
      }
    KvRdd(outputRdd, groupBy.keySchema, irSchema)
  }

}

object GroupByUpload {
  @transient lazy val logger = LoggerFactory.getLogger(getClass)

  // TODO - remove this if spark streaming can't reach hive tables
  private def buildServingInfo(groupByConf: api.GroupBy,
                               session: SparkSession,
                               endDs: String): GroupByServingInfoParsed = {
    val groupByServingInfo = new GroupByServingInfo()
    implicit val tableUtils: TableUtils = TableUtils(session)
    val nextDay = tableUtils.partitionSpec.after(endDs)

    val groupBy = ai.chronon.spark.GroupBy
      .from(groupByConf, PartitionRange(endDs, endDs), TableUtils(session), computeDependency = false)
    groupByServingInfo.setBatchEndDate(nextDay)
    groupByServingInfo.setGroupBy(groupByConf)
    groupByServingInfo.setKeyAvroSchema(groupBy.keySchema.toAvroSchema("Key").toString(true))
    groupByServingInfo.setSelectedAvroSchema(groupBy.preAggSchema.toAvroSchema("Value").toString(true))
    if (groupByConf.streamingSource.isDefined) {
      val streamingSource = groupByConf.streamingSource.get

      // TODO: move this to SourceOps
      @tailrec
      def getInfo(source: api.Source): (String, api.Query, Boolean) = {
        if (source.isSetEvents) {
          (source.getEvents.getTable, source.getEvents.getQuery, false)
        } else if (source.isSetEntities) {
          (source.getEntities.getSnapshotTable, source.getEntities.getQuery, true)
        } else {
          val left = source.getJoinSource.getJoin.getLeft
          getInfo(left)
        }
      }

      val (rootTable, query, _) = getInfo(streamingSource)
      val fullInputSchema = tableUtils.getSchemaFromTable(rootTable)
      val inputSchema: types.StructType =
        if (Option(query.selects).isEmpty) fullInputSchema
        else {
          val selects = query.selects.toScala ++ Map(Constants.TimeColumn -> query.timeColumn)
          val streamingQuery =
            QueryUtils.build(selects, rootTable, query.wheres.toScala)
          val reqColumns = tableUtils.getColumnsFromQuery(streamingQuery)
          types.StructType(fullInputSchema.filter(col => reqColumns.contains(col.name)))
        }
      groupByServingInfo.setInputAvroSchema(inputSchema.toAvroSchema(name = "Input").toString(true))
    } else {
      logger.info("Not setting InputAvroSchema to GroupByServingInfo as there is no streaming source defined.")
    }

    val result = new GroupByServingInfoParsed(groupByServingInfo, tableUtils.partitionSpec)
    val firstSource = groupByConf.sources.get(0)
    logger.info(s"""
        |Built GroupByServingInfo for ${groupByConf.metaData.name}:
        |table: ${firstSource.table} / data-model: ${firstSource.dataModel}
        |     keySchema: ${Try(result.keyChrononSchema.catalogString)}
        |   valueSchema: ${Try(result.valueChrononSchema.catalogString)}
        |mutationSchema: ${Try(result.mutationChrononSchema.catalogString)}
        |   inputSchema: ${Try(result.inputChrononSchema.catalogString)}
        |selectedSchema: ${Try(result.selectedChrononSchema.catalogString)}
        |  streamSchema: ${Try(result.streamChrononSchema.catalogString)}
        |""".stripMargin)
    result
  }

  def run(groupByConf: api.GroupBy,
          endDs: String,
          tableUtilsOpt: Option[TableUtils] = None,
          showDf: Boolean = false,
          jsonPercent: Int = 1): Unit = {
    val context = Metrics.Context(Metrics.Environment.GroupByUpload, groupByConf)
    val startTs = System.currentTimeMillis()
    implicit val tableUtils: TableUtils =
      tableUtilsOpt.getOrElse(
        TableUtils(
          SparkSessionBuilder
            .build(s"groupBy_${groupByConf.metaData.name}_upload")))
    groupByConf.setups.foreach(tableUtils.sql)
    // add 1 day to the batch end time to reflect data [ds 00:00:00.000, ds + 1 00:00:00.000)
    val batchEndDate = tableUtils.partitionSpec.after(endDs)
    // for snapshot accuracy - we don't need to scan mutations
    lazy val groupBy =
      GroupBy.from(groupByConf, PartitionRange(endDs, endDs), tableUtils, computeDependency = true, showDf = showDf)
    lazy val groupByUpload = new GroupByUpload(endDs, groupBy)
    // for temporal accuracy - we don't need to scan mutations for upload
    // when endDs = xxxx-01-02 the timestamp from airflow is more than (xxxx-01-03 00:00:00)
    // we wait for event partitions of (xxxx-01-02) which contain data until (xxxx-01-02 23:59:59.999)
    lazy val shiftedGroupBy =
      GroupBy.from(groupByConf,
                   PartitionRange(endDs, endDs).shift(1),
                   tableUtils,
                   computeDependency = true,
                   showDf = showDf)
    lazy val shiftedGroupByUpload = new GroupByUpload(batchEndDate, shiftedGroupBy)
    // for mutations I need the snapshot from the previous day, but a batch end date of ds +1
    lazy val otherGroupByUpload = new GroupByUpload(batchEndDate, groupBy)

    logger.info(s"""
         |GroupBy upload for: ${groupByConf.metaData.team}.${groupByConf.metaData.name}
         |Accuracy: ${groupByConf.inferredAccuracy}
         |Data Model: ${groupByConf.dataModel}
         |""".stripMargin)

    val kvRdd = (groupByConf.inferredAccuracy, groupByConf.dataModel) match {
      case (Accuracy.SNAPSHOT, DataModel.Events)   => groupByUpload.snapshotEvents
      case (Accuracy.SNAPSHOT, DataModel.Entities) => groupByUpload.snapshotEntities
      case (Accuracy.TEMPORAL, DataModel.Events)   => shiftedGroupByUpload.temporalEvents()
      case (Accuracy.TEMPORAL, DataModel.Entities) => otherGroupByUpload.temporalEvents()
    }

    val kvDf = kvRdd.toAvroDf(jsonPercent = jsonPercent)
    if (showDf) {
      kvRdd.toFlatDf.prettyPrint()
    }

    val groupByServingInfo = buildServingInfo(groupByConf, session = tableUtils.sparkSession, endDs).groupByServingInfo

    val metaRows = Seq(
      Row(
        Constants.GroupByServingInfoKey.getBytes(Constants.UTF8),
        ThriftJsonCodec.toJsonStr(groupByServingInfo).getBytes(Constants.UTF8),
        Constants.GroupByServingInfoKey,
        ThriftJsonCodec.toJsonStr(groupByServingInfo)
      ))
    val metaRdd = tableUtils.sparkSession.sparkContext.parallelize(metaRows.toSeq)
    val metaDf = tableUtils.sparkSession.createDataFrame(metaRdd, kvDf.schema)
    kvDf
      .union(metaDf)
      .withColumn("ds", lit(endDs))
      .saveUnPartitioned(groupByConf.metaData.uploadTable, groupByConf.metaData.tableProps)

    val kvDfReloaded = tableUtils.sparkSession
      .table(groupByConf.metaData.uploadTable)
      .where(not(col("key_json").eqNullSafe(Constants.GroupByServingInfoKey)))

    val metricRow =
      kvDfReloaded.selectExpr("sum(bit_length(key_bytes))/8", "sum(bit_length(value_bytes))/8", "count(*)").collect()
    context.gauge(Metrics.Name.KeyBytes, metricRow(0).getDouble(0).toLong)
    context.gauge(Metrics.Name.ValueBytes, metricRow(0).getDouble(1).toLong)
    context.gauge(Metrics.Name.RowCount, metricRow(0).getLong(2))
    context.gauge(Metrics.Name.LatencyMinutes, (System.currentTimeMillis() - startTs) / (60 * 1000))
  }
}
