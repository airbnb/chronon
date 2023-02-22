package ai.chronon.spark

import ai.chronon.aggregator.windowing.{FinalBatchIr, FiveMinuteResolution, Resolution, SawtoothOnlineAggregator}
import ai.chronon.api
import ai.chronon.api.{Accuracy, Constants, DataModel, GroupByServingInfo, ThriftJsonCodec}
import ai.chronon.api.Extensions.{GroupByOps, MetadataOps, SourceOps}
import ai.chronon.online.{Metrics, SparkConversions}
import ai.chronon.spark.Extensions._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Row, SparkSession}

class GroupByUpload(endPartition: String, groupBy: GroupBy) extends Serializable {
  implicit val sparkSession: SparkSession = groupBy.sparkSession

  private def fromBase(rdd: RDD[(Array[Any], Array[Any])]): KvRdd = {
    KvRdd(rdd.map { case (keyAndDs, values) => keyAndDs.init -> values }, groupBy.keySchema, groupBy.postAggSchema)
  }
  def snapshotEntities: KvRdd = {
    if (groupBy.aggregations == null || groupBy.aggregations.isEmpty) {
      // pre-agg to PairRdd
      val keysAndPartition = (groupBy.keyColumns :+ Constants.PartitionColumn).toArray
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
    val endTs = Constants.Partition.epochMillis(endPartition)
    println(s"TemporalEvents upload end ts: $endTs")
    val sawtoothOnlineAggregator = new SawtoothOnlineAggregator(
      endTs,
      groupBy.aggregations,
      SparkConversions.toChrononSchema(groupBy.inputDf.schema),
      resolution)
    val irSchema = SparkConversions.fromChrononSchema(sawtoothOnlineAggregator.batchIrSchema)
    val keyBuilder = FastHashing.generateKeyBuilder(groupBy.keyColumns.toArray, groupBy.inputDf.schema)

    val outputRdd = groupBy.inputDf.rdd
      .keyBy(keyBuilder)
      .mapValues(SparkConversions.toChrononRow(_, groupBy.tsIndex))
      .aggregateByKey(sawtoothOnlineAggregator.init)(
        seqOp = sawtoothOnlineAggregator.update,
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
  def run(groupByConf: api.GroupBy, endDs: String, tableUtilsOpt: Option[TableUtils] = None): Unit = {
    val context = Metrics.Context(Metrics.Environment.GroupByUpload, groupByConf)
    val startTs = System.currentTimeMillis()
    val tableUtils =
      tableUtilsOpt.getOrElse(
        TableUtils(
          SparkSessionBuilder
            .build(s"groupBy_${groupByConf.metaData.name}_upload")))
    groupByConf.setups.foreach(tableUtils.sql)
    // add 1 day to the batch end time to reflect data [ds 00:00:00.000, ds + 1 00:00:00.000)
    val batchEndDate = Constants.Partition.after(endDs)
    // for snapshot accuracy
    lazy val groupBy = GroupBy.from(groupByConf, PartitionRange(endDs, endDs), tableUtils)
    lazy val groupByUpload = new GroupByUpload(endDs, groupBy)
    // for temporal accuracy
    lazy val shiftedGroupBy = GroupBy.from(groupByConf, PartitionRange(endDs, endDs).shift(1), tableUtils)
    lazy val shiftedGroupByUpload = new GroupByUpload(batchEndDate, shiftedGroupBy)
    // for mutations I need the snapshot from the previous day, but a batch end date of ds +1
    lazy val otherGroupByUpload = new GroupByUpload(batchEndDate, groupBy)

    println(s"""
         |GroupBy upload for: ${groupByConf.metaData.team}.${groupByConf.metaData.name}
         |Accuracy: ${groupByConf.inferredAccuracy}
         |Data Model: ${groupByConf.dataModel}
         |""".stripMargin)

    val kvDf = ((groupByConf.inferredAccuracy, groupByConf.dataModel) match {
      case (Accuracy.SNAPSHOT, DataModel.Events)   => groupByUpload.snapshotEvents
      case (Accuracy.SNAPSHOT, DataModel.Entities) => groupByUpload.snapshotEntities
      case (Accuracy.TEMPORAL, DataModel.Events)   => shiftedGroupByUpload.temporalEvents()
      case (Accuracy.TEMPORAL, DataModel.Entities) => otherGroupByUpload.temporalEvents()
    }).toAvroDf

    val groupByServingInfo = new GroupByServingInfo()
    groupByServingInfo.setBatchEndDate(batchEndDate)
    groupByServingInfo.setGroupBy(groupByConf)
    groupByServingInfo.setKeyAvroSchema(groupBy.keySchema.toAvroSchema("Key").toString(true))
    groupByServingInfo.setSelectedAvroSchema(groupBy.preAggSchema.toAvroSchema("Value").toString(true))
    if (groupByConf.streamingSource.isDefined) {
      val streamingSource = groupByConf.streamingSource.get
      val fullInputSchema = tableUtils.getSchemaFromTable(streamingSource.table)
      val streamingQuery = groupByConf.buildStreamingQuery
      val inputSchema =
        if (Option(streamingSource.query.selects).isEmpty) fullInputSchema
        else {
          val reqColumns = tableUtils.getColumnsFromQuery(streamingQuery)
          StructType(fullInputSchema.filter(col => reqColumns.contains(col.name)))
        }
      groupByServingInfo.setInputAvroSchema(inputSchema.toAvroSchema(name = "Input").toString(true))
    } else {
      println("Not setting InputAvroSchema to GroupByServingInfo as there is no streaming source defined.")
    }

    val metaRows = Seq(
      Row(
        Constants.GroupByServingInfoKey.getBytes(Constants.UTF8),
        ThriftJsonCodec.toJsonStr(groupByServingInfo).getBytes(Constants.UTF8),
        Constants.GroupByServingInfoKey,
        ThriftJsonCodec.toJsonStr(groupByServingInfo)
      ))
    val metaRdd = tableUtils.sparkSession.sparkContext.parallelize(metaRows)
    val metaDf = tableUtils.sparkSession.createDataFrame(metaRdd, kvDf.schema)
    kvDf
      .union(metaDf)
      .withColumn("ds", lit(endDs))
      .saveUnPartitioned(groupByConf.metaData.uploadTable, groupByConf.metaData.tableProps)

    val metricRow =
      kvDf.selectExpr("sum(bit_length(key_bytes))/8", "sum(bit_length(value_bytes))/8", "count(*)").collect()
    context.gauge(Metrics.Name.KeyBytes, metricRow(0).getDouble(0).toLong)
    context.gauge(Metrics.Name.ValueBytes, metricRow(0).getDouble(1).toLong)
    context.gauge(Metrics.Name.RowCount, metricRow(0).getLong(2))
    context.gauge(Metrics.Name.LatencyMinutes, (System.currentTimeMillis() - startTs) / (60 * 1000))
  }

  def main(args: Array[String]): Unit = {
    val parsedArgs = new Args(args)
    parsedArgs.verify()
    assert(parsedArgs.stepDays.isEmpty, "Don't need to specify step days for GroupBy uploads")
    println(s"Parsed Args: $parsedArgs")
    run(parsedArgs.parseConf[api.GroupBy], parsedArgs.endDate())
  }
}
