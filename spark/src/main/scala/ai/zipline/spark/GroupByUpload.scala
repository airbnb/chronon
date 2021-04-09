package ai.zipline.spark

import ai.zipline.aggregator.windowing._
import ai.zipline.api.Extensions.{GroupByOps, MetadataOps}
import ai.zipline.api.{
  Accuracy,
  Aggregation,
  Constants,
  DataModel,
  GroupByServingInfo,
  ThriftJsonCodec,
  GroupBy => GroupByConf
}
import ai.zipline.spark.Extensions._
import ai.zipline.spark.GroupBy.{ParsedArgs, computeBackfill}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.catalyst.expressions.GenericRow

import scala.collection.JavaConverters._

class GroupByUpload(endPartition: String, groupBy: GroupBy) extends Serializable {
  implicit val sparkSession = groupBy.sparkSession

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

  def temporalEvents(resolution: Resolution = FiveMinuteResolution): KvRdd = {
    val endTs = Constants.Partition.epochMillis(endPartition)
    val sawtoothOnlineAggregator = new SawtoothOnlineAggregator(endTs,
                                                                groupBy.aggregations,
                                                                Conversions.toZiplineSchema(groupBy.inputDf.schema),
                                                                resolution)
    val irSchema = Conversions.fromZiplineSchema(sawtoothOnlineAggregator.batchIrSchema)
    val keyBuilder = FastHashing.generateKeyBuilder(groupBy.keyColumns.toArray, groupBy.inputDf.schema)

    val outputRdd = groupBy.inputDf.rdd
      .keyBy(keyBuilder)
      .mapValues(Conversions.toZiplineRow(_, groupBy.tsIndex))
      .aggregateByKey(sawtoothOnlineAggregator.init)(
        seqOp = sawtoothOnlineAggregator.update,
        combOp = sawtoothOnlineAggregator.merge
      )
      .mapValues(sawtoothOnlineAggregator.finalizeTail)
      .map {
        case (keyWithHash: KeyWithHash, finalBatchIr: FinalBatchIr) =>
          val collapsed = new GenericRow(finalBatchIr.collapsed)
          val tail = Option(finalBatchIr.tailHops).map(_.map(_.map(new GenericRow(_)))).orNull
          val irArray = new Array[Any](2)
          irArray.update(0, collapsed)
          irArray.update(1, tail)
          //          val gson = new Gson()
          //          println(s"""
          //               |keys: ${gson.toJson(keyWithHash.data)}
          //               |collapsed: ${gson.toJson(finalBatchIr.collapsed)}
          //               |tailHops: ${gson.toJson(finalBatchIr.tailHops)}
          //               |""".stripMargin)
          keyWithHash.data -> irArray

      }
    KvRdd(outputRdd, groupBy.keySchema, irSchema)
  }

  // TODO
  def temporalEntities: KvRdd = ???
}

object GroupByUpload {
  def run(groupByConf: GroupByConf, endDs: String): Unit = {
    val tableUtils = TableUtils(SparkSessionBuilder.build(s"groupBy_${groupByConf.metaData.name}_upload"))
    val groupBy = GroupBy.from(groupByConf, PartitionRange(endDs, endDs), tableUtils)
    val groupByUpload = new GroupByUpload(endDs, groupBy)

    val kvDf = ((groupByConf.inferredAccuracy, groupByConf.dataModel) match {
      case (Accuracy.SNAPSHOT, DataModel.Events)   => groupByUpload.snapshotEvents
      case (Accuracy.SNAPSHOT, DataModel.Entities) => groupByUpload.snapshotEntities
      case (Accuracy.TEMPORAL, DataModel.Events)   => groupByUpload.temporalEvents()
      case (Accuracy.TEMPORAL, DataModel.Entities) =>
        throw new UnsupportedOperationException("Mutations are not yet supported")
    }).toAvroDf

    val groupByServingInfo = new GroupByServingInfo()
    groupByServingInfo.setBatchDateStamp(endDs)
    groupByServingInfo.setGroupBy(groupByConf)
    groupByServingInfo.setKeyAvroSchema(groupBy.keySchema.toAvroSchema().toString(true))
    groupByServingInfo.setKeyAvroSchema(groupBy.preAggSchema.toAvroSchema().toString(true))
    val metaRows = Seq(
      Row(Constants.GroupByServingInfoKey.getBytes(Constants.UTF8),
          ThriftJsonCodec.toJsonStr(groupByServingInfo).getBytes(Constants.UTF8)))
    val metaRdd = tableUtils.sparkSession.sparkContext.parallelize(metaRows)
    val metaDf = tableUtils.sparkSession.createDataFrame(metaRdd, kvDf.schema)
    val tableName = s"${groupByConf.metaData.outputNamespace}.${groupByConf.metaData.cleanName}_upload"
    val tableProps = groupByConf.metaData.tableProperties.asScala.toMap
    kvDf.union(metaDf).save(tableName, tableProps)
  }

  def main(args: Array[String]): Unit = {
    val parsedArgs = new ParsedArgs(args)
    println(s"Parsed Args: $parsedArgs")
    run(parsedArgs.groupByConf, parsedArgs.endDate())
  }
}
