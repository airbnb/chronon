package ai.chronon.spark.test

import ai.chronon.api
import ai.chronon.api.{Accuracy, Constants, DataModel, StructType}
import ai.chronon.online.{KVStore, SparkConversions, TileCodec}
import ai.chronon.spark.{GroupByUpload, SparkSessionBuilder, TableUtils}
import ai.chronon.spark.streaming.GroupBy
import ai.chronon.spark.stats.SummaryJob
import org.apache.spark.sql.streaming.Trigger
import ai.chronon.api.Extensions.{GroupByOps, JoinOps, MetadataOps, SourceOps}
import org.apache.spark.sql.SparkSession

object OnlineUtils {
  def putStreaming(session: SparkSession,
                   groupByConf: api.GroupBy,
                   kvStore: () => KVStore,
                   tableUtils: TableUtils,
                   ds: String,
                   namespace: String): Unit = {
    val inputStreamDf = groupByConf.dataModel match {
      case DataModel.Entities =>
        if (TileCodec.isTilingEnabled(groupByConf)) {
          throw new RuntimeException("Tiling is not supported for Entity type data models. Tiling can only be used if the streaming data is Events only.")
        }

        val entity = groupByConf.streamingSource.get
        val df = tableUtils.sql(s"SELECT * FROM ${entity.getEntities.mutationTable} WHERE ds = '$ds'")
        df.withColumnRenamed(entity.query.reversalColumn, Constants.ReversalColumn)
          .withColumnRenamed(entity.query.mutationTimeColumn, Constants.MutationTimeColumn)
      case DataModel.Events =>
        val table = groupByConf.streamingSource.get.table
        tableUtils.sql(s"SELECT * FROM $table WHERE ds >= '$ds'")
    }
    val inputStream = new InMemoryStream
    val mockApi = new MockApi(kvStore, namespace)
    mockApi.streamSchema = StructType.from("Stream", SparkConversions.toChrononSchema(inputStreamDf.schema))

    val memoryStreamDF = if (TileCodec.isTilingEnabled(groupByConf)) {
      inputStream.getInMemoryTiledStreamDF(session, inputStreamDf, groupByConf)
    } else {
      inputStream.getInMemoryStreamDF(session, inputStreamDf)
    }

    val groupByStreaming = new GroupBy(memoryStreamDF, session, groupByConf, mockApi)
    // We modify the arguments for running to make sure all data gets into the KV Store before fetching.
    val dataStream = groupByStreaming.buildDataStream()
    val query = dataStream.trigger(Trigger.Once()).start()
    query.awaitTermination()
  }

  def serve(tableUtils: TableUtils,
            inMemoryKvStore: InMemoryKvStore,
            kvStoreGen: () => InMemoryKvStore,
            namespace: String,
            endDs: String,
            groupByConf: api.GroupBy): Unit = {
    val prevDs = tableUtils.partitionSpec.before(endDs)
    GroupByUpload.run(groupByConf, prevDs, Some(tableUtils))
    inMemoryKvStore.bulkPut(groupByConf.metaData.uploadTable, groupByConf.batchDataset, null)
    if (groupByConf.inferredAccuracy == Accuracy.TEMPORAL && groupByConf.streamingSource.isDefined) {
      inMemoryKvStore.create(groupByConf.streamingDataset)
      OnlineUtils.putStreaming(tableUtils.sparkSession, groupByConf, kvStoreGen, tableUtils, endDs, namespace)
    }
  }

  def serveStats(tableUtils: TableUtils, inMemoryKvStore: InMemoryKvStore, endDs: String, joinConf: api.Join): Unit = {
    val statsJob = new SummaryJob(tableUtils.sparkSession, joinConf, endDs)
    statsJob.dailyRun()
    inMemoryKvStore.bulkPut(joinConf.metaData.dailyStatsUploadTable, Constants.StatsBatchDataset, null)
  }

  def buildInMemoryKVStore(sessionName: String): InMemoryKvStore = {
    InMemoryKvStore.build(sessionName, { () => TableUtils(SparkSessionBuilder.build(sessionName, local = true)) })
  }
}
