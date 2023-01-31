package ai.chronon.spark.test

import ai.chronon.api
import ai.chronon.api.{Accuracy, Constants, DataModel, StructType}
import ai.chronon.online.KVStore
import ai.chronon.spark.{Conversions, GroupByUpload, SparkSessionBuilder, TableUtils}
import ai.chronon.spark.streaming.GroupBy
import ai.chronon.spark.stats.SummaryJob
import org.apache.spark.sql.streaming.Trigger
import ai.chronon.api.Extensions.{GroupByOps, MetadataOps, SourceOps, JoinOps}
import org.apache.spark.sql.SparkSession

object OnlineUtils {
  def putStreaming(session: SparkSession, groupByConf: api.GroupBy, kvStore: () => KVStore, tableUtils: TableUtils, ds: String, namespace: String): Unit = {
    val inputStreamDf = groupByConf.dataModel match {
      case DataModel.Entities =>
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
    mockApi.streamSchema = StructType.from("Stream", Conversions.toChrononSchema(inputStreamDf.schema))
    val groupByStreaming =
      new GroupBy(inputStream.getInMemoryStreamDF(session, inputStreamDf), session, groupByConf, mockApi)
    // We modify the arguments for running to make sure all data gets into the KV Store before fetching.
    val dataStream = groupByStreaming.buildDataStream()
    val query = dataStream.trigger(Trigger.Once()).start()
    query.awaitTermination()
  }

  def serve(tableUtils: TableUtils, inMemoryKvStore: InMemoryKvStore, kvStoreGen: () => InMemoryKvStore, namespace: String, endDs: String, groupByConf: api.GroupBy): Unit = {
    val prevDs = Constants.Partition.before(endDs)
    GroupByUpload.run(groupByConf, prevDs, Some(tableUtils))
    inMemoryKvStore.bulkPut(groupByConf.metaData.uploadTable, groupByConf.batchDataset, null)
    if (groupByConf.inferredAccuracy == Accuracy.TEMPORAL && groupByConf.streamingSource.isDefined) {
      inMemoryKvStore.create(groupByConf.streamingDataset)
      OnlineUtils.putStreaming(tableUtils.sparkSession, groupByConf, kvStoreGen, tableUtils, endDs, namespace)
    }
  }

  def serveStats(tableUtils: TableUtils, inMemoryKvStore: InMemoryKvStore, kvStoreGen: () => InMemoryKvStore, namespace: String, endDs: String, joinConf: api.Join): Unit = {
    val statsJob = new SummaryJob(tableUtils.sparkSession, joinConf, endDs)
    statsJob.dailyRun()
    inMemoryKvStore.bulkPut(joinConf.metaData.dailyStatsUploadTable, joinConf.statsBatchDataset, null)
  }

  def buildInMemoryKVStore(sessionName: String): InMemoryKvStore = {
    InMemoryKvStore.build(sessionName, { () => TableUtils(SparkSessionBuilder.build(sessionName, local = true)) })
  }
}
