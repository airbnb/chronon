package ai.chronon.spark.test

import ai.chronon.api
import ai.chronon.api.{Accuracy, Constants, DataModel, StructType}
import ai.chronon.online.{KVStore, SparkConversions}
import ai.chronon.spark.{GroupByUpload, SparkSessionBuilder, TableUtils}
import ai.chronon.spark.streaming.{GroupBy, GroupByRunner}
import ai.chronon.spark.stats.SummaryJob
import org.apache.spark.sql.streaming.Trigger
import ai.chronon.api.Extensions.{GroupByOps, MetadataOps, SourceOps}
import org.apache.spark.sql.SparkSession

object OnlineUtils {

  // TODO: deprecate
  def putStreaming(session: SparkSession,
                   groupByConf: api.GroupBy,
                   kvStore: () => KVStore,
                   tableUtils: TableUtils,
                   ds: String,
                   namespace: String): Unit = {
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
    mockApi.streamSchema = StructType.from("Stream", SparkConversions.toChrononSchema(inputStreamDf.schema))
    val groupByStreaming =
      new GroupBy(inputStream.getInMemoryStreamDF(session, inputStreamDf), session, groupByConf, mockApi)
    // We modify the arguments for running to make sure all data gets into the KV Store before fetching.
    val dataStream = groupByStreaming.buildDataStream()
    val query = dataStream.trigger(Trigger.Once()).start()
    query.awaitTermination()
  }

  private def mutateTopicWithDs(source: api.Source, ds: String): Unit = {
    if (source.isSetEntities) {
      source.getEntities.setMutationTopic(s"${source.getEntities.mutationTable}/ds=$ds")
    } else if (source.isSetEvents) {
      source.getEntities.setMutationTopic(s"${source.getEvents.table}/ds=$ds")
    } else {
      val joinLeft = source.getJoinSource.getJoin.left
      mutateTopicWithDs(joinLeft, ds)
    }
  }

  // TODO - deprecate putStreaming
  def putStreamingNew(session: SparkSession, originalGroupByConf: api.GroupBy, ds: String, namespace: String): Unit = {
    val kvStoreFunc = () => OnlineUtils.buildInMemoryKVStore("FetcherTest")
    val mockApi = new MockApi(kvStoreFunc, namespace)
    val groupByConf = originalGroupByConf.deepCopy()
    val source = groupByConf.streamingSource.get
    mutateTopicWithDs(source, ds)
    val groupByStreaming =
      new GroupByRunner(groupByConf, session, Map.empty, mockApi, debug = true)
    val query = groupByStreaming.startWriting
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
