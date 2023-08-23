package ai.chronon.spark.test

import ai.chronon.api
import ai.chronon.api.{Accuracy, Constants, DataModel, JoinSource, StructType}
import ai.chronon.online.{KVStore, SparkConversions}
import ai.chronon.spark.{GroupByUpload, SparkSessionBuilder, TableUtils}
import ai.chronon.spark.streaming.{GroupBy, JoinSourceRunner}
import ai.chronon.spark.stats.SummaryJob
import org.apache.spark.sql.streaming.Trigger
import ai.chronon.api.Extensions.{GroupByOps, MetadataOps, SourceOps}
import org.apache.spark.sql.SparkSession

import scala.annotation.tailrec

object OnlineUtils {

  def putStreaming(session: SparkSession,
                   groupByConf: api.GroupBy,
                   kvStore: () => KVStore,
                   tableUtils: TableUtils,
                   ds: String,
                   namespace: String,
                   debug: Boolean,
                   dropDsOnWrite: Boolean): Unit = {
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
    var inputModified = inputStreamDf
    if (dropDsOnWrite && inputStreamDf.schema.fieldNames.contains(tableUtils.partitionColumn)) {
      inputModified = inputStreamDf.drop(tableUtils.partitionColumn)
    }
    // re-arrange so that mutation_ts and is_before come to the end - to match with streamSchema of GroupBy servingInfo
    val fields = inputModified.schema.fieldNames
    if (fields.contains(Constants.ReversalColumn)) {
      val trailingColumns = Seq(Constants.MutationTimeColumn, Constants.ReversalColumn)
      val finalOrder = (fields.filterNot(trailingColumns.contains) ++ trailingColumns).toSeq
      inputModified = inputModified.selectExpr(finalOrder: _*)
    }
    // mockApi.streamSchema = StructType.from("Stream", SparkConversions.toChrononSchema(inputModified.schema))
    val groupByStreaming =
      new GroupBy(inputStream.getInMemoryStreamDF(session, inputModified), session, groupByConf, mockApi, debug = debug)
    // We modify the arguments for running to make sure all data gets into the KV Store before fetching.
    val dataStream = groupByStreaming.buildDataStream()
    val query = dataStream.trigger(Trigger.Once()).start()
    query.awaitTermination()
  }

  @tailrec
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
  def putStreamingNew(originalGroupByConf: api.GroupBy,
                      ds: String,
                      namespace: String,
                      kvStoreFunc: () => KVStore,
                      debug: Boolean)(implicit session: SparkSession): Unit = {
    implicit val mockApi = new MockApi(kvStoreFunc, namespace)
    val groupByConf = originalGroupByConf.deepCopy()
    val source = groupByConf.streamingSource.get
    mutateTopicWithDs(source, ds)
    val groupByStreaming = new JoinSourceRunner(groupByConf, Map.empty, debug = debug)
    val query = groupByStreaming.chainedStreamingQuery.trigger(Trigger.Once()).start()
    // there is async stuff under the hood of chained streaming query
    query.awaitTermination()
  }

  def serve(tableUtils: TableUtils,
            inMemoryKvStore: InMemoryKvStore,
            kvStoreGen: () => InMemoryKvStore,
            namespace: String,
            endDs: String,
            groupByConf: api.GroupBy,
            debug: Boolean = false,
            // TODO: I don't fully understand why this is needed, but this is a quirk of the test harness
            // we need to fix the quirk and drop this flag
            dropDsOnWrite: Boolean = false): Unit = {
    val prevDs = tableUtils.partitionSpec.before(endDs)
    GroupByUpload.run(groupByConf, prevDs, Some(tableUtils))
    inMemoryKvStore.bulkPut(groupByConf.metaData.uploadTable, groupByConf.batchDataset, null)
    if (groupByConf.inferredAccuracy == Accuracy.TEMPORAL && groupByConf.streamingSource.isDefined) {
      val streamingSource = groupByConf.streamingSource.get
      inMemoryKvStore.create(groupByConf.streamingDataset)
      if (streamingSource.isSetJoinSource) {
        inMemoryKvStore.create(Constants.ChrononMetadataKey)
        new MockApi(kvStoreGen, namespace).buildFetcher().putJoinConf(streamingSource.getJoinSource.getJoin)
        OnlineUtils.putStreamingNew(groupByConf, endDs, namespace, kvStoreGen, debug)(tableUtils.sparkSession)
      } else {
        OnlineUtils.putStreaming(tableUtils.sparkSession,
                                 groupByConf,
                                 kvStoreGen,
                                 tableUtils,
                                 endDs,
                                 namespace,
                                 debug,
                                 dropDsOnWrite)
      }
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
