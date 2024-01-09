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

package ai.chronon.spark.test

import ai.chronon.api
import ai.chronon.api.{Accuracy, Constants, DataModel}
import ai.chronon.spark.{GenericRowHandler, GroupByUpload, SparkSessionBuilder, TableUtils}
import ai.chronon.spark.streaming.{GroupBy, JoinSourceRunner}
import ai.chronon.spark.stats.SummaryJob
import org.apache.spark.sql.streaming.Trigger
import ai.chronon.api.Extensions.{GroupByOps, MetadataOps, SourceOps}
import ai.chronon.online.{AvroConversions, KVStore, TileCodec}
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
    val isTiled = groupByConf.isTilingEnabled
    val inputStreamDf = groupByConf.dataModel match {
      case DataModel.Entities =>
        assert(!isTiled, "Tiling is not supported for Entity groupBy's yet (Only Event groupBy are supported)")

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

    if (isTiled) {
      val memoryStream: Array[(Array[Any], Long, Array[Byte])] =
        inputStream.getInMemoryTiledStreamArray(session, inputModified, groupByConf)
      val inMemoryKvStore: KVStore = kvStore()

      val fetcher = mockApi.buildFetcher(false)
      val groupByServingInfo = fetcher.getGroupByServingInfo(groupByConf.getMetaData.getName).get

      val keyZSchema: api.StructType = groupByServingInfo.keyChrononSchema
      val keyToBytes = AvroConversions.encodeBytes(keyZSchema, GenericRowHandler.func)

      val putRequests = memoryStream.map { entry =>
        val keys = entry._1
        val timestamp = entry._2
        val tileBytes = entry._3

        val keyBytes = keyToBytes(keys)

        KVStore.PutRequest(keyBytes, tileBytes, groupByConf.streamingDataset, Some(timestamp))
      }

      inMemoryKvStore.multiPut(putRequests)
    } else {
      val groupByStreaming =
        new GroupBy(inputStream.getInMemoryStreamDF(session, inputModified),
                    session,
                    groupByConf,
                    mockApi,
                    debug = debug)
      // We modify the arguments for running to make sure all data gets into the KV Store before fetching.
      val dataStream = groupByStreaming.buildDataStream()
      val query = dataStream.trigger(Trigger.Once()).start()
      query.awaitTermination()
    }
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
    val groupByStreaming = new JoinSourceRunner(groupByConf, Map.empty, debug = debug, lagMillis = 0)
    val query = groupByStreaming.chainedStreamingQuery.trigger(Trigger.Once()).start()
    // drain work scheduled as futures over the executioncontext
    Thread.sleep(5000)
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
    inMemoryKvStore.bulkPut(joinConf.metaData.toUploadTable(joinConf.metaData.dailyStatsOutputTable),
                            Constants.StatsBatchDataset,
                            null)
  }

  def serveLogStats(tableUtils: TableUtils,
                    inMemoryKvStore: InMemoryKvStore,
                    endDs: String,
                    joinConf: api.Join): Unit = {
    val statsJob = new SummaryJob(tableUtils.sparkSession, joinConf, endDs)
    statsJob.loggingRun()
    inMemoryKvStore.bulkPut(joinConf.metaData.toUploadTable(joinConf.metaData.loggingStatsTable),
                            Constants.LogStatsBatchDataset,
                            null)
  }

  def serveConsistency(tableUtils: TableUtils,
                       inMemoryKvStore: InMemoryKvStore,
                       endDs: String,
                       joinConf: api.Join): Unit = {
    inMemoryKvStore.bulkPut(joinConf.metaData.consistencyUploadTable, Constants.ConsistencyMetricsDataset, null)
  }

  def buildInMemoryKVStore(sessionName: String): InMemoryKvStore = {
    InMemoryKvStore.build(sessionName, { () => TableUtils(SparkSessionBuilder.build(sessionName, local = true)) })
  }
}
