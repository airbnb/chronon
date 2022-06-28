package ai.chronon.spark.streaming

import ai.chronon.online.KVStore.PutRequest
import ai.chronon.online.Metrics.Context
import ai.chronon.online.{Api, KVStore, Metrics}
import org.apache.spark.sql.ForeachWriter

class DataWriter(onlineImpl: Api, context: Context, statsIntervalSecs: Int, debug: Boolean = false)
    extends ForeachWriter[PutRequest] {

  var kvStore: KVStore = _
  @transient private lazy val localStats = new ThreadLocal[StreamingStats]() {
    override def initialValue(): StreamingStats = new StreamingStats(statsIntervalSecs)
  }

  override def open(partitionId: Long, epochId: Long): Boolean = {
    kvStore = onlineImpl.genKvStore
    true
  }

  override def process(putRequest: PutRequest): Unit = {
    localStats.get().increment(putRequest)
    if (!debug) {
      kvStore.put(putRequest)
      putRequest.tsMillis.foreach { ts: Long =>
        context.histogram(Metrics.Name.FreshnessMillis, System.currentTimeMillis() - ts)
        context.increment(Metrics.Name.RowCount)
        context.histogram(Metrics.Name.ValueBytes, putRequest.valueBytes.length)
        context.histogram(Metrics.Name.KeyBytes, putRequest.keyBytes.length)
      }
    }
  }

  override def close(errorOrNull: Throwable): Unit = {}
}
