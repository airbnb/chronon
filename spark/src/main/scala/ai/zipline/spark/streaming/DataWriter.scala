package ai.zipline.spark.streaming

import ai.zipline.online.KVStore.PutRequest
import ai.zipline.online.Metrics.Context
import ai.zipline.online.{Api, KVStore}
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
        context.histogram("freshness", System.currentTimeMillis() - ts)
        context.increment("rows")
        context.increment("value_bytes")
        context.histogram("value_bytes", putRequest.valueBytes.length)
        context.histogram("key_bytes", putRequest.keyBytes.length)
      }
    }
  }

  override def close(errorOrNull: Throwable): Unit = {}
}
