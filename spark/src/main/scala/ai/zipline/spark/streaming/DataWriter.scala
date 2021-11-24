package ai.zipline.spark.streaming

import ai.zipline.online.KVStore.PutRequest
import ai.zipline.online.Metrics.Context
import ai.zipline.online.{Api, AvroCodec, GroupByServingInfoParsed, KVStore}
import com.google.gson.Gson
import org.apache.spark.sql.ForeachWriter

import java.util.Base64

class DataWriter(onlineImpl: Api, context: Context, debug: Boolean = false) extends ForeachWriter[PutRequest] {

  var kvStore: KVStore = _

  override def open(partitionId: Long, epochId: Long): Boolean = {
    kvStore = onlineImpl.genKvStore
    true
  }

  override def process(putRequest: PutRequest): Unit = {
    if (!debug) {
      kvStore.put(putRequest)
      putRequest.tsMillis.foreach { ts: Long =>
        Metrics.Egress.reportLatency(System.currentTimeMillis() - ts, metricsContext = context)
      }
    }
  }

  override def close(errorOrNull: Throwable): Unit = {}
}
