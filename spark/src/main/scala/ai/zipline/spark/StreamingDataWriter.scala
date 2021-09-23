package ai.zipline.spark

import ai.zipline.api.KVStore.PutRequest
import ai.zipline.api.{KVStore, OnlineImpl, StructType}
import ai.zipline.fetcher.{AvroCodec, GroupByServingInfoParsed, RowConversions}
import ai.zipline.lib.StreamingMetrics
import org.apache.spark.sql.ForeachWriter
import java.util.Base64

import ai.zipline.lib.Metrics.Context



class StreamingDataWriter(onlineImpl: OnlineImpl, groupByServingInfoParsed: GroupByServingInfoParsed, context: Context, debug: Boolean = false) extends ForeachWriter[PutRequest] {

  var kvStore : KVStore = _

  override def open(partitionId: Long, epochId: Long): Boolean = {
    kvStore = onlineImpl.genKvStore
    true
  }

  def decodeAvroRecord(bytes: Array[Byte], codec: AvroCodec, ts: Long): Array[Any] = {
    codec.decodeRow(bytes, ts).values
  }

  def print(input: PutRequest): Unit = {
    if (input.tsMillis.isEmpty) {
      println("no ts defined")
      return
    }
    val keyB64 = Base64.getEncoder.encodeToString(input.keyBytes)
    val valueB64 = Base64.getEncoder.encodeToString(input.valueBytes)
    val keys = decodeAvroRecord(input.keyBytes, groupByServingInfoParsed.keyCodec, input.tsMillis.get)
    val values = decodeAvroRecord(input.valueBytes, groupByServingInfoParsed.selectedCodec, input.tsMillis.get)
    println(s"Writing key:[$keyB64],  value:[$valueB64], ts:[${input.tsMillis.get}] keys:[${keys.mkString(", ")}] values:[${values.mkString(", ")}]")
  }

  override def process(putRequest: PutRequest): Unit = {
    if (debug) print(putRequest)
    kvStore.put(putRequest)
    putRequest.tsMillis.map {
      case ts: Long => StreamingMetrics.Egress.reportLatency(System.currentTimeMillis() - ts,
        metricsContext = context)
    }
  }

  override def close(errorOrNull: Throwable): Unit = {
  }
}
