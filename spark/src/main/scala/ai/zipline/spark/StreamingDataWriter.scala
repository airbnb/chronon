package ai.zipline.spark

import ai.zipline.api.KVStore.PutRequest
import ai.zipline.lib.Metrics.Context
import ai.zipline.api.{KVStore, OnlineImpl}
import ai.zipline.fetcher.{AvroCodec, GroupByServingInfoParsed}
import ai.zipline.lib.StreamingMetrics
import org.apache.spark.sql.ForeachWriter
import java.util.Base64

import com.google.gson.Gson


class StreamingDataWriter(
    onlineImpl: OnlineImpl,
    groupByServingInfoParsed: GroupByServingInfoParsed,
    context: Context,
    debug: Boolean = false,
    mockWrites: Boolean = false) extends ForeachWriter[PutRequest] {

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
    val gson = new Gson()
    println(s"""
        | Writing keyBytes: $keyB64
        | valueBytes: $valueB64
        | ts:        ${input.tsMillis.get}
        | keys:    ${gson.toJson(keys)}
        | values: ${gson.toJson(values)}""".stripMargin)
  }

  override def process(putRequest: PutRequest): Unit = {
    if (debug) print(putRequest)
    if (mockWrites) {
      println("Skipping writing to Mussel in mock writes mode")
    } else {
      kvStore.put(putRequest)
      putRequest.tsMillis.foreach {
        ts: Long =>
          StreamingMetrics.Egress.reportLatency(System.currentTimeMillis() - ts,
            metricsContext = context)
      }
    }
  }

  override def close(errorOrNull: Throwable): Unit = {
  }
}
