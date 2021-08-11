package ai.zipline.spark

import ai.zipline.api.KVStore.PutRequest
import ai.zipline.api.{KVStore, OnlineImpl, StructType}
import ai.zipline.fetcher.{GroupByServingInfoParsed, RowConversions}
import org.apache.spark.sql.ForeachWriter
import java.util.Base64


class StreamingDataWriter(onlineImpl: OnlineImpl, groupByServingInfoParsed: GroupByServingInfoParsed, debug: Boolean = false) extends ForeachWriter[PutRequest] {

  var kvStore : KVStore = _

  override def open(partitionId: Long, epochId: Long): Boolean = {
    kvStore = onlineImpl.genKvStore
    true
  }

  lazy val avroCodec = new GroupByServingInfoParsed(groupByServingInfoParsed).selectedCodec

  def decodeAvroRecord(bytes: Array[Byte], structType: StructType): Array[Any] = {
    RowConversions.fromAvroRecord(avroCodec.decode(bytes), structType).asInstanceOf[Array[Any]]
  }

  def print(input: PutRequest): Unit = {
    val keyB64 = Base64.getEncoder.encodeToString(input.keyBytes)
    val valueB64 = Base64.getEncoder.encodeToString(input.valueBytes)
    val keys = decodeAvroRecord(input.keyBytes, groupByServingInfoParsed.keyZiplineSchema)
    val values = decodeAvroRecord(input.valueBytes, groupByServingInfoParsed.selectedZiplineSchema)
    println(s" key:[$keyB64],  value:[$valueB64], ts:[${input.tsMillis}] keys:[${keys.mkString(", ")}] values:[${values.mkString(", ")}]")
  }

  override def process(putRequest: PutRequest): Unit = {
    if (debug) print(putRequest)
    kvStore.put(putRequest)
  }

  override def close(errorOrNull: Throwable): Unit = Unit
}
