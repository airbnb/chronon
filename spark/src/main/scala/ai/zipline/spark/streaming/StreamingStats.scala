package ai.zipline.spark.streaming

import ai.zipline.online.KVStore.PutRequest
import org.apache.commons.io.FileUtils

import java.time.format.DateTimeFormatter
import java.time.{Instant, ZoneId, ZoneOffset}

class StreamingStats(val publishDelaySeconds: Int) {
  private var latencyMsTotal: Long = 0
  private var writesTotal: Long = 0
  private var keyBytesTotal: Long = 0
  private var valueBytesTotal: Long = 0
  private var startMs: Long = System.currentTimeMillis()

  private val utc = DateTimeFormatter.ISO_LOCAL_DATE_TIME.withZone(ZoneId.from(ZoneOffset.UTC))

  private def timeString(formatter: DateTimeFormatter, ts: Long) = formatter.format(Instant.ofEpochMilli(ts))

  def printStatus(): Unit = {
    if (writesTotal > 0) {
      println("in print sync")
      val now = System.currentTimeMillis()
      def readable = (x: Long) => FileUtils.byteCountToDisplaySize(x)
      val threadName = s"Thread-${Thread.currentThread().getId}"
      println(s"""
         |[$threadName][${timeString(utc, now)}] Wrote $writesTotal records in last ${now - startMs} ms.         
         |    Latency: ${latencyMsTotal / writesTotal} (avg)
         |   Key Size: ${keyBytesTotal / writesTotal} bytes (avg) / ${readable(keyBytesTotal)} (total)
         | Value Size: ${valueBytesTotal / writesTotal} bytes (avg) / ${readable(valueBytesTotal)} (total)
         |""".stripMargin)
      latencyMsTotal = 0
      writesTotal = 0
      keyBytesTotal = 0
      valueBytesTotal = 0
      startMs = now
    } else {
      println("No writes registered")
    }
  }

  def increment(putRequest: PutRequest): Unit = {
    putRequest.tsMillis.foreach(latencyMsTotal += System.currentTimeMillis() - _)
    writesTotal += 1
    if (putRequest.keyBytes != null) keyBytesTotal += putRequest.keyBytes.length
    if (putRequest.valueBytes != null) valueBytesTotal += putRequest.valueBytes.length
    if (System.currentTimeMillis() - startMs > publishDelaySeconds * 1000L) printStatus()
  }
}
