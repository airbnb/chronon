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

package ai.chronon.spark.streaming

import org.slf4j.LoggerFactory
import ai.chronon.online.KVStore.PutRequest
import com.yahoo.sketches.kll.KllFloatsSketch
import org.apache.commons.io.FileUtils

import java.time.format.DateTimeFormatter
import java.time.{Instant, ZoneId, ZoneOffset}

class StreamingStats(val publishDelaySeconds: Int) {
  @transient lazy val logger = LoggerFactory.getLogger(getClass)
  private var latencyHistogram: KllFloatsSketch = new KllFloatsSketch()
  private var latencyMsTotal: Long = 0
  private var writesTotal: Long = 0
  private var keyBytesTotal: Long = 0
  private var valueBytesTotal: Long = 0
  private var startMs: Long = System.currentTimeMillis()

  private val utc = DateTimeFormatter.ISO_LOCAL_DATE_TIME.withZone(ZoneId.from(ZoneOffset.UTC))

  private def timeString(formatter: DateTimeFormatter, ts: Long) = formatter.format(Instant.ofEpochMilli(ts))

  def printStatus(): Unit = {
    if (writesTotal > 0) {
      val now = System.currentTimeMillis()
      def readable = (x: Long) => FileUtils.byteCountToDisplaySize(x)
      val threadName = s"Thread-${Thread.currentThread().getId}"
      val medianLatency = latencyHistogram.getQuantile(.5)
      val p95Latency = latencyHistogram.getQuantile(.95)
      val p99Latency = latencyHistogram.getQuantile(.99)
      logger.info(s"""
         |[$threadName][${timeString(utc, now)}] Wrote $writesTotal records in last ${now - startMs} ms.         
         | Latency ms: ${latencyMsTotal / writesTotal} (avg) / $medianLatency (median) / $p95Latency (p95) / $p99Latency (p99) 
         |   Key Size: ${keyBytesTotal / writesTotal} bytes (avg) / ${readable(keyBytesTotal)} (total)
         | Value Size: ${valueBytesTotal / writesTotal} bytes (avg) / ${readable(valueBytesTotal)} (total)
         |""".stripMargin)
      latencyMsTotal = 0
      writesTotal = 0
      keyBytesTotal = 0
      valueBytesTotal = 0
      latencyHistogram = new KllFloatsSketch()
      startMs = now
    } else {
      logger.info("No writes registered")
    }
  }

  def increment(putRequest: PutRequest): Unit = {
    putRequest.tsMillis.foreach { queryTime =>
      val latency = System.currentTimeMillis() - queryTime
      latencyMsTotal += latency
      latencyHistogram.update(latency)
    }
    writesTotal += 1
    if (putRequest.keyBytes != null) keyBytesTotal += putRequest.keyBytes.length
    if (putRequest.valueBytes != null) valueBytesTotal += putRequest.valueBytes.length
    if (System.currentTimeMillis() - startMs > publishDelaySeconds * 1000L) printStatus()
  }
}
