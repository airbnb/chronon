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
        context.distribution(Metrics.Name.FreshnessMillis, System.currentTimeMillis() - ts)
        context.increment(Metrics.Name.RowCount)
        context.distribution(Metrics.Name.ValueBytes, putRequest.valueBytes.length)
        context.distribution(Metrics.Name.KeyBytes, putRequest.keyBytes.length)
      }
    }
  }

  override def close(errorOrNull: Throwable): Unit = {}
}
