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

package ai.chronon.online

import org.slf4j.LoggerFactory

import java.util.concurrent.{ArrayBlockingQueue, ConcurrentHashMap, ThreadPoolExecutor, TimeUnit}
import java.util.concurrent.atomic.AtomicBoolean
import java.util.function

object TTLCache {
  private[TTLCache] val executor = FlexibleExecutionContext.buildExecutor
}
// can continuously grow, only used for schemas
// has two methods apply & refresh. Apply uses a longer ttl before updating than refresh
// Four 9's of availability is 8.64 secs of downtime per day. Batch uploads happen once per day
// we choose 8 secs as the refresh interval. Refresh is to be used when an exception happens and we want to re-fetch.
class TTLCache[I, O](f: I => O,
                     contextBuilder: I => Metrics.Context,
                     ttlMillis: Long = 2 * 60 * 60 * 1000, // 2 hours
                     nowFunc: () => Long = { () => System.currentTimeMillis() },
                     refreshIntervalMillis: Long = 8 * 1000 // 8 seconds
) {
  case class Entry(value: O, updatedAtMillis: Long, var markedForUpdate: AtomicBoolean = new AtomicBoolean(false))
  @transient implicit lazy val logger = LoggerFactory.getLogger(getClass)
  private val updateWhenNull =
    new function.BiFunction[I, Entry, Entry] {
      override def apply(t: I, u: Entry): Entry = {
        val now = nowFunc()
        if (u == null) {
          Entry(f(t), now)
        } else {
          u
        }
      }
    }

  val cMap = new ConcurrentHashMap[I, Entry]()
  // use the fact that cache update is not immediately necessary during regular reads
  // sync update would block the calling threads on every update
  private def asyncUpdateOnExpiry(i: I, intervalMillis: Long): O = {
    val entry = cMap.get(i)
    if (entry == null) {
      // block all concurrent callers of this key only on the very first read
      val entry = cMap.compute(i, updateWhenNull)
      contextBuilder(i).increment("cache.insert")
      entry.value
    } else {
      if (
        (nowFunc() - entry.updatedAtMillis > intervalMillis) &&
        // CAS so that update is enqueued only once per expired entry
        entry.markedForUpdate.compareAndSet(false, true)
      ) {
        // enqueue async update and return old value
        TTLCache.executor.execute(new Runnable {
          override def run(): Unit = {
            try {
              cMap.put(i, Entry(f(i), nowFunc()))
              contextBuilder(i).increment("cache.update")
            } catch {
              case ex: Exception =>
                // reset the mark so that another thread can retry
                cMap.get(i).markedForUpdate.compareAndSet(true, false);
                contextBuilder(i).incrementException(ex);
            }
          }
        })
      }
      entry.value
    }
  }

  def apply(i: I): O = asyncUpdateOnExpiry(i, ttlMillis)
  // manually refresh entry with a lower interval
  def refresh(i: I): O = asyncUpdateOnExpiry(i, refreshIntervalMillis)
  def force(i: I): O = asyncUpdateOnExpiry(i, 0)
}
