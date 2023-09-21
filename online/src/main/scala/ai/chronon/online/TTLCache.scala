package ai.chronon.online

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
      cMap.compute(i, updateWhenNull).value
      contextBuilder(i).increment("cache.insert")
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
