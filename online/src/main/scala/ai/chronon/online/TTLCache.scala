package ai.chronon.online

import jnr.ffi.annotations.Synchronized

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicBoolean
import java.util.function
import scala.concurrent.ExecutionContext

// can continuously grow, only used for schemas
// has two methods apply & refresh. Apply uses a longer ttl before updating than refresh
// Four 9's of availability is 8.64 secs of downtime per day. Batch uploads happen once per day
// we choose 8 secs as the refresh interval. Refresh is to be used when an exception happens and we want to re-fetch.
class TTLCache[I, O](f: I => O,
                     ttlMillis: Long = 2 * 60 * 60 * 1000, // 2 hours
                     nowFunc: () => Long = { () => System.currentTimeMillis() },
                     refreshIntervalMillis: Long = 8 * 1000 // 8 seconds
) {
  case class Entry(value: O, updatedAtMillis: Long, var markedForUpdate: AtomicBoolean = new AtomicBoolean(false))

  private def funcForInterval(intervalMillis: Long) =
    new function.BiFunction[I, Entry, Entry] {
      override def apply(t: I, u: Entry): Entry = {
        val now = nowFunc()
        if (u == null || now - u.updatedAtMillis > intervalMillis) {
          Entry(f(t), now)
        } else {
          u
        }
      }
    }
  private val refreshFunc = funcForInterval(refreshIntervalMillis)
  private val applyFunc = funcForInterval(ttlMillis)

  val cMap = new ConcurrentHashMap[I, Entry]()

  // use the fact that cache update is not immediately necessary during regular reads
  private def asyncUpdateOnExpiry(i: I, intervalMillis: Long): O = {
    val entry = cMap.get(i)
    if (entry == null) {
      // block all concurrent callers of this key only on the very first read
      cMap.compute(i, applyFunc).value
    } else {
      // CAS so that update is issued only once
      if ((nowFunc() - entry.updatedAtMillis > intervalMillis) && entry.markedForUpdate.compareAndSet(false, true)) {
        // enqueue async update and return old value
        ExecutionContext.global.execute(new Runnable {
          override def run(): Unit = {
            cMap.put(i, Entry(f(i), nowFunc()))
          }
        })
      }
      entry.value
    }
  }

  def apply(i: I): O = asyncUpdateOnExpiry(i, ttlMillis)
  // manually refresh entry with a lower interval
  def refresh(i: I): O = cMap.compute(i, refreshFunc).value
  def force(i: I): O = cMap.put(i, Entry(f(i), nowFunc())).value
}
