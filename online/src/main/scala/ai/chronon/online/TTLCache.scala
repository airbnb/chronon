package ai.chronon.online

import java.util.concurrent.ConcurrentHashMap
import java.util.function

// can continuously grow, only used for schemas
// has two methods apply & refresh. Apply uses a longer ttl before updating than refresh
// Four 9's of availability is 8.64 secs of downtime per day. Batch uploads happen once per day
// we choose 8 secs as the refresh interval. Refresh is to be used when an exception happens and we want to re-fetch.
class TTLCache[I, O](f: I => O,
                     ttlMillis: Long = 2 * 60 * 60 * 1000, // 2 hours
                     nowFunc: () => Long = { () => System.currentTimeMillis() },
                     refreshIntervalMillis: Long = 8 * 1000 // 8 seconds
) {
  case class Entry(value: O, updatedAtMillis: Long)

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
  def apply(i: I): O = cMap.compute(i, applyFunc).value
  // manually refresh entry with a lower interval
  def refresh(i: I): O = cMap.compute(i, refreshFunc).value
  def force(i: I): O = cMap.put(i, Entry(f(i), nowFunc())).value
}
