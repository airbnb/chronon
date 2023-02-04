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

  private def updateFuncWrapper(intervalMillis: Long) =
    new function.BiFunction[I, Entry, Entry] {
      override def apply(t: I, u: Entry): Entry =
        updateFunc(
          u,
          intervalMillis,
          (now: Long) => Entry(f(t), now)
        )
    }

  def updateFunc(u: Entry, intervalMillis: Long, computeFunc: Long => Entry): Entry = {
    val now = nowFunc()
    if (u == null || now - u.updatedAtMillis > intervalMillis) {
      computeFunc(now)
    } else {
      u
    }
  }

  def get(i: I, intervalMillis: Long): O = {
    val u = cMap.getOrDefault(i, null)
    updateFunc(
      u,
      ttlMillis,
      _ => cMap.compute(i, updateFuncWrapper(intervalMillis))
    ).value
  }

  val cMap = new ConcurrentHashMap[I, Entry]()
  def apply(i: I): O = get(i, intervalMillis = ttlMillis)
  // manually refresh entry with a lower interval
  def refresh(i: I): O = get(i, intervalMillis = refreshIntervalMillis)
  def force(i: I): O = cMap.put(i, Entry(f(i), nowFunc())).value
}
