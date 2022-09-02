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

  // isNew: when true, we should attempt to capture this entry update in control event
  // duringException: when true, we are blocked from publishing control event, so we must capture this status so
  //                  that we can publish it next time when we are not in an exception
  case class Entry(value: O, updatedAtMillis: Long, isNew: Boolean, duringException: Boolean)

  private def funcForInterval(intervalMillis: Long, duringException: Boolean) =
    new function.BiFunction[I, Entry, Entry] {
      override def apply(t: I, u: Entry): Entry = {
        val now = nowFunc()
        if (u == null || now - u.updatedAtMillis > intervalMillis) {
          Entry(
            f(t),
            now,
            isNew = true,
            duringException = duringException
          )
        } else {
          u.copy(
            isNew = u.duringException,
            duringException = duringException
          )
        }
      }
    }
  private val refreshFunc = funcForInterval(refreshIntervalMillis, duringException = true)
  private val applyFunc = funcForInterval(ttlMillis, duringException = false)

  val cMap = new ConcurrentHashMap[I, Entry]()
  def apply(i: I): O = applyAndGetStatus(i)._1
  def applyAndGetStatus(i: I): (O, Boolean) = {
    val entry = cMap.compute(i, applyFunc)
    (entry.value, entry.isNew)
  }
  // manually refresh entry with a lower interval
  def refresh(i: I): O = cMap.compute(i, refreshFunc).value
  def force(i: I, duringException: Boolean = false): O =
    cMap.put(i, Entry(f(i), nowFunc(), isNew = true, duringException)).value
}
