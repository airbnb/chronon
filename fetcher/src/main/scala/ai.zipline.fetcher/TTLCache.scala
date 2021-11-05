package ai.zipline.fetcher

import java.util.concurrent.ConcurrentHashMap
import java.util.function

// can continuously grow, only used for schemas
class TTLCache[I, O](f: I => O,
                     ttlMillis: Long = 2 * 60 * 60 * 1000, // 2 hours
                     nowFunc: () => Long = { () => System.currentTimeMillis() }) {
  val func = new function.BiFunction[I, (Long, O), (Long, O)] {
    override def apply(t: I, u: (Long, O)): (Long, O) = {
      val now = nowFunc()
      if (u == null || now - u._1 > ttlMillis) {
        now -> f(t)
      } else {
        u
      }
    }
  }

  val cMap = new ConcurrentHashMap[I, (Long, O)]()
  def apply(i: I): O = cMap.compute(i, func)._2
  def force(i: I): O = cMap.put(i, nowFunc() -> f(i))._2
}
