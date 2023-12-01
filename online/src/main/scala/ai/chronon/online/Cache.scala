package ai.chronon.online

import com.github.benmanes.caffeine.cache.{Caffeine, Cache => CaffeineCache}

/**
 * Cache utility for caching KVStore calls and decoded IRs.
 *
 * The purpose of in-memory caching is to decrease feature serving latency on the online side of Chronon.
 */
object Cache {
  /**
   * Build a bounded, thread-safe Caffeine cache that stores KEY-VALUE pairs.
   *
   * @param cacheName Name of the cache
   * @param maximumSize Maximum number of entries in the cache
   * @tparam KEY   The type of the key used to access the cache
   * @tparam VALUE The type of the value stored in the cache
   * @return Caffeine cache
   */
  def apply[KEY <: Object, VALUE <: Object](cacheName: String, maximumSize: Int = 20000): CaffeineCache[KEY, VALUE] = {
    buildCaffeineCache[KEY, VALUE](cacheName, maximumSize)
  }

  private def buildCaffeineCache[KEY <: Object, VALUE <: Object](cacheName: String, maximumSize: Int = 20000): CaffeineCache[KEY, VALUE] = {
    println(s"Chronon Cache build started. cacheName=$cacheName")
    val cache: CaffeineCache[KEY, VALUE] = Caffeine.newBuilder()
      .maximumSize(maximumSize)
      .recordStats()
      .build[KEY, VALUE]()
    println(s"Chronon Cache build finished. cacheName=$cacheName")
    cache
  }

  /**
   * Report metrics for a Caffeine cache. The "cache" tag is added to all metrics.
   *
   * @param metricsContext Metrics.Context for recording metrics
   * @param cache          Caffeine cache to get metrics from
   * @param cacheName      Cache name for tagging
   */
  def collectCaffeineCacheMetrics(metricsContext: Metrics.Context, cache: CaffeineCache[_, _], cacheName: String): Unit = {
    val context = metricsContext.withSuffix("cache")
    val stats = cache.stats()
    context.gauge(s"$cacheName.hits", stats.hitCount())
    context.gauge(s"$cacheName.misses", stats.missCount())
    context.gauge(s"$cacheName.evictions", stats.evictionCount())
    context.gauge(s"$cacheName.loads", stats.loadCount())
    context.gauge(s"$cacheName.hit_rate", stats.hitRate())
    context.gauge(s"$cacheName.average_load_penalty", stats.averageLoadPenalty())
  }
}
