package ai.chronon.online

import com.github.benmanes.caffeine.cache.{Caffeine, Cache => CaffeineCache}
import org.slf4j.{Logger, LoggerFactory}

/**
  * Utility to create a cache with LRU semantics.
  *
  * The original purpose of having an LRU cache in Chronon is to cache KVStore calls and decoded IRs
  * in the Fetcher. This helps decrease to feature serving latency.
  */
object LRUCache {
  @transient private lazy val logger: Logger = LoggerFactory.getLogger(getClass)

  /**
    * Build a bounded, thread-safe Caffeine cache that stores KEY-VALUE pairs.
    *
    * @param cacheName Name of the cache
    * @param maximumSize Maximum number of entries in the cache
    * @tparam KEY   The type of the key used to access the cache
    * @tparam VALUE The type of the value stored in the cache
    * @return Caffeine cache
    */
  def apply[KEY <: Object, VALUE <: Object](cacheName: String, maximumSize: Int = 10000): CaffeineCache[KEY, VALUE] = {
    buildCaffeineCache[KEY, VALUE](cacheName, maximumSize)
  }

  private def buildCaffeineCache[KEY <: Object, VALUE <: Object](
      cacheName: String,
      maximumSize: Int = 10000): CaffeineCache[KEY, VALUE] = {
    logger.info(s"Chronon Cache build started. cacheName=$cacheName")
    val cache: CaffeineCache[KEY, VALUE] = Caffeine
      .newBuilder()
      .maximumSize(maximumSize)
      .recordStats()
      .build[KEY, VALUE]()
    logger.info(s"Chronon Cache build finished. cacheName=$cacheName")
    cache
  }

  /**
    * Report metrics for a Caffeine cache. The "cache" tag is added to all metrics.
    *
    * @param metricsContext Metrics.Context for recording metrics
    * @param cache          Caffeine cache to get metrics from
    * @param cacheName      Cache name for tagging
    */
  def collectCaffeineCacheMetrics(metricsContext: Metrics.Context,
                                  cache: CaffeineCache[_, _],
                                  cacheName: String): Unit = {
    val stats = cache.stats()
    metricsContext.gauge(s"$cacheName.hits", stats.hitCount())
    metricsContext.gauge(s"$cacheName.misses", stats.missCount())
    metricsContext.gauge(s"$cacheName.evictions", stats.evictionCount())
    metricsContext.gauge(s"$cacheName.loads", stats.loadCount())
    metricsContext.gauge(s"$cacheName.hit_rate", stats.hitRate())
    metricsContext.gauge(s"$cacheName.average_load_penalty", stats.averageLoadPenalty())
  }
}
