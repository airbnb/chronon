package ai.chronon.online.test

import ai.chronon.aggregator.windowing.FinalBatchIr
import org.junit.Assert.{assertEquals, assertFalse, assertTrue}
import ai.chronon.api.{Aggregation, Builders, FloatType, GroupBy, IntType, ListType, LongType, Operation, Row, StringType, TimeUnit, Window}
import org.junit.Test
import com.github.benmanes.caffeine.cache.{Cache => CaffeineCache}
import ai.chronon.online.Cache
import ai.chronon.online.BaseFetcher.BatchIrCacheKey

import scala.collection.JavaConverters._

class CacheTest {
  val testCache: CaffeineCache[String, String] = Cache[String, String]("testCache")

  @Test
  def testGetsNothingWhenThereIsNothing(): Unit = {
    assert(testCache.getIfPresent("key") == null)
    assert(testCache.estimatedSize() == 0)
  }

  @Test
  def testGetsSomethingWhenThereIsSomething(): Unit = {
    assert(testCache.getIfPresent("key") == null)
    testCache.put("key", "value")
    assert(testCache.getIfPresent("key") == "value")
    assert(testCache.estimatedSize() == 1)
  }

  @Test
  def testEvictsWhenSomethingIsSet(): Unit = {
    assert(testCache.estimatedSize() == 0)
    assert(testCache.getIfPresent("key") == null)
    testCache.put("key", "value")
    assert(testCache.estimatedSize() == 1)
    assert(testCache.getIfPresent("key") == "value")
    testCache.invalidate("key")
    assert(testCache.estimatedSize() == 0)
    assert(testCache.getIfPresent("key") == null)
  }
}
