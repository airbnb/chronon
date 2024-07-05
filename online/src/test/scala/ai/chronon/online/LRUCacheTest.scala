package ai.chronon.online

import org.junit.Test
import com.github.benmanes.caffeine.cache.{Cache => CaffeineCache}
import ai.chronon.online.LRUCache

import scala.collection.JavaConverters._

class LRUCacheTest {
  val testCache: CaffeineCache[String, String] = LRUCache[String, String]("testCache")

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
