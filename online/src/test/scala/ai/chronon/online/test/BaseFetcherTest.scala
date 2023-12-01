package ai.chronon.online.test

import ai.chronon.aggregator.windowing.FinalBatchIr
import ai.chronon.online.BaseFetcher.BatchIrCacheKey
import ai.chronon.online.Cache
import com.github.benmanes.caffeine.cache.{Cache => CaffeineCache}
import org.junit.Assert.assertEquals
import org.junit.Test

import scala.collection.JavaConverters._

class BaseFetcherTest {
  /* Test the BaseFetcher Cache */
  val batchIrCacheMaximumSize = 50

  @Test
  def testCorrectlyCachesBatchIrs(): Unit = {
    val batchIrCache: CaffeineCache[BatchIrCacheKey, FinalBatchIr] =
      Cache[BatchIrCacheKey, FinalBatchIr]("test", batchIrCacheMaximumSize)
    val dataset = "TEST_GROUPBY_BATCH"
    val batchEndTsMillis = 1000L

    def createBatchir(i: Int) = FinalBatchIr(collapsed = Array(i), tailHops = Array(Array(Array(i)), Array(Array(i))))
    def createCacheKey(i: Int) = BatchIrCacheKey(dataset, Map("key" -> i), batchEndTsMillis)

    // Create a bunch of test batchIrs and store them in cache
    val batchIrs = (0 until batchIrCacheMaximumSize).map(
      i => createCacheKey(i) -> createBatchir(i)
    ).toMap
    batchIrCache.putAll(batchIrs.asJava)

    // Check that the cache contains all the batchIrs we created
    batchIrs.foreach((entry) => {
      val cachedBatchIr = batchIrCache.getIfPresent(entry._1)
      assertEquals(cachedBatchIr, entry._2)
    })
  }


  /**
   * Test that the cache keys are compared by equality, not by reference.
   * In practice, this means that if two keys have the same (dataset, keys, batchEndTsMillis), they will only be
   * stored once in the cache.
   */
  @Test
  def testBatchIrCacheKeysAreComparedByEquality(): Unit = {
    val batchIrCache: CaffeineCache[BatchIrCacheKey, FinalBatchIr] =
      Cache[BatchIrCacheKey, FinalBatchIr]("test", batchIrCacheMaximumSize)
    val dataset = "TEST_GROUPBY_BATCH"
    val batchEndTsMillis = 1000L

    def createBatchir(i: Int) = FinalBatchIr(collapsed = Array(i), tailHops = Array(Array(Array(i)), Array(Array(i))))
    def createCacheKey(i: Int) = BatchIrCacheKey(dataset, Map("key" -> i), batchEndTsMillis)

    assert(batchIrCache.estimatedSize() == 0)
    batchIrCache.put(createCacheKey(1), createBatchir(1))
    assert(batchIrCache.estimatedSize() == 1)
    // Create a second key object with the same values as the first key, make sure it's not stored separately
    batchIrCache.put(createCacheKey(1), createBatchir(1))
    assert(batchIrCache.estimatedSize() == 1)
  }
}
