package ai.chronon.online

import ai.chronon.aggregator.windowing.FinalBatchIr
import ai.chronon.api.Extensions.GroupByOps
import ai.chronon.api.GroupBy
import ai.chronon.online.FetcherBase._
import ai.chronon.online.Fetcher.Request
import ai.chronon.online.FetcherCache.{BatchIrCache, BatchResponses, CachedMapBatchResponse}
import ai.chronon.online.KVStore.TimedValue
import ai.chronon.online.Metrics.Context
import org.junit.Assert.{assertArrayEquals, assertEquals, assertNull, fail}
import org.junit.Test
import org.mockito.Mockito._
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito
import org.mockito.stubbing.Stubber
import org.scalatestplus.mockito.MockitoSugar

import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}

trait MockitoHelper extends MockitoSugar {
  // We override doReturn to fix a known Java/Scala interoperability issue. Without this fix, we see "doReturn:
  // ambiguous reference to overloaded definition" errors. An alternative would be to use the 'mockito-scala' library.
  def doReturn(toBeReturned: Any): Stubber = {
    Mockito.doReturn(toBeReturned, Nil: _*)
  }
}

class FetcherCacheTest extends MockitoHelper {
  class TestableFetcherCache(cache: Option[BatchIrCache]) extends FetcherCache {
    override val maybeBatchIrCache: Option[BatchIrCache] = cache
  }
  val batchIrCacheMaximumSize = 50

  @Test
  def testBatchIrCacheCorrectlyCachesBatchIrs(): Unit = {
    val cacheName = "test"
    val batchIrCache = new BatchIrCache(cacheName, batchIrCacheMaximumSize)
    val dataset = "TEST_GROUPBY_BATCH"
    val batchEndTsMillis = 1000L

    def createBatchir(i: Int) =
      BatchResponses(FinalBatchIr(collapsed = Array(i), tailHops = Array(Array(Array(i)), Array(Array(i)))))
    def createCacheKey(i: Int) = BatchIrCache.Key(dataset, Map("key" -> i), batchEndTsMillis)

    // Create a bunch of test batchIrs and store them in cache
    val batchIrs: Map[BatchIrCache.Key, BatchIrCache.Value] =
      (0 until batchIrCacheMaximumSize).map(i => createCacheKey(i) -> createBatchir(i)).toMap
    batchIrCache.cache.putAll(batchIrs.asJava)

    // Check that the cache contains all the batchIrs we created
    batchIrs.foreach(entry => {
      val cachedBatchIr = batchIrCache.cache.getIfPresent(entry._1)
      assertEquals(cachedBatchIr, entry._2)
    })
  }

  @Test
  def testBatchIrCacheCorrectlyCachesMapResponse(): Unit = {
    val cacheName = "test"
    val batchIrCache = new BatchIrCache(cacheName, batchIrCacheMaximumSize)
    val dataset = "TEST_GROUPBY_BATCH"
    val batchEndTsMillis = 1000L

    def createMapResponse(i: Int) =
      BatchResponses(Map("group_by_key" -> i.asInstanceOf[AnyRef]))
    def createCacheKey(i: Int) = BatchIrCache.Key(dataset, Map("key" -> i), batchEndTsMillis)

    // Create a bunch of test mapResponses and store them in cache
    val mapResponses: Map[BatchIrCache.Key, BatchIrCache.Value] =
      (0 until batchIrCacheMaximumSize).map(i => createCacheKey(i) -> createMapResponse(i)).toMap
    batchIrCache.cache.putAll(mapResponses.asJava)

    // Check that the cache contains all the mapResponses we created
    mapResponses.foreach(entry => {
      val cachedBatchIr = batchIrCache.cache.getIfPresent(entry._1)
      assertEquals(cachedBatchIr, entry._2)
    })
  }

  // Test that the cache keys are compared by equality, not by reference. In practice, this means that if two keys
  // have the same (dataset, keys, batchEndTsMillis), they will only be stored once in the cache.
  @Test
  def testBatchIrCacheKeysAreComparedByEquality(): Unit = {
    val cacheName = "test"
    val batchIrCache = new BatchIrCache(cacheName, batchIrCacheMaximumSize)

    val dataset = "TEST_GROUPBY_BATCH"
    val batchEndTsMillis = 1000L

    def createCacheValue(i: Int) =
      BatchResponses(FinalBatchIr(collapsed = Array(i), tailHops = Array(Array(Array(i)), Array(Array(i)))))
    def createCacheKey(i: Int) = BatchIrCache.Key(dataset, Map("key" -> i), batchEndTsMillis)

    assert(batchIrCache.cache.estimatedSize() == 0)
    batchIrCache.cache.put(createCacheKey(1), createCacheValue(1))
    assert(batchIrCache.cache.estimatedSize() == 1)
    // Create a second key object with the same values as the first key, make sure it's not stored separately
    batchIrCache.cache.put(createCacheKey(1), createCacheValue(1))
    assert(batchIrCache.cache.estimatedSize() == 1)
  }

  @Test
  def testGetCachedRequestsReturnsCorrectCachedDataWhenCacheIsEnabled(): Unit = {
    val cacheName = "test"
    val testCache = Some(new BatchIrCache(cacheName, batchIrCacheMaximumSize))
    val fetcherCache = new TestableFetcherCache(testCache) {
      override def isCachingEnabled(groupBy: GroupBy) = true
    }

    // Prepare groupByRequestToKvRequest
    val batchEndTsMillis = 0L
    val keys = Map("key" -> "value")
    val eventTs = 1000L
    val dataset = "TEST_GROUPBY_BATCH"
    val mockGroupByServingInfoParsed = mock[GroupByServingInfoParsed]
    val mockContext = mock[Metrics.Context]
    val request = Request("req_name", keys, Some(eventTs), Some(mock[Context]))
    val getRequest = KVStore.GetRequest("key".getBytes, dataset, Some(eventTs))
    val requestMeta =
      GroupByRequestMeta(mockGroupByServingInfoParsed, getRequest, Some(getRequest), Some(eventTs), mockContext)
    val groupByRequestToKvRequest: Seq[(Request, Try[GroupByRequestMeta])] = Seq((request, Success(requestMeta)))

    // getCachedRequests should return an empty list when the cache is empty
    val cachedRequestBeforePopulating = fetcherCache.getCachedRequests(groupByRequestToKvRequest)
    assert(cachedRequestBeforePopulating.isEmpty)

    // Add a GetRequest and a FinalBatchIr
    val key = BatchIrCache.Key(getRequest.dataset, keys, batchEndTsMillis)
    val finalBatchIr = BatchResponses(FinalBatchIr(Array(1), Array(Array(Array(1)), Array(Array(1)))))
    testCache.get.cache.put(key, finalBatchIr)

    // getCachedRequests should return the GetRequest and FinalBatchIr we cached
    val cachedRequestsAfterAddingItem = fetcherCache.getCachedRequests(groupByRequestToKvRequest)
    assert(cachedRequestsAfterAddingItem.head._1 == getRequest)
    assert(cachedRequestsAfterAddingItem.head._2 == finalBatchIr)
  }

  @Test
  def testGetCachedRequestsDoesNotCacheWhenCacheIsDisabledForGroupBy(): Unit = {
    val testCache = new BatchIrCache("test", batchIrCacheMaximumSize)
    val spiedTestCache = spy(testCache)
    val fetcherCache = new TestableFetcherCache(Some(testCache)) {
      // Cache is enabled globally, but disabled for a specific groupBy
      override def isCachingEnabled(groupBy: GroupBy) = false
    }

    // Prepare groupByRequestToKvRequest
    val keys = Map("key" -> "value")
    val eventTs = 1000L
    val dataset = "TEST_GROUPBY_BATCH"
    val mockGroupByServingInfoParsed = mock[GroupByServingInfoParsed]
    val mockContext = mock[Metrics.Context]
    val request = Request("req_name", keys, Some(eventTs))
    val getRequest = KVStore.GetRequest("key".getBytes, dataset, Some(eventTs))
    val requestMeta =
      GroupByRequestMeta(mockGroupByServingInfoParsed, getRequest, Some(getRequest), Some(eventTs), mockContext)
    val groupByRequestToKvRequest: Seq[(Request, Try[GroupByRequestMeta])] = Seq((request, Success(requestMeta)))

    val cachedRequests = fetcherCache.getCachedRequests(groupByRequestToKvRequest)
    assert(cachedRequests.isEmpty)
    // Cache was never called
    verify(spiedTestCache, never()).cache
  }

  @Test
  def testGetBatchBytesReturnsLatestTimedValueBytesIfGreaterThanBatchEnd(): Unit = {
    val kvStoreResponse = Success(
      Seq(TimedValue(Array(1.toByte), 1000L), TimedValue(Array(2.toByte), 2000L))
    )
    val batchResponses = BatchResponses(kvStoreResponse)
    val batchBytes = batchResponses.getBatchBytes(1500L)
    assertArrayEquals(Array(2.toByte), batchBytes)
  }

  @Test
  def testGetBatchBytesReturnsNullIfLatestTimedValueTimestampIsLessThanBatchEnd(): Unit = {
    val kvStoreResponse = Success(
      Seq(TimedValue(Array(1.toByte), 1000L), TimedValue(Array(2.toByte), 1500L))
    )
    val batchResponses = BatchResponses(kvStoreResponse)
    val batchBytes = batchResponses.getBatchBytes(2000L)
    assertNull(batchBytes)
  }

  @Test
  def testGetBatchBytesReturnsNullWhenCachedBatchResponse(): Unit = {
    val finalBatchIr = mock[FinalBatchIr]
    val batchResponses = BatchResponses(finalBatchIr)
    val batchBytes = batchResponses.getBatchBytes(1000L)
    assertNull(batchBytes)
  }

  @Test
  def testGetBatchBytesReturnsNullWhenKvStoreBatchResponseFails(): Unit = {
    val kvStoreResponse = Failure(new RuntimeException("KV Store error"))
    val batchResponses = BatchResponses(kvStoreResponse)
    val batchBytes = batchResponses.getBatchBytes(1000L)
    assertNull(batchBytes)
  }

  @Test
  def testGetBatchIrFromBatchResponseReturnsCorrectIRsWithCacheEnabled(): Unit = {
    // Use a real cache
    val batchIrCache = new BatchIrCache("test_cache", batchIrCacheMaximumSize)

    // Create all necessary mocks
    val servingInfo = mock[GroupByServingInfoParsed]
    val groupByOps = mock[GroupByOps]
    val toBatchIr = mock[(Array[Byte], GroupByServingInfoParsed) => FinalBatchIr]
    when(servingInfo.groupByOps).thenReturn(groupByOps)
    when(groupByOps.batchDataset).thenReturn("test_dataset")
    when(servingInfo.groupByOps.batchDataset).thenReturn("test_dataset")
    when(servingInfo.batchEndTsMillis).thenReturn(1000L)

    // Dummy data
    val batchBytes = Array[Byte](1, 1)
    val keys = Map("key" -> "value")
    val cacheKey = BatchIrCache.Key(servingInfo.groupByOps.batchDataset, keys, servingInfo.batchEndTsMillis)

    val fetcherCache = new TestableFetcherCache(Some(batchIrCache))
    val spiedFetcherCache = Mockito.spy(fetcherCache)
    doReturn(true).when(spiedFetcherCache).isCachingEnabled(any())

    // 1. Cached BatchResponse returns the same IRs passed in
    val finalBatchIr1 = mock[FinalBatchIr]
    val cachedBatchResponse = BatchResponses(finalBatchIr1)
    val cachedIr =
      spiedFetcherCache.getBatchIrFromBatchResponse(cachedBatchResponse, batchBytes, servingInfo, toBatchIr, keys)
    assertEquals(finalBatchIr1, cachedIr)
    verify(toBatchIr, never())(any(classOf[Array[Byte]]), any()) // no decoding needed

    // 2. Un-cached BatchResponse has IRs added to cache
    val finalBatchIr2 = mock[FinalBatchIr]
    val kvStoreBatchResponses = BatchResponses(Success(Seq(TimedValue(batchBytes, 1000L))))
    when(toBatchIr(any(), any())).thenReturn(finalBatchIr2)
    val uncachedIr =
      spiedFetcherCache.getBatchIrFromBatchResponse(kvStoreBatchResponses, batchBytes, servingInfo, toBatchIr, keys)
    assertEquals(finalBatchIr2, uncachedIr)
    assertEquals(batchIrCache.cache.getIfPresent(cacheKey), BatchResponses(finalBatchIr2)) // key was added
    verify(toBatchIr, times(1))(any(), any()) // decoding did happen
  }

  @Test
  def testGetBatchIrFromBatchResponseDecodesBatchBytesIfCacheDisabled(): Unit = {
    // Set up mocks and dummy data
    val servingInfo = mock[GroupByServingInfoParsed]
    val batchBytes = Array[Byte](1, 2, 3)
    val keys = Map("key" -> "value")
    val finalBatchIr = mock[FinalBatchIr]
    val toBatchIr = mock[(Array[Byte], GroupByServingInfoParsed) => FinalBatchIr]
    val kvStoreBatchResponses = BatchResponses(Success(Seq(TimedValue(batchBytes, 1000L))))

    val spiedFetcherCache = Mockito.spy(new TestableFetcherCache(None))
    when(toBatchIr(any(), any())).thenReturn(finalBatchIr)

    // When getBatchIrFromBatchResponse is called, it decodes the bytes and doesn't hit the cache
    val ir =
      spiedFetcherCache.getBatchIrFromBatchResponse(kvStoreBatchResponses, batchBytes, servingInfo, toBatchIr, keys)
    verify(toBatchIr, times(1))(batchBytes, servingInfo) // decoding did happen
    assertEquals(finalBatchIr, ir)
  }

  @Test
  def testGetBatchIrFromBatchResponseReturnsCorrectMapResponseWithCacheEnabled(): Unit = {
    // Use a real cache
    val batchIrCache = new BatchIrCache("test_cache", batchIrCacheMaximumSize)
    // Set up mocks and dummy data
    val servingInfo = mock[GroupByServingInfoParsed]
    val groupByOps = mock[GroupByOps]
    val outputCodec = mock[AvroCodec]
    when(servingInfo.groupByOps).thenReturn(groupByOps)
    when(groupByOps.batchDataset).thenReturn("test_dataset")
    when(servingInfo.groupByOps.batchDataset).thenReturn("test_dataset")
    when(servingInfo.batchEndTsMillis).thenReturn(1000L)
    val batchBytes = Array[Byte](1, 2, 3)
    val keys = Map("key" -> "value")
    val cacheKey = BatchIrCache.Key(servingInfo.groupByOps.batchDataset, keys, servingInfo.batchEndTsMillis)

    val spiedFetcherCache = Mockito.spy(new TestableFetcherCache(Some(batchIrCache)))
    doReturn(true).when(spiedFetcherCache).isCachingEnabled(any())

    // 1. Cached BatchResponse returns the same Map responses passed in
    val mapResponse1 = mock[Map[String, AnyRef]]
    val cachedBatchResponse = BatchResponses(mapResponse1)
    val decodingFunction1 = (bytes: Array[Byte]) => {
      fail("Decoding function should not be called when batch response is cached")
      mapResponse1
    }
    val cachedMapResponse = spiedFetcherCache.getMapResponseFromBatchResponse(cachedBatchResponse,
                                                                              batchBytes,
                                                                              decodingFunction1,
                                                                              servingInfo,
                                                                              keys)
    assertEquals(mapResponse1, cachedMapResponse)

    // 2. Un-cached BatchResponse has Map responses added to cache
    val mapResponse2 = mock[Map[String, AnyRef]]
    val kvStoreBatchResponses = BatchResponses(Success(Seq(TimedValue(batchBytes, 1000L))))
    def decodingFunction2 = (bytes: Array[Byte]) => mapResponse2
    val decodedMapResponse = spiedFetcherCache.getMapResponseFromBatchResponse(kvStoreBatchResponses,
                                                                               batchBytes,
                                                                               decodingFunction2,
                                                                               servingInfo,
                                                                               keys)
    assertEquals(mapResponse2, decodedMapResponse)
    assertEquals(batchIrCache.cache.getIfPresent(cacheKey), CachedMapBatchResponse(mapResponse2)) // key was added
  }

  @Test
  def testGetMapResponseFromBatchResponseDecodesBatchBytesIfCacheDisabled(): Unit = {
    // Set up mocks and dummy data
    val servingInfo = mock[GroupByServingInfoParsed]
    val batchBytes = Array[Byte](1, 2, 3)
    val keys = Map("key" -> "value")
    val mapResponse = mock[Map[String, AnyRef]]
    val outputCodec = mock[AvroCodec]
    val kvStoreBatchResponses = BatchResponses(Success(Seq(TimedValue(batchBytes, 1000L))))
    when(servingInfo.outputCodec).thenReturn(outputCodec)
    when(outputCodec.decodeMap(any())).thenReturn(mapResponse)

    val spiedFetcherCache = Mockito.spy(new TestableFetcherCache(None))

    // When getMapResponseFromBatchResponse is called, it decodes the bytes and doesn't hit the cache
    val decodedMapResponse = spiedFetcherCache.getMapResponseFromBatchResponse(kvStoreBatchResponses,
                                                                               batchBytes,
                                                                               servingInfo.outputCodec.decodeMap,
                                                                               servingInfo,
                                                                               keys)
    verify(servingInfo.outputCodec.decodeMap(any()), times(1)) // decoding did happen
    assertEquals(mapResponse, decodedMapResponse)
  }
}
