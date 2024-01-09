package ai.chronon.online.test

import ai.chronon.aggregator.windowing.FinalBatchIr
import ai.chronon.api.Extensions.GroupByOps
import ai.chronon.api.MetaData
import ai.chronon.online.BaseFetcher._
import ai.chronon.online.{AvroCodec, BaseFetcher, GroupByServingInfoParsed, KVStore, Metrics, TTLCache}
import ai.chronon.online.Fetcher.Request
import ai.chronon.online.KVStore.TimedValue
import org.junit.Assert.{assertArrayEquals, assertEquals, assertNull, assertSame, fail}
import org.junit.Test
import org.mockito.Mockito._
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito
import org.mockito.stubbing.Stubber
import org.scalatestplus.mockito.MockitoSugar

import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}

trait MockitoHelper extends MockitoSugar {
  // Overriding doReturn to fix a known Java/Scala interoperability issue when using Mockito. (doReturn: ambiguous
  // reference to overloaded definition). An alternative would be to use the 'mockito-scala' library.
  def doReturn(toBeReturned: Any): Stubber = {
    Mockito.doReturn(toBeReturned, Nil: _*)
  }
}

class BaseFetcherTest extends MockitoHelper {
  /* Test the BaseFetcher Cache */
  val batchIrCacheMaximumSize = 50

  @Test
  def test_BatchIrCache_CorrectlyCachesBatchIrs(): Unit = {
    val cacheName = "test"
    val batchIrCache = new BatchIrCache(cacheName, batchIrCacheMaximumSize)
    val dataset = "TEST_GROUPBY_BATCH"
    val batchEndTsMillis = 1000L

    def createBatchir(i: Int) =
      Left(FinalBatchIr(collapsed = Array(i), tailHops = Array(Array(Array(i)), Array(Array(i)))))
    def createCacheKey(i: Int) = BatchIrCache.Key(dataset, Map("key" -> i), batchEndTsMillis)

    // Create a bunch of test batchIrs and store them in cache
    val batchIrs: Map[BatchIrCache.Key, BatchIrCache.Value] =
      (0 until batchIrCacheMaximumSize).map(i => createCacheKey(i) -> createBatchir(i)).toMap
    batchIrCache.cache.putAll(batchIrs.asJava)

    // Check that the cache contains all the batchIrs we created
    batchIrs.foreach((entry) => {
      val cachedBatchIr = batchIrCache.cache.getIfPresent(entry._1)
      assertEquals(cachedBatchIr, entry._2)
    })
  }

  @Test
  def test_BatchIrCache_CorrectlyCachesMapResponse(): Unit = {
    val cacheName = "test"
    val batchIrCache = new BatchIrCache(cacheName, batchIrCacheMaximumSize)
    val dataset = "TEST_GROUPBY_BATCH"
    val batchEndTsMillis = 1000L

    def createMapResponse(i: Int) =
      Right(Map("group_by_key" -> i.asInstanceOf[AnyRef]))
    def createCacheKey(i: Int) = BatchIrCache.Key(dataset, Map("key" -> i), batchEndTsMillis)

    // Create a bunch of test mapResponses and store them in cache
    val mapResponses: Map[BatchIrCache.Key, BatchIrCache.Value] =
      (0 until batchIrCacheMaximumSize).map(i => createCacheKey(i) -> createMapResponse(i)).toMap
    batchIrCache.cache.putAll(mapResponses.asJava)

    // Check that the cache contains all the mapResponses we created
    mapResponses.foreach((entry) => {
      val cachedBatchIr = batchIrCache.cache.getIfPresent(entry._1)
      assertEquals(cachedBatchIr, entry._2)
    })
  }

  // Test that the cache keys are compared by equality, not by reference. In practice, this means that if two keys
  // have the same (dataset, keys, batchEndTsMillis), they will only be stored once in the cache.
  @Test
  def test_BatchIrCache_KeysAreComparedByEquality(): Unit = {
    val cacheName = "test"
    val batchIrCache = new BatchIrCache(cacheName, batchIrCacheMaximumSize)

    val dataset = "TEST_GROUPBY_BATCH"
    val batchEndTsMillis = 1000L

    def createCacheValue(i: Int) =
      Left(FinalBatchIr(collapsed = Array(i), tailHops = Array(Array(Array(i)), Array(Array(i)))))
    def createCacheKey(i: Int) = BatchIrCache.Key(dataset, Map("key" -> i), batchEndTsMillis)

    assert(batchIrCache.cache.estimatedSize() == 0)
    batchIrCache.cache.put(createCacheKey(1), createCacheValue(1))
    assert(batchIrCache.cache.estimatedSize() == 1)
    // Create a second key object with the same values as the first key, make sure it's not stored separately
    batchIrCache.cache.put(createCacheKey(1), createCacheValue(1))
    assert(batchIrCache.cache.estimatedSize() == 1)
  }

  @Test
  def test_getCachedRequests_ReturnsCorrectCachedData(): Unit = {
    val cacheName = "test"
    val batchIrCache = new BatchIrCache(cacheName, batchIrCacheMaximumSize)

    // Prepare groupByRequestToKvRequest
    val batchEndTsMillis = 0L
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

    // getCachedRequests should return an empty list when the cache is empty
    val cachedRequestBeforePopulating = BatchIrCache.getCachedRequests(batchIrCache.cache, groupByRequestToKvRequest)
    assert(cachedRequestBeforePopulating.isEmpty)

    // Add a GetRequest and a FinalBatchIr
    val key = BatchIrCache.Key(getRequest.dataset, keys, batchEndTsMillis)
    val finalBatchIr = Left(FinalBatchIr(Array(1), Array(Array(Array(1)), Array(Array(1)))))
    batchIrCache.cache.put(key, finalBatchIr)

    // getCachedRequests should return the GetRequest and FinalBatchIr we cached
    val cachedRequestsAfterAddingItem = BatchIrCache.getCachedRequests(batchIrCache.cache, groupByRequestToKvRequest)
    assert(cachedRequestsAfterAddingItem.head._1 == getRequest)
    assert(cachedRequestsAfterAddingItem.head._2 == finalBatchIr)
  }

  @Test
  def test_getBatchBytes_ReturnsLatestTimedValueBytesIfGreaterThanBatchEnd(): Unit = {
    val kvStoreResponse = Success(
      Seq(TimedValue(Array(1.toByte), 1000L), TimedValue(Array(2.toByte), 2000L))
    )
    val batchResponses = new BatchResponses(kvStoreResponse)
    val batchBytes = batchResponses.getBatchBytes(1500L)
    assertArrayEquals(Array(2.toByte), batchBytes)
  }

  @Test
  def test_getBatchBytes_ReturnsNullIfLatestTimedValueTimestampIsLessThanBatchEnd(): Unit = {
    val kvStoreResponse = Success(
      Seq(TimedValue(Array(1.toByte), 1000L), TimedValue(Array(2.toByte), 1500L))
    )
    val batchResponses = new BatchResponses(kvStoreResponse)
    val batchBytes = batchResponses.getBatchBytes(2000L)
    assertNull(batchBytes)
  }

  @Test
  def test_getBatchBytes_ReturnsNullWhenCachedBatchResponse(): Unit = {
    val finalBatchIr = mock[FinalBatchIr]
    val batchResponses = new BatchResponses(finalBatchIr)
    val batchBytes = batchResponses.getBatchBytes(1000L)
    assertNull(batchBytes)
  }

  @Test
  def test_getBatchBytes_ReturnsNullWhenKvStoreBatchResponseFails(): Unit = {
    val kvStoreResponse = Failure(new RuntimeException("KV Store error"))
    val batchResponses = new BatchResponses(kvStoreResponse)
    val batchBytes = batchResponses.getBatchBytes(1000L)
    assertNull(batchBytes)
  }

  class TestableBaseFetcher(kvStore: KVStore, cache: Option[BaseFetcher.BatchIrCache]) extends BaseFetcher(kvStore) {
    override val maybeBatchIrCache: Option[BaseFetcher.BatchIrCache] = cache
  }

  @Test
  def test_getBatchIrFromBatchResponse_ReturnsCorrectIRsWithCacheEnabled(): Unit = {
    // Use a real cache
    val batchIrCache = new BatchIrCache("test_cache", batchIrCacheMaximumSize)

    // Create all necessary mocks
    val servingInfo = mock[GroupByServingInfoParsed]
    val groupByOps = mock[GroupByOps]
    when(servingInfo.groupByOps).thenReturn(groupByOps)
    when(groupByOps.batchDataset).thenReturn("test_dataset")
    when(servingInfo.groupByOps.batchDataset).thenReturn("test_dataset")
    when(servingInfo.batchEndTsMillis).thenReturn(1000L)

    // Dummy data
    val batchBytes = Array[Byte](1, 1)
    val keys = Map("key" -> "value")
    val cacheKey = BatchIrCache.Key(servingInfo.groupByOps.batchDataset, keys, servingInfo.batchEndTsMillis)

    val baseFetcher = new TestableBaseFetcher(mock[KVStore], Some(batchIrCache))
    val spiedBaseFetcher = Mockito.spy(baseFetcher)

    // 1. Cached BatchResponse returns the same IRs passed in
    val finalBatchIr1 = mock[FinalBatchIr]
    val cachedBatchResponse = new BatchResponses(finalBatchIr1)
    val cachedIr = spiedBaseFetcher.getBatchIrFromBatchResponse(cachedBatchResponse, batchBytes, servingInfo, keys)
    assertEquals(finalBatchIr1, cachedIr)
    verify(spiedBaseFetcher, never()).toBatchIr(any(classOf[Array[Byte]]), any()) // no decoding needed

    // 2. Un-cached BatchResponse has IRs added to cache
    val finalBatchIr2 = mock[FinalBatchIr]
    val kvStoreBatchResponses = new BatchResponses(Success(Seq(TimedValue(batchBytes, 1000L))))
    when(spiedBaseFetcher.toBatchIr(any(), any())).thenReturn(finalBatchIr2)
    val uncachedIr = spiedBaseFetcher.getBatchIrFromBatchResponse(kvStoreBatchResponses, batchBytes, servingInfo, keys)
    assertEquals(finalBatchIr2, uncachedIr)
    assertEquals(batchIrCache.cache.getIfPresent(cacheKey).left.get, finalBatchIr2) // key was added
    verify(spiedBaseFetcher, times(1)).toBatchIr(any(), any()) // decoding did happen
  }

  @Test
  def test_getBatchIrFromBatchResponse_DecodesBatchBytesIfCacheDisabled(): Unit = {
    // Set up mocks and dummy data
    val servingInfo = mock[GroupByServingInfoParsed]
    val batchBytes = Array[Byte](1, 2, 3)
    val keys = Map("key" -> "value")
    val finalBatchIr = mock[FinalBatchIr]
    val kvStoreBatchResponses = new BatchResponses(Success(Seq(TimedValue(batchBytes, 1000L))))

    val spiedBaseFetcher = Mockito.spy(new TestableBaseFetcher(mock[KVStore], None))
    when(spiedBaseFetcher.toBatchIr(any(), any())).thenReturn(finalBatchIr)

    // When getBatchIrFromBatchResponse is called, it decodes the bytes and doesn't hit the cache
    val ir = spiedBaseFetcher.getBatchIrFromBatchResponse(kvStoreBatchResponses, batchBytes, servingInfo, keys)
    verify(spiedBaseFetcher, times(1)).toBatchIr(batchBytes, servingInfo) // decoding did happen
    assertEquals(finalBatchIr, ir)
  }

  @Test
  def test_getBatchIrFromBatchResponse_ReturnsCorrectMapResponseWithCacheEnabled(): Unit = {
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

    val spiedBaseFetcher = Mockito.spy(new TestableBaseFetcher(mock[KVStore], Some(batchIrCache)))

    // 1. Cached BatchResponse returns the same Map responses passed in
    val mapResponse1 = mock[Map[String, AnyRef]]
    val cachedBatchResponse = new BatchResponses(mapResponse1)
    val decodingFunction1 = (bytes: Array[Byte]) => {
      fail("Decoding function should not be called when batch response is cached")
      mapResponse1
    }
    val cachedMapResponse = spiedBaseFetcher.getMapResponseFromBatchResponse(cachedBatchResponse,
                                                                             batchBytes,
                                                                             decodingFunction1,
                                                                             servingInfo,
                                                                             keys)
    assertEquals(mapResponse1, cachedMapResponse)

    // 2. Un-cached BatchResponse has Map responses added to cache
    val mapResponse2 = mock[Map[String, AnyRef]]
    val kvStoreBatchResponses = new BatchResponses(Success(Seq(TimedValue(batchBytes, 1000L))))
    def decodingFunction2 = (bytes: Array[Byte]) => mapResponse2
    val decodedMapResponse = spiedBaseFetcher.getMapResponseFromBatchResponse(kvStoreBatchResponses,
                                                                              batchBytes,
                                                                              decodingFunction2,
                                                                              servingInfo,
                                                                              keys)
    assertEquals(mapResponse2, decodedMapResponse)
    assertEquals(batchIrCache.cache.getIfPresent(cacheKey).right.get, mapResponse2) // key was added
  }

  @Test
  def test_getMapResponseFromBatchResponse_DecodesBatchBytesIfCacheDisabled(): Unit = {
    // Set up mocks and dummy data
    val servingInfo = mock[GroupByServingInfoParsed]
    val batchBytes = Array[Byte](1, 2, 3)
    val keys = Map("key" -> "value")
    val mapResponse = mock[Map[String, AnyRef]]
    val outputCodec = mock[AvroCodec]
    val kvStoreBatchResponses = new BatchResponses(Success(Seq(TimedValue(batchBytes, 1000L))))
    when(servingInfo.outputCodec).thenReturn(outputCodec)
    when(outputCodec.decodeMap(any())).thenReturn(mapResponse)

    val spiedBaseFetcher = Mockito.spy(new TestableBaseFetcher(mock[KVStore], None))

    // When getMapResponseFromBatchResponse is called, it decodes the bytes and doesn't hit the cache
    val decodedMapResponse = spiedBaseFetcher.getMapResponseFromBatchResponse(kvStoreBatchResponses,
                                                                              batchBytes,
                                                                              servingInfo.outputCodec.decodeMap,
                                                                              servingInfo,
                                                                              keys)
    verify(servingInfo.outputCodec.decodeMap(any()), times(1)) // decoding did happen
    assertEquals(mapResponse, decodedMapResponse)
  }

  @Test
  def test_getServingInfo_ShouldCallUpdateServingInfoIfBatchResponseIsFromKvStore(): Unit = {
    val baseFetcher = new BaseFetcher(mock[KVStore])
    val spiedBaseFetcher = spy(baseFetcher)
    val oldServingInfo = mock[GroupByServingInfoParsed]
    val updatedServingInfo = mock[GroupByServingInfoParsed]
    val batchTimedValuesSuccess = Success(Seq(TimedValue(Array(1.toByte), 2000L)))
    val kvStoreBatchResponses = new BatchResponses(batchTimedValuesSuccess)
    doReturn(updatedServingInfo).when(spiedBaseFetcher).updateServingInfo(any(), any())

    // updateServingInfo is called
    val result = spiedBaseFetcher.getServingInfo(oldServingInfo, kvStoreBatchResponses)
    assertSame(result, updatedServingInfo)
    verify(spiedBaseFetcher).updateServingInfo(any(), any())
  }

  @Test
  def test_getServingInfo_ShouldRefreshServingInfoIfBatchResponseIsCached(): Unit = {
    val baseFetcher = new BaseFetcher(mock[KVStore])
    val spiedBaseFetcher = spy(baseFetcher)
    val oldServingInfo = mock[GroupByServingInfoParsed]
    val metaData = mock[MetaData]
    val groupByOpsMock = mock[GroupByOps]
    val cachedBatchResponses = new BatchResponses(mock[FinalBatchIr])
    val ttlCache = mock[TTLCache[String, Try[GroupByServingInfoParsed]]]
    doReturn(ttlCache).when(spiedBaseFetcher).getGroupByServingInfo
    doReturn(Success(oldServingInfo)).when(ttlCache).refresh(any[String])
    metaData.name = "test"
    groupByOpsMock.metaData = metaData
    when(oldServingInfo.groupByOps).thenReturn(groupByOpsMock)

    // BaseFetcher.updateServingInfo is not called, but getGroupByServingInfo.refresh() is.
    val result = spiedBaseFetcher.getServingInfo(oldServingInfo, cachedBatchResponses)
    assertSame(result, oldServingInfo)
    verify(ttlCache).refresh(any())
    verify(spiedBaseFetcher, never()).updateServingInfo(any(), any())
  }

}
