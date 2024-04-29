package ai.chronon.online.test

import ai.chronon.aggregator.windowing.FinalBatchIr
import ai.chronon.api.Extensions.GroupByOps
import ai.chronon.api.{MetaData, TimeUnit, Window}
import ai.chronon.online.FetcherCache.BatchResponses
import ai.chronon.api.Extensions.WindowOps
import ai.chronon.online.{BaseFetcher, GroupByServingInfoParsed, KVStore, TTLCache}
import ai.chronon.online.KVStore.TimedValue
import org.junit.Assert.assertSame
import org.junit.Test
import org.mockito.Mockito._
import org.mockito.ArgumentMatchers.any

import scala.util.{Success, Try}

class BaseFetcherTest extends MockitoHelper {
  @Test
  def test_getServingInfo_ShouldCallUpdateServingInfoIfBatchResponseIsFromKvStore(): Unit = {
    val baseFetcher = new BaseFetcher(mock[KVStore])
    val spiedBaseFetcher = spy(baseFetcher)
    val oldServingInfo = mock[GroupByServingInfoParsed]
    val updatedServingInfo = mock[GroupByServingInfoParsed]
    val batchTimedValuesSuccess = Success(Seq(TimedValue(Array(1.toByte), 2000L)))
    val kvStoreBatchResponses = BatchResponses(batchTimedValuesSuccess)
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
    val cachedBatchResponses = BatchResponses(mock[FinalBatchIr])
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

  @Test
  def test_checkLateBatchData_ShouldHandle_BatchDataIsLate(): Unit = {
    val baseFetcher = new BaseFetcher(mock[KVStore])

    // lookup request - 03/20/2024 01:00 UTC
    // batch landing time 03/17/2024 00:00 UTC
    val longWindows = Seq(new Window(7, TimeUnit.DAYS), new Window(10, TimeUnit.DAYS))
    val tailHops2d = new Window(2, TimeUnit.DAYS).millis
    val result = baseFetcher.checkLateBatchData(1710896400000L, "myGroupBy", 1710633600000L, tailHops2d, longWindows)
    assertSame(result, 1L)

    // try the same with a shorter lookback window
    val shortWindows = Seq(new Window(1, TimeUnit.DAYS), new Window(10, TimeUnit.HOURS))
    val result2 = baseFetcher.checkLateBatchData(1710896400000L, "myGroupBy", 1710633600000L, tailHops2d, shortWindows)
    assertSame(result2, 0L)
  }

  @Test
  def test_checkLateBatchData_ShouldHandle_BatchDataIsNotLate(): Unit = {
    val baseFetcher = new BaseFetcher(mock[KVStore])

    // lookup request - 03/20/2024 01:00 UTC
    // batch landing time 03/19/2024 00:00 UTC
    val longWindows = Seq(new Window(7, TimeUnit.DAYS), new Window(10, TimeUnit.DAYS))
    val tailHops2d = new Window(2, TimeUnit.DAYS).millis
    val result = baseFetcher.checkLateBatchData(1710896400000L, "myGroupBy", 1710806400000L, tailHops2d, longWindows)
    assertSame(result, 0L)

    // try the same with a shorter lookback window
    val shortWindows = Seq(new Window(1, TimeUnit.DAYS), new Window(10, TimeUnit.HOURS))
    val result2 = baseFetcher.checkLateBatchData(1710896400000L, "myGroupBy", 1710633600000L, tailHops2d, shortWindows)
    assertSame(result2, 0L)
  }
}
