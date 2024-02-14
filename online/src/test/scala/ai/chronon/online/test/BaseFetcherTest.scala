package ai.chronon.online.test

import ai.chronon.aggregator.windowing.FinalBatchIr
import ai.chronon.api.Extensions.GroupByOps
import ai.chronon.api.MetaData
import ai.chronon.online.BaseFetcher._
import ai.chronon.online.FetcherCache.BatchResponses
import ai.chronon.online.{BaseFetcher, GroupByServingInfoParsed, KVStore, TTLCache}
import ai.chronon.online.KVStore.TimedValue
import org.junit.Assert.assertSame
import org.junit.Test
import org.mockito.Mockito._
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito
import org.mockito.stubbing.Stubber
import org.scalatestplus.mockito.MockitoSugar

import java.util.function.BiPredicate
import scala.util.{Success, Try}

class BaseFetcherTest extends MockitoHelper {
  @Test
  def test_getServingInfo_ShouldCallUpdateServingInfoIfBatchResponseIsFromKvStore(): Unit = {
    val baseFetcher = new BaseFetcher(mock[KVStore], mock[BiPredicate[String, java.util.Map[String, String]]])
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
    val baseFetcher = new BaseFetcher(mock[KVStore], mock[BiPredicate[String, java.util.Map[String, String]]])
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
}
