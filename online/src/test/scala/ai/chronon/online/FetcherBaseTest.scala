/*
 *    Copyright (C) 2023 The Chronon Authors.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package ai.chronon.online

import ai.chronon.aggregator.windowing.FinalBatchIr
import ai.chronon.api.Extensions.{GroupByOps, WindowOps}
import ai.chronon.api.{Builders, GroupBy, GroupByServingInfo, MetaData, PartitionSpec, TimeUnit, Window}
import ai.chronon.online.Fetcher.{ColumnSpec, Request, Response}
import ai.chronon.online.FetcherCache.BatchResponses
import ai.chronon.online.KVStore.TimedValue
import org.junit.Assert.{assertFalse, assertTrue, assertSame, fail}
import org.junit.{Before, Test}
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito._
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer
import org.mockito.{Answers, ArgumentCaptor}
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.mockito.MockitoSugar

import java.util.concurrent.atomic.{AtomicLong, AtomicReference}
import scala.concurrent.duration.DurationInt
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.util.{Failure, Success}
import scala.util.Try

class FetcherBaseTest extends MockitoSugar with Matchers with MockitoHelper {
  val GroupBy = "relevance.short_term_user_features"
  val Column = "pdp_view_count_14d"
  val GuestKey = "guest"
  val HostKey = "host"
  val GuestId: AnyRef = 123.asInstanceOf[AnyRef]
  val HostId = "456"
  var fetcherBase: FetcherBase = _
  var kvStore: KVStore = _

  @Before
  def setup(): Unit = {
    kvStore = mock[KVStore](Answers.RETURNS_DEEP_STUBS)
    // The KVStore execution context is implicitly used for
    // Future compositions in the Fetcher so provision it in
    // the mock to prevent hanging.
    when(kvStore.executionContext).thenReturn(ExecutionContext.global)
    fetcherBase = spy(new FetcherBase(kvStore))
  }

  @Test
  def testFetchColumnsSingleQuery(): Unit = {
    // Fetch a single query
    val keyMap = Map(GuestKey -> GuestId)
    val query = ColumnSpec(GroupBy, Column, None, Some(keyMap))

    doAnswer(new Answer[Future[Seq[Fetcher.Response]]] {
      def answer(invocation: InvocationOnMock): Future[Seq[Response]] = {
        val requests = invocation.getArgument(0).asInstanceOf[Seq[Request]]
        val request = requests.head
        val response = Response(request, Success(Map(request.name -> "100")))
        Future.successful(Seq(response))
      }
    }).when(fetcherBase).fetchGroupBys(any())

    // Map should contain query with valid response
    val queryResults = Await.result(fetcherBase.fetchColumns(Seq(query)), 1.second)
    queryResults.contains(query) shouldBe true
    queryResults.get(query).map(_.values) shouldBe Some(Success(Map(s"$GroupBy.$Column" -> "100")))

    // GroupBy request sent to KV store for the query
    val requestsCaptor = ArgumentCaptor.forClass(classOf[Seq[_]])
    verify(fetcherBase, times(1)).fetchGroupBys(requestsCaptor.capture().asInstanceOf[Seq[Request]])
    val actualRequest = requestsCaptor.getValue.asInstanceOf[Seq[Request]].headOption
    actualRequest shouldNot be(None)
    actualRequest.get.name shouldBe s"${query.groupByName}.${query.columnName}"
    actualRequest.get.keys shouldBe query.keyMapping.get
  }

  @Test
  def testFetchColumnsBatch(): Unit = {
    // Fetch a batch of queries
    val guestKeyMap = Map(GuestKey -> GuestId)
    val guestQuery = ColumnSpec(GroupBy, Column, Some(GuestKey), Some(guestKeyMap))
    val hostKeyMap = Map(HostKey -> HostId)
    val hostQuery = ColumnSpec(GroupBy, Column, Some(HostKey), Some(hostKeyMap))

    doAnswer(new Answer[Future[Seq[Fetcher.Response]]] {
      def answer(invocation: InvocationOnMock): Future[Seq[Response]] = {
        val requests = invocation.getArgument(0).asInstanceOf[Seq[Request]]
        val responses = requests.map(r => Response(r, Success(Map(r.name -> "100"))))
        Future.successful(responses)
      }
    }).when(fetcherBase).fetchGroupBys(any())

    // Map should contain query with valid response
    val queryResults = Await.result(fetcherBase.fetchColumns(Seq(guestQuery, hostQuery)), 1.second)
    queryResults.contains(guestQuery) shouldBe true
    queryResults.get(guestQuery).map(_.values) shouldBe Some(Success(Map(s"${GuestKey}_$GroupBy.$Column" -> "100")))
    queryResults.contains(hostQuery) shouldBe true
    queryResults.get(hostQuery).map(_.values) shouldBe Some(Success(Map(s"${HostKey}_$GroupBy.$Column" -> "100")))

    // GroupBy request sent to KV store for the query
    val requestsCaptor = ArgumentCaptor.forClass(classOf[Seq[_]])
    verify(fetcherBase, times(1)).fetchGroupBys(requestsCaptor.capture().asInstanceOf[Seq[Request]])
    val actualRequests = requestsCaptor.getValue.asInstanceOf[Seq[Request]]
    actualRequests.length shouldBe 2
    actualRequests.head.name shouldBe s"${guestQuery.groupByName}.${guestQuery.columnName}"
    actualRequests.head.keys shouldBe guestQuery.keyMapping.get
    actualRequests(1).name shouldBe s"${hostQuery.groupByName}.${hostQuery.columnName}"
    actualRequests(1).keys shouldBe hostQuery.keyMapping.get
  }

  @Test
  def testFetchColumnsMissingResponse(): Unit = {
    // Fetch a single query
    val keyMap = Map(GuestKey -> GuestId)
    val query = ColumnSpec(GroupBy, Column, None, Some(keyMap))

    doAnswer(new Answer[Future[Seq[Fetcher.Response]]] {
      def answer(invocation: InvocationOnMock): Future[Seq[Response]] = {
        Future.successful(Seq())
      }
    }).when(fetcherBase).fetchGroupBys(any())

    // Map should contain query with Failure response
    val queryResults = Await.result(fetcherBase.fetchColumns(Seq(query)), 1.second)
    queryResults.contains(query) shouldBe true
    queryResults.get(query).map(_.values) match {
      case Some(Failure(ex: IllegalStateException)) => succeed
      case _                                        => fail()
    }

    // GroupBy request sent to KV store for the query
    val requestsCaptor = ArgumentCaptor.forClass(classOf[Seq[_]])
    verify(fetcherBase, times(1)).fetchGroupBys(requestsCaptor.capture().asInstanceOf[Seq[Request]])
    val actualRequest = requestsCaptor.getValue.asInstanceOf[Seq[Request]].headOption
    actualRequest shouldNot be(None)
    actualRequest.get.name shouldBe query.groupByName + "." + query.columnName
    actualRequest.get.keys shouldBe query.keyMapping.get
  }

  // updateServingInfo() is called when the batch response is from the KV store.
  @Test
  def testGetServingInfoShouldCallUpdateServingInfoIfBatchResponseIsFromKvStore(): Unit = {
    val oldServingInfo = mock[GroupByServingInfoParsed]
    val updatedServingInfo = mock[GroupByServingInfoParsed]
    doReturn(updatedServingInfo).when(fetcherBase).updateServingInfo(any(), any())

    val batchTimedValuesSuccess = Success(Seq(TimedValue(Array(1.toByte), 2000L)))
    val kvStoreBatchResponses = BatchResponses(batchTimedValuesSuccess)

    val (result, _) = fetcherBase.getServingInfo(oldServingInfo, kvStoreBatchResponses)

    // updateServingInfo is called
    result shouldEqual updatedServingInfo
    verify(fetcherBase).updateServingInfo(any(), any())
  }

  // If a batch response is cached, the serving info should be refreshed. This is needed to prevent
  // the serving info from becoming stale if all the requests are cached.
  @Test
  def testGetServingInfoShouldRefreshServingInfoIfBatchResponseIsCached(): Unit = {
    val ttlCache = mock[TTLCache[String, Try[GroupByServingInfoParsed]]]
    doReturn(ttlCache).when(fetcherBase).getGroupByServingInfo

    val oldServingInfo = mock[GroupByServingInfoParsed]
    doReturn(Success(oldServingInfo)).when(ttlCache).refresh(any[String])

    val metaDataMock = mock[MetaData]
    val groupByOpsMock = mock[GroupByOps]
    metaDataMock.name = "test"
    groupByOpsMock.metaData = metaDataMock
    doReturn(groupByOpsMock).when(oldServingInfo).groupByOps

    val cachedBatchResponses = BatchResponses(mock[FinalBatchIr], 1000L)
    val (result, batchEndTs) = fetcherBase.getServingInfo(oldServingInfo, cachedBatchResponses)

    // FetcherBase.updateServingInfo is not called, but getGroupByServingInfo.refresh() is.
    result shouldEqual oldServingInfo
    // The batch end of the cached IR is what the response is served as of.
    batchEndTs shouldBe Some(1000L)
    verify(ttlCache).refresh(any())
    verify(fetcherBase, never()).updateServingInfo(any(), any())
  }

  @Test
  def testIsCachingEnabledCorrectlyDetermineIfCacheIsEnabled(): Unit = {
    val flagStore: FlagStore = (flagName: String, attributes: java.util.Map[String, String]) => {
      flagName match {
        case "enable_fetcher_batch_ir_cache" =>
          attributes.get("groupby_streaming_dataset") match {
            case "test_groupby_2" => false
            case "test_groupby_3" => true
            case other @ _ =>
              fail(s"Unexpected groupby_streaming_dataset: $other")
              false
          }
        case _ => false
      }
    }

    kvStore = mock[KVStore](Answers.RETURNS_DEEP_STUBS)
    when(kvStore.executionContext).thenReturn(ExecutionContext.global)
    val fetcherBaseWithFlagStore = spy(new FetcherBase(kvStore, flagStore = flagStore))
    when(fetcherBaseWithFlagStore.isCacheSizeConfigured).thenReturn(true)

    def buildGroupByWithCustomJson(name: String): GroupBy = Builders.GroupBy(metaData = Builders.MetaData(name = name))

    // no name set
    assertFalse(fetcherBaseWithFlagStore.isCachingEnabled(Builders.GroupBy()))

    assertFalse(fetcherBaseWithFlagStore.isCachingEnabled(buildGroupByWithCustomJson("test_groupby_2")))
    assertTrue(fetcherBaseWithFlagStore.isCachingEnabled(buildGroupByWithCustomJson("test_groupby_3")))
  }

  @Test
  def testIsEntityValidityCheckEnabledFlag(): Unit = {
    val flagStore: FlagStore = (flagName: String, attributes: java.util.Map[String, String]) => {
      flagName match {
        case "enable_entity_validity_check" => true
        case _                              => false
      }
    }

    kvStore = mock[KVStore](Answers.RETURNS_DEEP_STUBS)
    val fetcherBaseWithFlagStore = spy(new FetcherBase(kvStore, flagStore = flagStore))

    assertTrue(fetcherBaseWithFlagStore.isEntityValidityCheckEnabled)
  }

  @Test
  def testCheckLateBatchDataShouldHandleBatchDataIsLate(): Unit = {
    val fetcherBase = new FetcherBase(mock[KVStore])

    // lookup request - 03/20/2024 01:00 UTC
    // batch landing time 03/17/2024 00:00 UTC
    val longWindows = Seq(new Window(7, TimeUnit.DAYS), new Window(10, TimeUnit.DAYS))
    val tailHops2d = new Window(2, TimeUnit.DAYS).millis
    val result = fetcherBase.checkLateBatchData(1710896400000L, "myGroupBy", 1710633600000L, tailHops2d, longWindows)
    assertSame(result, 1L)

    // try the same with a shorter lookback window
    val shortWindows = Seq(new Window(1, TimeUnit.DAYS), new Window(10, TimeUnit.HOURS))
    val result2 = fetcherBase.checkLateBatchData(1710896400000L, "myGroupBy", 1710633600000L, tailHops2d, shortWindows)
    assertSame(result2, 0L)
  }

  @Test
  def testCheckLateBatchDataShouldHandleBatchDataIsNotLate(): Unit = {
    val fetcherBase = new FetcherBase(mock[KVStore])

    // lookup request - 03/20/2024 01:00 UTC
    // batch landing time 03/19/2024 00:00 UTC
    val longWindows = Seq(new Window(7, TimeUnit.DAYS), new Window(10, TimeUnit.DAYS))
    val tailHops2d = new Window(2, TimeUnit.DAYS).millis
    val result = fetcherBase.checkLateBatchData(1710896400000L, "myGroupBy", 1710806400000L, tailHops2d, longWindows)
    assertSame(result, 0L)

    // try the same with a shorter lookback window
    val shortWindows = Seq(new Window(1, TimeUnit.DAYS), new Window(10, TimeUnit.HOURS))
    val result2 = fetcherBase.checkLateBatchData(1710896400000L, "myGroupBy", 1710633600000L, tailHops2d, shortWindows)
    assertSame(result2, 0L)
  }

  @Test
  def testParsingGroupByResponse_HappyCase(): Unit = {
    val baseFetcher = new FetcherBase(mock[KVStore])
    val request = Request(name = "name", keys = Map("email" -> "email"), atMillis = None, context = None)
    val response: Map[Request, Try[Map[String, AnyRef]]] = Map(
      request -> Success(
        Map(
          "key" -> "value"
        ))
    )

    val result = baseFetcher.parseGroupByResponse("prefix", request, response)
    result shouldBe Map("prefix_key" -> "value")
  }

  @Test
  def testParsingGroupByResponse_NullKey(): Unit = {
    val baseFetcher = new FetcherBase(mock[KVStore])
    val request = Request(name = "name", keys = Map("email" -> null), atMillis = None, context = None)
    val request2 = Request(name = "name2", keys = Map("email" -> null), atMillis = None, context = None)

    val response: Map[Request, Try[Map[String, AnyRef]]] = Map(
      request2 -> Success(
        Map(
          "key" -> "value"
        ))
    )

    val result = baseFetcher.parseGroupByResponse("prefix", request, response)
    result shouldBe Map()
  }

  @Test
  def testParsingGroupByResponse_MissingKey(): Unit = {
    val baseFetcher = new FetcherBase(mock[KVStore])
    val request = Request(name = "name", keys = Map("email" -> "email"), atMillis = None, context = None)
    val request2 = Request(name = "name2", keys = Map("email" -> "email"), atMillis = None, context = None)

    val response: Map[Request, Try[Map[String, AnyRef]]] = Map(
      request2 -> Success(
        Map(
          "key" -> "value"
        ))
    )

    val result = baseFetcher.parseGroupByResponse("prefix", request, response)
    result.keySet shouldBe Set("prefix_exception")
  }

  private val NewerBatchEndDate = "2026-11-02"
  private val OlderBatchEndDate = "2026-10-31"

  private def servingInfoWithBatchEnd(batchEndDate: String): GroupByServingInfoParsed = {
    val servingInfo = new GroupByServingInfo()
    servingInfo.setGroupBy(Builders.GroupBy(metaData = Builders.MetaData(name = GroupBy)))
    servingInfo.setBatchEndDate(batchEndDate)
    new GroupByServingInfoParsed(servingInfo, PartitionSpec(format = "yyyy-MM-dd", spanMillis = 24 * 60 * 60 * 1000L))
  }

  private def batchResponseAt(millis: Long): BatchResponses =
    BatchResponses(Success(Seq(TimedValue(Array(1.toByte), millis))))

  // An older batch upload replaced a newer one. The response is served as of the older batch end right away, and
  // the serving info is refreshed so the streaming scan start moves back too (without waiting for the TTL).
  @Test
  def testOlderBatchUploadIsServedAsOfItsOwnBatchEndAndRefreshesServingInfo(): Unit = {
    val newerServingInfo = servingInfoWithBatchEnd(NewerBatchEndDate)
    val olderServingInfo = servingInfoWithBatchEnd(OlderBatchEndDate)
    val servingInfoInKvStore = new AtomicReference[GroupByServingInfoParsed](newerServingInfo)
    val nowMillis = new AtomicLong(0L)
    val servingInfoCache = new TTLCache[String, Try[GroupByServingInfoParsed]](
      _ => Success(servingInfoInKvStore.get()),
      name => Metrics.Context(environment = "test", groupBy = name),
      nowFunc = () => nowMillis.get()
    )
    doReturn(servingInfoCache).when(fetcherBase).getGroupByServingInfo

    val cachedServingInfo = servingInfoCache(GroupBy).get
    servingInfoInKvStore.set(olderServingInfo)
    nowMillis.addAndGet(10 * 1000L)
    val olderBatchResponse = batchResponseAt(olderServingInfo.batchEndTsMillis)
    olderBatchResponse.getBatchBytes should not be null

    val (_, effectiveBatchEndTs) = fetcherBase.getServingInfo(cachedServingInfo, olderBatchResponse)
    effectiveBatchEndTs shouldBe Some(olderServingInfo.batchEndTsMillis)

    val deadline = System.currentTimeMillis() + 5000L
    var currentServingInfo = servingInfoCache(GroupBy).get
    while (
      currentServingInfo.batchEndTsMillis != olderServingInfo.batchEndTsMillis &&
      System.currentTimeMillis() < deadline
    ) {
      Thread.sleep(50)
      currentServingInfo = servingInfoCache(GroupBy).get
    }
    currentServingInfo.batchEndTsMillis shouldBe olderServingInfo.batchEndTsMillis
  }

  @Test
  def testStreamingRequestIsRealignedOnlyWhenBatchEndsBeforeTheScanStart(): Unit = {
    val streamingRequest = KVStore.GetRequest(Array(1.toByte), "GB_STREAMING", Some(2000L))
    // older batch end: scan has to start earlier
    fetcherBase.streamingRequestFrom(streamingRequest, 1000L) shouldBe Some(
      streamingRequest.copy(afterTsMillis = Some(1000L)))
    // same or newer batch end: the rows already fetched cover it
    fetcherBase.streamingRequestFrom(streamingRequest, 2000L) shouldBe None
    fetcherBase.streamingRequestFrom(streamingRequest, 3000L) shouldBe None
    // no scan start at all
    fetcherBase.streamingRequestFrom(streamingRequest.copy(afterTsMillis = None), 1000L) shouldBe None
  }

  @Test
  def testNewerBatchUploadRefreshesServingInfo(): Unit = {
    val ttlCache = mock[TTLCache[String, Try[GroupByServingInfoParsed]]]
    doReturn(ttlCache).when(fetcherBase).getGroupByServingInfo
    val cachedServingInfo = servingInfoWithBatchEnd(OlderBatchEndDate)
    doReturn(Success(cachedServingInfo)).when(ttlCache).refresh(any[String])

    fetcherBase.getServingInfo(cachedServingInfo,
                               batchResponseAt(servingInfoWithBatchEnd(NewerBatchEndDate).batchEndTsMillis))

    verify(ttlCache).refresh(GroupBy)
  }

  @Test
  def testSameBatchEndTsDoesNotRefreshServingInfo(): Unit = {
    val ttlCache = mock[TTLCache[String, Try[GroupByServingInfoParsed]]]
    doReturn(ttlCache).when(fetcherBase).getGroupByServingInfo
    val cachedServingInfo = servingInfoWithBatchEnd(NewerBatchEndDate)

    val (result, _) = fetcherBase.getServingInfo(cachedServingInfo, batchResponseAt(cachedServingInfo.batchEndTsMillis))

    result shouldBe cachedServingInfo
    verify(ttlCache, never()).refresh(any())
  }
}
