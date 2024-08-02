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
import ai.chronon.api.Extensions.GroupByOps
import ai.chronon.api.{Builders, GroupBy, MetaData}
import ai.chronon.online.Fetcher.{ColumnSpec, Request, Response}
import ai.chronon.online.FetcherCache.BatchResponses
import ai.chronon.online.KVStore.TimedValue
import org.junit.Assert.{assertFalse, assertTrue, fail}
import org.junit.{Before, Test}
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito._
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer
import org.mockito.{Answers, ArgumentCaptor}
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.mockito.MockitoSugar

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

    val result = fetcherBase.getServingInfo(oldServingInfo, kvStoreBatchResponses)

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

    val cachedBatchResponses = BatchResponses(mock[FinalBatchIr])
    val result = fetcherBase.getServingInfo(oldServingInfo, cachedBatchResponses)

    // FetcherBase.updateServingInfo is not called, but getGroupByServingInfo.refresh() is.
    result shouldEqual oldServingInfo
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
}
