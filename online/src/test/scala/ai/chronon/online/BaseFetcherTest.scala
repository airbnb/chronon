package ai.chronon.online

import ai.chronon.api.KeyMissingException
import ai.chronon.online.Fetcher.{Request, Response}
import org.junit.{Before, Test}
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito._
import org.mockito.{Answers, ArgumentCaptor}
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.mockito.MockitoSugar

import scala.concurrent.duration.DurationInt
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.util.{Failure, Success}

class BaseFetcherTest extends MockitoSugar with Matchers {
  val TestColumn = "relevance.short_term_user_features.pdp_view_count_14d"
  val GuestKey = "guest"
  val HostKey = "host"
  val GuestId = "123"
  val HostId = "456"
  var baseFetcher: BaseFetcher = _
  var kvStore: KVStore = _

  @Before
  def setup(): Unit = {
    kvStore = mock[KVStore](Answers.RETURNS_DEEP_STUBS)
    // The KVStore execution context is implicitly used for
    // Future compositions in the Fetcher so provision it in
    // the mock to prevent hanging.
    when(kvStore.executionContext).thenReturn(ExecutionContext.global)
    baseFetcher = spy(new BaseFetcher(kvStore))
  }

  @Test
  def testFetchColumnGroup_SingleQuery(): Unit = {
    // Fetch a single query
    val keyMap = Map(GuestKey -> GuestId)
    val query = ("prefix", TestColumn)
    val keySet = Set(GuestKey)
    val queryMap = Map(query -> keySet)

    doAnswer(invocation => {
      val requests = invocation.getArgument(0).asInstanceOf[Seq[Request]]
      val request = requests.head
      val response = Response(request, Success(Map(request.name -> "100")))
      Future.successful(Seq(response))
    }).when(baseFetcher).fetchGroupBys(any())

    // Map should contain query with valid response
    val queryResults = Await.result(baseFetcher.fetchColumnGroup(keyMap, queryMap), 1.second)
    queryResults.contains(query) shouldBe true
    queryResults.get(query).map(_.values) shouldBe Some(Success(Map(query._1 + "_" + query._2 -> "100")))

    // GroupBy request sent to KV store for the query
    val requestsCaptor = ArgumentCaptor.forClass(classOf[Seq[_]])
    verify(baseFetcher, times(1)).fetchGroupBys(requestsCaptor.capture().asInstanceOf[Seq[Request]])
    val actualRequest = requestsCaptor.getValue.asInstanceOf[Seq[Request]].headOption
    actualRequest shouldNot be(None)
    actualRequest.get.name shouldBe query._2
    actualRequest.get.keys shouldBe keyMap
  }

  @Test
  def testFetchColumnGroup_Batch(): Unit = {
    // Fetch host and guest query with all keys present
    val keyMap = Map(GuestKey -> GuestId, HostKey -> HostId)
    val guestQuery = (GuestKey, TestColumn)
    val guestKeySet = Set(GuestKey)
    val hostQuery = (HostKey, TestColumn)
    val hostKeySet = Set(HostKey)
    val queryMap = Map(
      guestQuery -> guestKeySet,
      hostQuery -> hostKeySet,
    )

    doAnswer(invocation => {
      val requests = invocation.getArgument(0).asInstanceOf[Seq[Request]]
      val responses = requests.map(r => Response(r, Success(Map(r.name -> "100"))))
      Future.successful(responses)
    }).when(baseFetcher).fetchGroupBys(any())

    // Result map should contain all queries with results
    val queryResults = Await.result(baseFetcher.fetchColumnGroup(keyMap, queryMap), 1.second)
    queryResults.contains(guestQuery) shouldBe true
    queryResults.get(guestQuery).map(_.values) shouldBe Some(Success(Map(guestQuery._1 + "_" + guestQuery._2 -> "100")))
    queryResults.contains(hostQuery) shouldBe true
    queryResults.get(hostQuery).map(_.values) shouldBe Some(Success(Map(hostQuery._1 + "_" + hostQuery._2 -> "100")))

    // Both queries sent to KV store for GroupBy fetch
    val requestsCaptor = ArgumentCaptor.forClass(classOf[Seq[_]])
    verify(baseFetcher, times(1)).fetchGroupBys(requestsCaptor.capture().asInstanceOf[Seq[Request]])
    val actualRequests = requestsCaptor.getValue.asInstanceOf[Seq[Request]]
    actualRequests.length shouldBe 2
    actualRequests.head.name shouldBe guestQuery._2
    actualRequests.head.keys shouldBe Map(GuestKey -> GuestId)
    actualRequests(1).name shouldBe hostQuery._2
    actualRequests(1).keys shouldBe Map(HostKey -> HostId)
  }

  @Test
  def testFetchColumnGroup_MissingKey(): Unit = {
    // Host key requested but not provided
    val keyMap = Map(GuestKey -> GuestId)
    val guestQuery = (GuestKey, TestColumn)
    val guestKeySet = Set(GuestKey)
    val hostQuery = (HostKey, TestColumn)
    val hostKeySet = Set(HostKey)
    val queryMap = Map(
      guestQuery -> guestKeySet,
      hostQuery -> hostKeySet,
    )

    doAnswer(invocation => {
      val requests = invocation.getArgument(0).asInstanceOf[Seq[Request]]
      val responses = requests.map(r => Response(r, Success(Map(r.name -> "100"))))
      Future.successful(responses)
    }).when(baseFetcher).fetchGroupBys(any())

    // Result map should contain all queries, but host query fails with missing key
    val queryResults = Await.result(baseFetcher.fetchColumnGroup(keyMap, queryMap), 1.second)
    queryResults.contains(guestQuery) shouldBe true
    queryResults.get(guestQuery).map(_.values) shouldBe Some(Success(Map(guestQuery._1 + "_" + guestQuery._2 -> "100")))
    queryResults.contains(hostQuery) shouldBe true
    queryResults.get(hostQuery).map(_.values) shouldBe Some(Failure(KeyMissingException(hostQuery._1 + "_" + hostQuery._2, Set(HostKey).toSeq, Map())))

    // Only the guest query is sent to KV store for GroupBy fetch
    val requestsCaptor = ArgumentCaptor.forClass(classOf[Seq[_]])
    verify(baseFetcher, times(1)).fetchGroupBys(requestsCaptor.capture().asInstanceOf[Seq[Request]])
    val actualRequests = requestsCaptor.getValue.asInstanceOf[Seq[Request]]
    actualRequests.length shouldBe 1
    actualRequests.head.name shouldBe guestQuery._2
    actualRequests.head.keys shouldBe Map(GuestKey -> GuestId)
  }
}
