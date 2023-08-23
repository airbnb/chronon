package ai.chronon.online

import ai.chronon.online.Fetcher.{ColumnSpec, Request, Response}
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

class FetcherBaseTest extends MockitoSugar with Matchers {
  val GroupBy = "relevance.short_term_user_features"
  val Column = "pdp_view_count_14d"
  val GuestKey = "guest"
  val HostKey = "host"
  val GuestId = "123"
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
  def testFetchColumns_SingleQuery(): Unit = {
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
  def testFetchColumns_Batch(): Unit = {
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
  def testFetchColumns_MissingResponse(): Unit = {
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
      case _ => fail()
    }

    // GroupBy request sent to KV store for the query
    val requestsCaptor = ArgumentCaptor.forClass(classOf[Seq[_]])
    verify(fetcherBase, times(1)).fetchGroupBys(requestsCaptor.capture().asInstanceOf[Seq[Request]])
    val actualRequest = requestsCaptor.getValue.asInstanceOf[Seq[Request]].headOption
    actualRequest shouldNot be(None)
    actualRequest.get.name shouldBe query.groupByName + "." + query.columnName
    actualRequest.get.keys shouldBe query.keyMapping.get
  }
}
