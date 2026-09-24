package ai.chronon.online

import ai.chronon.online.ApiSerializationCheck.ApiNotExecutorSafeException
import org.junit.Assert.{assertEquals, assertTrue, fail}
import org.junit.Test
import org.slf4j.{Logger, LoggerFactory}

import java.util.concurrent.ConcurrentHashMap
import scala.concurrent.Future

object ApiSerializationCheckTest {
  class NoopKvStore extends KVStore with Serializable {
    override def create(dataset: String): Unit = {}
    override def multiGet(requests: collection.Seq[KVStore.GetRequest]): Future[collection.Seq[KVStore.GetResponse]] =
      Future.successful(Seq.empty)
    override def multiPut(putRequests: collection.Seq[KVStore.PutRequest]): Future[collection.Seq[Boolean]] =
      Future.successful(putRequests.map(_ => true))
    override def bulkPut(sourceOfflineTable: String, destinationOnlineDataSet: String, partition: String): Unit = {}
  }

  abstract class BaseApi extends Api(Map.empty) {
    override def streamDecoder(groupByServingInfoParsed: GroupByServingInfoParsed): StreamDecoder = null
    override def genKvStore: KVStore = new NoopKvStore
    override def externalRegistry: ExternalSourceRegistry = new ExternalSourceRegistry
    override def logResponse(resp: LoggableResponse): Unit = {}
  }

  /** The shape that caused the Sept 2026 push_write_notification outage. */
  class BrokenApi extends BaseApi {
    @transient val logger: Logger = LoggerFactory.getLogger("BrokenApi")
    @transient private val producers = new ConcurrentHashMap[String, String]()
    def producerCount: Int = producers.size()
  }

  /** The fixed shape: lazy transient fields re-initialize on the executor. */
  class SafeApi extends BaseApi {
    @transient lazy val logger: Logger = LoggerFactory.getLogger("SafeApi")
    @transient private lazy val producers = new ConcurrentHashMap[String, String]()
    def producerCount: Int = producers.size()
  }

  /** A transient var the implementation re-creates on first use; must be allow-listed. */
  class LazyVarApi extends BaseApi {
    @transient private var cache: ConcurrentHashMap[String, String] = new ConcurrentHashMap[String, String]()
    def cacheOrInit: ConcurrentHashMap[String, String] = {
      if (cache == null) cache = new ConcurrentHashMap[String, String]()
      cache
    }
  }
}

class ApiSerializationCheckTest {
  import ApiSerializationCheckTest._

  @Test
  def testFlagsNonLazyTransientVals(): Unit = {
    val violations = ApiSerializationCheck.findViolations(new BrokenApi)
    val names = violations.map(_.field).toSet
    assertEquals(Set("logger", "producers"), names)
  }

  @Test
  def testVerifyThrowsWithFieldNames(): Unit = {
    try {
      ApiSerializationCheck.verify(new BrokenApi)
      fail("expected ApiNotExecutorSafeException")
    } catch {
      case e: ApiNotExecutorSafeException =>
        assertTrue(e.getMessage, e.getMessage.contains("logger"))
        assertTrue(e.getMessage, e.getMessage.contains("producers"))
        assertTrue(e.getMessage, e.getMessage.contains("@transient lazy val"))
    }
  }

  @Test
  def testLazyTransientValsPass(): Unit = {
    val api = new SafeApi
    // Touch the lazy fields on the "driver" so they are non-null before serialization, which is
    // the case that must not be reported as a violation.
    api.logger.debug("init")
    api.producerCount
    assertEquals(Seq.empty, ApiSerializationCheck.findViolations(api))
    ApiSerializationCheck.verify(api)
    val copy = ApiSerializationCheck.roundTrip(api)
    assertTrue(copy.logger != null)
    assertEquals(0, copy.producerCount)
  }

  @Test
  def testAllowListSuppressesKnownFields(): Unit = {
    val api = new LazyVarApi
    assertEquals(Seq("cache"), ApiSerializationCheck.findViolations(api).map(_.field))
    ApiSerializationCheck.verify(api, allowNullFields = Set("cache"))
    assertTrue(ApiSerializationCheck.roundTrip(api).cacheOrInit != null)
  }
}
