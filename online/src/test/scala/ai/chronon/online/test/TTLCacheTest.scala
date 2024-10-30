package ai.chronon.online.test

import ai.chronon.online.{Metrics, TTLCache}

import scala.collection.mutable
import org.junit.{Before, Test}

import java.util.concurrent.{AbstractExecutorService, ExecutorService, SynchronousQueue, ThreadPoolExecutor, TimeUnit}
import scala.util.{Failure, Success, Try}

class TTLCacheTest {

  class MockTime(var currentTime: Long) {

    def getTime: Long = currentTime

    def setTime(time: Long): Unit = {
      currentTime = time
    }
  }

  // Creates an executor that blocks and runs within the current thread.
  def currentThreadExecutorService(): AbstractExecutorService = {
    val callerRunsPolicy = new ThreadPoolExecutor.CallerRunsPolicy()

    new ThreadPoolExecutor(0, 1, 0L, TimeUnit.SECONDS, new SynchronousQueue[Runnable](), callerRunsPolicy) {
      override def execute(command: Runnable): Unit = {
        callerRunsPolicy.rejectedExecution(command, this)
      }
    }
  }

  var mockTime: MockTime = _
  val nowFunc: () => Long = () => { mockTime.getTime}
  val currentThreadExecutor: AbstractExecutorService = currentThreadExecutorService()
  var ttlCache: TTLCache[String, Try[String]] = _
  var fetchData: mutable.Map[String, Try[String]] = _
  var fetchTimes: mutable.Map[String, Long] = _

  @Before
  def setupTTLCache(): Unit = {
    mockTime = new MockTime(0L)
    fetchData = mutable.Map[String, Try[String]]()
    fetchTimes = mutable.Map[String, Long]()
    TTLCache.setExecutor(currentThreadExecutor)

    ttlCache = new TTLCache[String, Try[String]](
      {
        key => {
          fetchTimes(key) = nowFunc()
          fetchData(key)
        }
      },
      {_ => Metrics.Context(environment = "test")},
      100, // Normal timeout interval.
      nowFunc,
      20,  // Refresh timeout interval.
      10, // Failure timeout interval.
    )
  }

  @Test
  def testValidEntry(): Unit = {
    // Test apply.
    fetchData("valid_1") = Success("success_1") // valid_1 enters cache at t = 0
    assert(ttlCache.apply("valid_1").get == "success_1") // key is correct.
    assert(fetchTimes("valid_1") == 0L) // We actively fetch.
    mockTime.setTime(1L) // Set to 1 ms.
    assert(ttlCache.apply("valid_1").get == "success_1") // key is correct.
    assert(fetchTimes("valid_1") == 0L) // We don't fetch.
    mockTime.setTime(200L) // Expire the key.
    assert(ttlCache.apply("valid_1").get == "success_1") // key is correct.
    assert(fetchTimes("valid_1") == 200L) // We actively fetch.

    // Test refresh.
    mockTime.setTime(230L)
    fetchData("valid_1") = Success("success_2")
    assert(ttlCache.refresh("valid_1").get == "success_1") // Get old value.
    assert(fetchTimes("valid_1") == 230L) // We actively fetch.
    assert(ttlCache.apply("valid_1").get == "success_2") // Value is replaced.

    // Test force.
    mockTime.setTime(231L)
    fetchData("valid_1") = Success("success_3")
    assert(ttlCache.force("valid_1").get == "success_2") // Get old value.
    assert(fetchTimes("valid_1") == 231L) // We actively fetch.
    assert(ttlCache.apply("valid_1").get == "success_3") // Value is replaced.
  }

  @Test
  def testFailureEntry(): Unit = {
    // invalid_1 enters cache at t = 0
    fetchData("invalid_1") = Failure(new Exception("test_exception_1"))
    assert(ttlCache.apply("invalid_1").isFailure) // key is correct.
    assert(fetchTimes("invalid_1") == 0L) // We actively fetch.
    mockTime.setTime(20L) // Expire the key.
    assert(ttlCache.apply("invalid_1").isFailure)
    assert(fetchTimes("invalid_1") == 20L) // We actively fetch.

    // Test refresh.
    mockTime.setTime(31L) // This is under the refresh interval but we should still fetch.
    assert(ttlCache.refresh("invalid_1").isFailure)
    assert(fetchTimes("invalid_1") == 31L)

    // Test force.
    mockTime.setTime(32L) // Under force, we should always refetch.
    assert(ttlCache.force("invalid_1").isFailure)
    assert(fetchTimes("invalid_1") == 32L)
  }

  @Test
  def testFailureRefreshes(): Unit = {
    ttlCache = new TTLCache[String, Try[String]](
      {
        key => {
          fetchTimes(key) = nowFunc()
          fetchData(key)
        }
      },
      {_ => Metrics.Context(environment = "test")},
      100, // Normal timeout interval.
      nowFunc,
      20,  // Refresh timeout interval (less than failure timeout in this case).
      50, // Failure timeout interval.
    )

    fetchData("invalid_1") = Failure(new Exception("test_exception_1"))
    assert(ttlCache.apply("invalid_1").isFailure) // key is correct.
    assert(fetchTimes("invalid_1") == 0L) // We actively fetch.

    mockTime.setTime(21L) // Hits refresh but not failure timeout interval.
    fetchData("invalid_1") = Success("success_1")
    assert(ttlCache.refresh("invalid_1").isFailure) // Return the old value.
    assert(ttlCache.apply("invalid_1").get == "success_1")
    assert(fetchTimes("invalid_1") == 21L)
  }

}
