package ai.chronon.online

import ai.chronon.online.FetcherModelUtils.{InferenceParams, RetryPolicy}
import org.junit.Assert.{assertEquals, assertFalse, assertTrue}
import org.junit.Test

import java.util.concurrent.atomic.AtomicInteger
import scala.concurrent.duration.{Duration, SECONDS}
import scala.concurrent.{Await, ExecutionContext, Future}

class FetcherModelUtilsRetryTest {
  implicit val ec: ExecutionContext = ExecutionContext.global

  private def await[T](f: Future[T]): T = Await.result(f, Duration(10, SECONDS))

  @Test
  def defaultsPreserveHistoricalBehavior(): Unit = {
    val policy = RetryPolicy.fromParams(Map.empty)
    assertEquals(2, policy.maxRetries)
    assertEquals(0L, policy.delayMsFor(0))
    assertTrue(policy.isRetryable(new RuntimeException("anything")))
    assertEquals(0, FetcherModelUtils.inferenceChunkSize(ai.chronon.api.Builders.Model()))
  }

  @Test
  def parsesParamsAndIgnoresGarbage(): Unit = {
    val policy = RetryPolicy.fromParams(
      Map(
        InferenceParams.MaxRetries -> " 4 ",
        InferenceParams.BackoffMs -> "100",
        InferenceParams.NonRetryableExceptions -> "BackPressure, RateLimit ,,",
        InferenceParams.MaxBackoffMs -> "not-a-number"
      ))
    assertEquals(4, policy.maxRetries)
    assertEquals(100L, policy.backoffMs)
    assertEquals(1000L, policy.maxBackoffMs) // 10x backoff when the cap is unparseable
    assertEquals(Seq("BackPressure", "RateLimit"), policy.nonRetryablePatterns)
  }

  @Test
  def nonRetryableMatchesClassNameOrMessageAnywhereInCauseChain(): Unit = {
    val policy = RetryPolicy.fromParams(Map(InferenceParams.NonRetryableExceptions -> "BackPressure"))
    class ServiceBackPressureException extends RuntimeException("slow down")
    assertFalse(policy.isRetryable(new ServiceBackPressureException))
    assertFalse(policy.isRetryable(new RuntimeException("wrapped", new ServiceBackPressureException)))
    assertFalse(policy.isRetryable(new RuntimeException("upstream BackPressure response")))
    assertTrue(policy.isRetryable(new RuntimeException("timeout")))
  }

  @Test
  def backoffDoublesUpToCapWithJitter(): Unit = {
    val policy = RetryPolicy(maxRetries = 5, backoffMs = 100, maxBackoffMs = 350, nonRetryablePatterns = Seq.empty)
    def inRange(attempt: Int, base: Long): Unit = {
      val d = policy.delayMsFor(attempt)
      assertTrue(s"attempt $attempt delay $d below $base", d >= base)
      assertTrue(s"attempt $attempt delay $d above ${base * 1.2}", d <= base + math.max(1, base / 5))
    }
    inRange(0, 100)
    inRange(1, 200)
    inRange(2, 350)
    inRange(10, 350)
  }

  @Test
  def retriesUpToMaxThenFails(): Unit = {
    val attempts = new AtomicInteger()
    val policy = RetryPolicy.default.copy(maxRetries = 2)
    val result = FetcherModelUtils.withRetry(policy) {
      attempts.incrementAndGet()
      Future.failed(new RuntimeException("boom"))
    }
    val failure = Await.ready(result, Duration(10, SECONDS)).value.get
    assertTrue(failure.isFailure)
    assertEquals(3, attempts.get()) // first attempt + 2 retries
  }

  @Test
  def succeedsOnceARetrySucceeds(): Unit = {
    val attempts = new AtomicInteger()
    val result = FetcherModelUtils.withRetry(RetryPolicy.default.copy(maxRetries = 3)) {
      if (attempts.incrementAndGet() < 3) Future.failed(new RuntimeException("flaky")) else Future.successful("ok")
    }
    assertEquals("ok", await(result))
    assertEquals(3, attempts.get())
  }

  @Test
  def nonRetryableFailureIsNotRetried(): Unit = {
    val attempts = new AtomicInteger()
    val policy = RetryPolicy.fromParams(
      Map(InferenceParams.MaxRetries -> "5", InferenceParams.NonRetryableExceptions -> "BackPressure"))
    val result = FetcherModelUtils.withRetry(policy) {
      attempts.incrementAndGet()
      Future.failed(new RuntimeException("ServiceBackPressureException from backend"))
    }
    assertTrue(Await.ready(result, Duration(10, SECONDS)).value.get.isFailure)
    assertEquals(1, attempts.get())
  }

  @Test
  def backoffDelaysTheRetry(): Unit = {
    val attempts = new AtomicInteger()
    val policy = RetryPolicy(maxRetries = 1, backoffMs = 300, maxBackoffMs = 300, nonRetryablePatterns = Seq.empty)
    val start = System.currentTimeMillis()
    val result = FetcherModelUtils.withRetry(policy) {
      if (attempts.incrementAndGet() == 1) Future.failed(new RuntimeException("first")) else Future.successful(1)
    }
    assertEquals(1, await(result))
    val elapsed = System.currentTimeMillis() - start
    assertTrue(s"retry fired after only ${elapsed}ms", elapsed >= 300)
  }

  @Test
  def chunkSizeIsReadFromModelBackendParams(): Unit = {
    val model = ai.chronon.api.Builders.Model(
      inferenceSpec = ai.chronon.api.Builders
        .InferenceSpec(modelBackend = "manifold", modelBackendParams = Map(InferenceParams.ChunkSize -> "64")))
    assertEquals(64, FetcherModelUtils.inferenceChunkSize(model))
  }
}
