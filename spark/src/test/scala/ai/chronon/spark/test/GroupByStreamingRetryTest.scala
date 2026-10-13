/*
 *    Copyright (C) 2026 The Chronon Authors.
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

package ai.chronon.spark.test

import ai.chronon.spark.Driver
import org.apache.spark.SparkException
import org.apache.spark.sql.execution.streaming.continuous.ContinuousTaskRetryException
import org.apache.spark.sql.streaming.{StreamingQuery, StreamingQueryException}
import org.junit.Assert._
import org.junit.Test
import org.mockito.ArgumentMatchers.anyLong
import org.mockito.Mockito._

class GroupByStreamingRetryTest {

  // StreamingQueryException's constructor is private[sql], so we can't `new` one directly from here;
  // mock it instead and stub getCause() to mirror how the failure actually surfaces at runtime:
  // ContinuousTaskRetryException wrapped in a SparkException, wrapped in the StreamingQueryException
  // that awaitTermination throws.
  private def exceptionWithCause(cause: Throwable): StreamingQueryException = {
    val e = mock(classOf[StreamingQueryException])
    when(e.getCause).thenReturn(cause)
    e
  }

  private def continuousRetryFailure(): StreamingQueryException =
    exceptionWithCause(new SparkException("wrapped", new ContinuousTaskRetryException()))

  private def unrelatedFailure(): StreamingQueryException =
    exceptionWithCause(new RuntimeException("boom"))

  @Test
  def restartsOnContinuousTaskRetryExceptionUntilItSucceeds(): Unit = {
    val query = mock(classOf[StreamingQuery])
    val firstFailure = continuousRetryFailure()
    val secondFailure = continuousRetryFailure()
    doThrow(firstFailure)
      .doThrow(secondFailure)
      .doReturn(true, Nil: _*)
      .when(query)
      .awaitTermination(anyLong())

    var startCalls = 0
    Driver.GroupByStreaming.runWithRetries(maxRetries = 2) { () =>
      startCalls += 1
      query
    }

    assertEquals(3, startCalls)
  }

  @Test
  def givesUpOnceMaxRetriesIsExhausted(): Unit = {
    val query = mock(classOf[StreamingQuery])
    doThrow(continuousRetryFailure()).when(query).awaitTermination(anyLong())

    var startCalls = 0
    try {
      Driver.GroupByStreaming.runWithRetries(maxRetries = 1) { () =>
        startCalls += 1
        query
      }
      fail("expected StreamingQueryException to propagate")
    } catch {
      case _: StreamingQueryException => // expected
    }
    assertEquals(2, startCalls)
  }

  @Test
  def defaultOfZeroRetriesDoesNotRestart(): Unit = {
    val query = mock(classOf[StreamingQuery])
    doThrow(continuousRetryFailure()).when(query).awaitTermination(anyLong())

    var startCalls = 0
    try {
      Driver.GroupByStreaming.runWithRetries(maxRetries = 0) { () =>
        startCalls += 1
        query
      }
      fail("expected StreamingQueryException to propagate")
    } catch {
      case _: StreamingQueryException => // expected
    }
    assertEquals(1, startCalls)
  }

  @Test
  def doesNotRetryFailuresUnrelatedToContinuousTaskRetry(): Unit = {
    val query = mock(classOf[StreamingQuery])
    doThrow(unrelatedFailure()).when(query).awaitTermination(anyLong())

    var startCalls = 0
    try {
      Driver.GroupByStreaming.runWithRetries(maxRetries = 5) { () =>
        startCalls += 1
        query
      }
      fail("expected StreamingQueryException to propagate")
    } catch {
      case _: StreamingQueryException => // expected
    }
    assertEquals(1, startCalls)
  }

  // Never terminates on its own.
  private def hangingQuery(): StreamingQuery = {
    val query = mock(classOf[StreamingQuery])
    when(query.awaitTermination(anyLong())).thenReturn(false)
    query
  }

  // Advances by stepMs on every read.
  private def clock(stepMs: Long): () => Long = {
    var t = 0L
    () => { t += stepMs; t }
  }

  @Test
  def stopsAndRestartsAQueryThatStopsMakingProgress(): Unit = {
    val stalled = hangingQuery()
    val healthy = mock(classOf[StreamingQuery])
    when(healthy.awaitTermination(anyLong())).thenReturn(true)

    val queries = Iterator(stalled, healthy)
    var startCalls = 0
    // Progress frozen at t=0, clock advances 1s per poll: the first query is stopped after 5s.
    Driver.GroupByStreaming.runWithRetries(maxRetries = 1,
                                           progressTimeoutMs = 5000,
                                           pollIntervalMs = 1,
                                           lastProgress = () => 0L,
                                           now = clock(1000)) { () =>
      startCalls += 1
      queries.next()
    }

    assertEquals(2, startCalls)
    verify(stalled).stop()
    verify(healthy, never()).stop()
  }

  @Test
  def failsOnceAStalledQueryHasExhaustedRetries(): Unit = {
    val first = hangingQuery()
    val second = hangingQuery()
    val queries = Iterator(first, second)

    var startCalls = 0
    try {
      Driver.GroupByStreaming.runWithRetries(maxRetries = 1,
                                             progressTimeoutMs = 5000,
                                             pollIntervalMs = 1,
                                             lastProgress = () => 0L,
                                             now = clock(1000)) { () =>
        startCalls += 1
        queries.next()
      }
      fail("expected StreamingQueryStalledException to propagate")
    } catch {
      case _: Driver.GroupByStreaming.StreamingQueryStalledException => // expected
    }
    assertEquals(2, startCalls)
    verify(first).stop()
    verify(second).stop()
  }

  @Test
  def doesNotStopAQueryThatKeepsMakingProgress(): Unit = {
    val query = mock(classOf[StreamingQuery])
    // Active for 9 polls, then terminates normally.
    when(query.awaitTermination(anyLong()))
      .thenReturn(false, false, false, false, false, false, false, false, false, true)

    val time = clock(1000)
    // Progress tracks the clock.
    var startCalls = 0
    Driver.GroupByStreaming.runWithRetries(maxRetries = 0,
                                           progressTimeoutMs = 5000,
                                           pollIntervalMs = 1,
                                           lastProgress = time,
                                           now = time) { () =>
      startCalls += 1
      query
    }

    assertEquals(1, startCalls)
    verify(query, never()).stop()
  }

  @Test
  def stalledQueryWithZeroRetriesFailsWithoutRestarting(): Unit = {
    val query = hangingQuery()
    var startCalls = 0
    try {
      Driver.GroupByStreaming.runWithRetries(maxRetries = 0,
                                             progressTimeoutMs = 5000,
                                             pollIntervalMs = 1,
                                             lastProgress = () => 0L,
                                             now = clock(1000)) { () =>
        startCalls += 1
        query
      }
      fail("expected StreamingQueryStalledException to propagate")
    } catch {
      case _: Driver.GroupByStreaming.StreamingQueryStalledException => // expected
    }
    assertEquals(1, startCalls)
    verify(query).stop()
  }
}
