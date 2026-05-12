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

import ai.chronon.online.Fetcher.{Request, ResponseWithContext}
import org.junit.Assert.{assertSame, fail}
import org.junit.{Before, Test}
import org.mockito.Answers
import org.mockito.Mockito.when
import org.scalatestplus.mockito.MockitoSugar

import scala.concurrent.ExecutionContext

/**
  * Verifies that Fetcher.logResponse never lets logging-side failures escape into the
  * user-facing fetch path. The original oncall hit an AssertionError thrown from
  * Row.to during avro encoding for the logging sample; that error (Error subclass) was
  * fatal-typed enough to bypass the surrounding scala.util.Try wrappers and fail the
  * fetch. logResponse now catches NonFatal explicitly and returns the original response.
  */
class FetcherLogResponseTest extends MockitoSugar with MockitoHelper {

  private var kvStore: KVStore = _

  @Before
  def setup(): Unit = {
    kvStore = mock[KVStore](Answers.RETURNS_DEEP_STUBS)
    when(kvStore.executionContext).thenReturn(ExecutionContext.global)
  }

  private def buildFetcher(throwInLog: => Throwable): Fetcher =
    new Fetcher(kvStore = kvStore, metaDataSet = "test_metadata") {
      override protected def logResponseInternal(resp: ResponseWithContext): ResponseWithContext =
        throw throwInLog
    }

  private def buildResponse(): ResponseWithContext =
    ResponseWithContext(
      request = Request(name = "test_join", keys = Map("user_id" -> Long.box(1L))),
      ctx = Metrics.Context(Metrics.Environment.JoinFetching),
      requestStartTs = System.currentTimeMillis(),
      baseValues = Map("foo" -> "bar")
    )

  @Test
  def logResponseSwallowsAssertionErrorAndReturnsOriginalResponse(): Unit = {
    // AssertionError extends Error, so it bypasses scala.util.Try, but is NonFatal —
    // logResponse must catch it and return the input unchanged so the fetch succeeds.
    val fetcher = buildFetcher(new AssertionError("boom from Row.to"))
    val resp = buildResponse()
    val result = fetcher.logResponse(resp)
    assertSame(resp, result)
  }

  @Test
  def logResponseSwallowsRuntimeException(): Unit = {
    val fetcher = buildFetcher(new RuntimeException("encode failed"))
    val resp = buildResponse()
    val result = fetcher.logResponse(resp)
    assertSame(resp, result)
  }

  @Test
  def logResponseRethrowsInterruptedException(): Unit = {
    // Cooperative-cancellation signal — must NOT be swallowed.
    val fetcher = buildFetcher(new InterruptedException("cancel me"))
    try {
      fetcher.logResponse(buildResponse())
      fail("Expected InterruptedException to propagate")
    } catch {
      case _: InterruptedException => // expected
    } finally {
      // Clear interrupted flag in case it leaked onto the test thread.
      Thread.interrupted()
    }
  }
}
