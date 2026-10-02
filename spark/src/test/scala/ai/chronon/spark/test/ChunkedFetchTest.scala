package ai.chronon.spark.test

import ai.chronon.spark.streaming.ChunkedFetch
import org.junit.Assert.{assertEquals, assertTrue}
import org.junit.Test

import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.{Semaphore, TimeoutException}
import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Future, Promise}

class ChunkedFetchTest {
  implicit val ec: ExecutionContext = ExecutionContext.global

  @Test
  def zeroChunkSizeFetchesWholePartitionOnce(): Unit = {
    val calls = mutable.ArrayBuffer[Seq[Int]]()
    val out = ChunkedFetch.run((1 to 10).toList, chunkSize = 0, inflight = None, timeoutMs = 5000) { chunk =>
      calls.synchronized(calls += chunk)
      Future.successful(chunk.map(_ * 2))
    }
    assertEquals((1 to 10).map(_ * 2).toList, out.toList)
    assertEquals(1, calls.size)
  }

  @Test
  def chunksPreserveOrderAndSizes(): Unit = {
    val calls = mutable.ArrayBuffer[Seq[Int]]()
    val out = ChunkedFetch.run((1 to 10).toList, chunkSize = 4, inflight = None, timeoutMs = 5000) { chunk =>
      calls.synchronized(calls += chunk)
      Future(chunk.map(_ * 2))
    }
    assertEquals((1 to 10).map(_ * 2).toList, out.toList)
    assertEquals(List(4, 4, 2), calls.map(_.size).toList)
  }

  @Test
  def semaphoreBoundsConcurrentChunks(): Unit = {
    val inflight = new AtomicInteger()
    val maxSeen = new AtomicInteger()
    val out = ChunkedFetch.run((1 to 40).toList, chunkSize = 2, inflight = Some(new Semaphore(3)), timeoutMs = 10000) {
      chunk =>
        val now = inflight.incrementAndGet()
        maxSeen.accumulateAndGet(now, math.max)
        Future {
          Thread.sleep(20)
          inflight.decrementAndGet()
          chunk
        }
    }
    assertEquals((1 to 40).toList, out.toList)
    assertTrue(s"saw ${maxSeen.get()} concurrent chunks", maxSeen.get() <= 3)
  }

  @Test
  def permitsAreReleasedWhenAChunkFails(): Unit = {
    val semaphore = new Semaphore(1)
    val thrown =
      try {
        ChunkedFetch.run((1 to 4).toList, chunkSize = 2, inflight = Some(semaphore), timeoutMs = 5000) { chunk =>
          if (chunk.head == 1) Future.failed(new IllegalStateException("backend down")) else Future.successful(chunk)
        }
        false
      } catch { case _: IllegalStateException => true }
    assertTrue(thrown)
    // Both chunks ran (the second waited on the first's permit) and both permits came back.
    assertTrue(semaphore.tryAcquire())
  }

  @Test
  def waitingForAPermitCountsAgainstTheTimeout(): Unit = {
    val semaphore = new Semaphore(1)
    val never = Promise[Seq[Int]]()
    val start = System.currentTimeMillis()
    try {
      ChunkedFetch.run((1 to 4).toList, chunkSize = 2, inflight = Some(semaphore), timeoutMs = 300) { _ =>
        never.future
      }
      throw new AssertionError("expected a timeout")
    } catch {
      case _: TimeoutException =>
        val elapsed = System.currentTimeMillis() - start
        assertTrue(s"timed out after ${elapsed}ms", elapsed >= 250 && elapsed < 5000)
    }
  }
}
