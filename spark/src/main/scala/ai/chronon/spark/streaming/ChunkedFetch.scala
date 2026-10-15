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

package ai.chronon.spark.streaming

import java.util.concurrent.{Semaphore, TimeoutException}
import scala.concurrent.duration.{Duration, MILLISECONDS}
import scala.concurrent.{Await, ExecutionContext, Future}

/**
  * Issues one fetch per chunk of a micro-batch partition instead of one fetch for the whole
  * partition, with an optional JVM-wide bound on how many chunk fetches are outstanding.
  *
  * Without this, the number of rows behind a single fetch equals the partition size, which grows
  * with lag (up to max_offsets_per_trigger / batch_repartition). Downstream backends that split a
  * fetch into fixed-size calls then see their concurrency scale with lag, which slows them further:
  * a feedback loop. Chunking fixes the rows per fetch; the semaphore fixes the fetches per executor.
  *
  * The semaphore is acquired on the task thread before each chunk is issued, so a saturated
  * executor blocks its tasks rather than queueing more requests. `timeoutMs` is measured from the
  * first chunk, so blocked acquisition counts against it the same way a slow backend would.
  */
object ChunkedFetch {

  def run[T, R](items: Seq[T], chunkSize: Int, inflight: Option[Semaphore], timeoutMs: Long)(
      fetch: Seq[T] => Future[Seq[R]])(implicit ec: ExecutionContext): Seq[R] = {
    if (chunkSize <= 0 || items.length <= chunkSize) {
      return Await.result(fetch(items), Duration(timeoutMs, MILLISECONDS))
    }

    val deadlineMs = System.currentTimeMillis() + timeoutMs
    // Force the grouped iterator: a lazy Stream here would issue chunks during Future.sequence.
    val futures: List[Future[Seq[R]]] = items.grouped(chunkSize).toList.map { chunk =>
      inflight.foreach { semaphore =>
        val remainingMs = deadlineMs - System.currentTimeMillis()
        if (remainingMs <= 0 || !semaphore.tryAcquire(remainingMs, java.util.concurrent.TimeUnit.MILLISECONDS)) {
          throw new TimeoutException(
            s"Timed out after ${timeoutMs}ms waiting for an in-flight fetch permit (${semaphore.availablePermits()} free)")
        }
      }
      val future = fetch(chunk)
      inflight.foreach(semaphore => future.onComplete(_ => semaphore.release()))
      future
    }

    val remainingMs = math.max(1L, deadlineMs - System.currentTimeMillis())
    Await.result(Future.sequence(futures), Duration(remainingMs, MILLISECONDS)).flatten
  }
}
