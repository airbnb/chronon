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

package ai.chronon.aggregator.windowing

import org.slf4j.LoggerFactory
import scala.collection.Seq
import ai.chronon.api.Extensions.{AggregationPartOps, WindowOps}
import ai.chronon.api._

// Wrapper class for handling Irs in the tiled chronon use case
case class TiledIr(ts: Long, ir: Array[Any])

// batchEndTs = upload time of the batch data as derived from GroupByServingInfo & Cached
// cache = Jul-22 / latest = Jul-23, streaming data = 22 - now (filter < jul 23)
class SawtoothOnlineAggregator(val batchEndTs: Long,
                               aggregations: Seq[Aggregation],
                               inputSchema: Seq[(String, DataType)],
                               resolution: Resolution = FiveMinuteResolution,
                               tailBufferMillis: Long = new Window(2, TimeUnit.DAYS).millis)
    extends SawtoothMutationAggregator(aggregations: Seq[Aggregation],
                                       inputSchema: Seq[(String, DataType)],
                                       resolution: Resolution,
                                       tailBufferMillis: Long) {
  @transient lazy val logger = LoggerFactory.getLogger(getClass)

  // logically, batch response is arranged like so
  // sum-90d =>  sum_ir_88d, [(sum_ir_1d, ts)] -> 1d is the hopSize for 90d
  // sum-1d =>   null, [(sum_ir_1h, ts)]  -> 1h is the hopSize for 1d
  // sum-1h =>   null, [(sum_ir_5min, ts)] -> 5min is the hopSize for 1h
  // sum-7d =>   sum_ir_5d, [(sum_ir_1h, ts)]
  // practically - the ts part is very repetitive - often repeating itself across all features once
  //              - the additional tuple nesting comes with its own overhead
  // So we store the tail hops separately.
  // 1d_irs - [(txn-sum, login-count, ..., ts)]
  // 1h_irs - [(txn-sum, login-count, ..., ts)]
  // 5min_irs - [(txn-sum, login-count, ..., ts)]

  val batchTailTs: Array[Option[Long]] = tailTs(batchEndTs)

  logger.info(s"Batch End: ${TsUtils.toStr(batchEndTs)}")
  logger.info("Window Tails: ")
  for (i <- windowMappings.indices) {
    logger.info(s"  ${windowMappings(i).aggregationPart.outputColumnName} -> ${batchTailTs(i).map(TsUtils.toStr)}")
  }

  def update(batchIr: BatchIr, row: Row): BatchIr = update(batchEndTs, batchIr, row, batchTailTs)

  def normalizeBatchIr(batchIr: BatchIr): FinalBatchIr =
    FinalBatchIr(
      windowedAggregator.normalize(batchIr.collapsed),
      Option(batchIr.tailHops)
        .map(hopsAggregator.toTimeSortedArray)
        .map(_.map(_.map { baseAggregator.normalizeInPlace }))
        .orNull
    )

  def denormalizeBatchIr(batchIr: FinalBatchIr): FinalBatchIr =
    FinalBatchIr(
      windowedAggregator.denormalize(batchIr.collapsed),
      Option(batchIr.tailHops)
        .map(_.map(_.map { baseAggregator.denormalizeInPlace }))
        .orNull
    )

  // TODO: We can cache aggregated values in a very interesting way
  //   - we would need to cache (tailHops, collapsed,  cumulative_streamingRows, latestStreamingTs)
  //   - upon a cache hit we would need to
  //        1. Scan everything from streaming only after latestStreamingTs
  def lambdaAggregateIr(finalBatchIr: FinalBatchIr,
                        streamingRows: Iterator[Row],
                        queryTs: Long,
                        hasReversal: Boolean = false): Array[Any] = {
    // null handling
    if (finalBatchIr == null && streamingRows == null) return null
    val batchIr = Option(finalBatchIr).getOrElse(normalizeBatchIr(init))
    val headRows = Option(streamingRows).getOrElse(Array.empty[Row].iterator)

    if (batchEndTs > queryTs) {
      throw new IllegalArgumentException(s"Request time of $queryTs is less than batch time $batchEndTs")
    }

    // initialize with collapsed
    val resultIr = windowedAggregator.clone(batchIr.collapsed)

    // add head events
    while (headRows.hasNext) {
      val row = headRows.next()
      val rowTs = row.ts // unbox long only once

      val shouldSelect = if (hasReversal) {
        // mutation case
        val mutationTs = row.mutationTs
        val rowBeforeQuery = queryTs > rowTs && queryTs > mutationTs
        val rowAfterBatchEnd = mutationTs >= batchEndTs
        rowBeforeQuery && rowAfterBatchEnd
      } else {
        // event case
        val rowBeforeQuery = queryTs > rowTs
        val rowAfterBatchEnd = rowTs >= batchEndTs
        rowBeforeQuery && rowAfterBatchEnd
      }

      if (shouldSelect) {
        updateIr(resultIr, row, queryTs, hasReversal)
      }
    }
    mergeTailHops(resultIr, queryTs, batchEndTs, batchIr)
    resultIr
  }

  def lambdaAggregateIrTiled(finalBatchIr: FinalBatchIr,
                             streamingTiledIrs: Iterator[TiledIr],
                             queryTs: Long): Array[Any] = {
    // null handling
    if (finalBatchIr == null && streamingTiledIrs == null) return null
    val batchIr = Option(finalBatchIr).getOrElse(normalizeBatchIr(init))
    val tiledIrs = Option(streamingTiledIrs).getOrElse(Array.empty[TiledIr].iterator)

    if (batchEndTs > queryTs) {
      throw new IllegalArgumentException(s"Request time of $queryTs is less than batch time $batchEndTs")
    }

    // initialize with collapsed
    val resultIr = windowedAggregator.clone(batchIr.collapsed)

    // add streaming tiled irs
    while (tiledIrs.hasNext) {
      val tiledIr = tiledIrs.next()
      val tiledIrTs = tiledIr.ts // unbox long only once
      if (queryTs > tiledIrTs && tiledIrTs >= batchEndTs) {
        updateIrTiled(resultIr, tiledIr, queryTs)
      }
    }
    mergeTailHops(resultIr, queryTs, batchEndTs, batchIr)
    resultIr
  }

  def lambdaAggregateFinalized(finalBatchIr: FinalBatchIr,
                               streamingRows: Iterator[Row],
                               ts: Long,
                               hasReversal: Boolean = false): Array[Any] = {
    windowedAggregator.finalize(lambdaAggregateIr(finalBatchIr, streamingRows, ts, hasReversal = hasReversal))
  }

  def lambdaAggregateFinalizedTiled(finalBatchIr: FinalBatchIr,
                                    streamingTiledIrs: Iterator[TiledIr],
                                    ts: Long): Array[Any] = {
    // TODO: Add support for mutations / hasReversal to the tiled implementation
    windowedAggregator.finalize(lambdaAggregateIrTiled(finalBatchIr, streamingTiledIrs, ts))
  }

}
