package ai.chronon.aggregator.windowing

import ai.chronon.api.Extensions.{AggregationPartOps, WindowOps}
import ai.chronon.api._

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

  println(s"Batch End: ${TsUtils.toStr(batchEndTs)}")
  println("Window Tails: ")
  for (i <- windowMappings.indices) {
    println(s"  ${windowMappings(i).aggregationPart.outputColumnName} -> ${batchTailTs(i).map(TsUtils.toStr)}")
  }

  def update(batchIr: BatchIr, row: Row): BatchIr = update(batchEndTs, batchIr, row)

  def finalizeTail(batchIr: BatchIr): FinalBatchIr =
    FinalBatchIr(
      windowedAggregator.normalize(batchIr.collapsed),
      Option(batchIr.tailHops)
        .map(hopsAggregator.toTimeSortedArray)
        .map(_.map(_.map { hopIr =>
          baseAggregator.indices.foreach(i => hopIr.update(i, baseAggregator(i).normalize(hopIr(i))))
          hopIr
        }))
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
    val batchIr = Option(finalBatchIr).getOrElse(finalizeTail(init))
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
      if (queryTs > rowTs && rowTs >= batchEndTs) {
        // When a request with afterTsMillis is passed, we don't consider mutations with mutationTs past the tsMillis
        if ((hasReversal && queryTs >= row.mutationTs) || !hasReversal)
          updateIr(resultIr, row, queryTs, hasReversal)
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

}
