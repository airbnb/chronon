package ai.zipline.aggregator.windowing

import java.lang
import ai.zipline.aggregator.base.{DataType, ListType, LongType, StructField, StructType}
import ai.zipline.aggregator.row.Row
import ai.zipline.api.Extensions.{AggregationPartOps, WindowOps}
import ai.zipline.api.{Aggregation, TimeUnit, Window}
import com.google.gson.Gson

import scala.Long

case class BatchIr(collapsed: Array[Any], tailHops: HopsAggregator.IrMapType)
case class FinalBatchIr(collapsed: Array[Any], tailHops: HopsAggregator.OutputArrayType)

class SawtoothOnlineAggregator(batchEndTs: Long,
                               aggregations: Seq[Aggregation],
                               inputSchema: Seq[(String, DataType)],
                               resolution: Resolution = FiveMinuteResolution,
                               tailBufferMillis: Long = new Window(2, TimeUnit.DAYS).millis)
    extends SawtoothAggregator(aggregations: Seq[Aggregation],
                               inputSchema: Seq[(String, DataType)],
                               resolution: Resolution) {

  val hopsAggregator = new HopsAggregatorBase(aggregations, inputSchema, resolution)

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

  def batchIrSchema: Array[(String, DataType)] = {
    val collapsedSchema = windowedAggregator.irSchema
    val hopFields = baseAggregator.irSchema :+ ("ts", LongType)
    Array("collapsedIr" -> StructType.from("WindowedIr", collapsedSchema),
          "tailHopIrs" -> ListType(ListType(StructType.from("HopIr", hopFields))))
  }

  val tailTs: Array[Option[Long]] = windowMappings.map { mapping =>
    Option(mapping.aggregationPart.window).map { batchEndTs - _.millis }
  }

  println(s"Batch End: ${TsUtils.toStr(batchEndTs)}")
  println("Window Tails: ")
  for (i <- windowMappings.indices) {
    println(s"  ${windowMappings(i).aggregationPart.outputColumnName} -> ${tailTs(i).map(TsUtils.toStr)}")
  }

  def init: BatchIr = {
    BatchIr(Array.fill(windowedAggregator.length)(null), hopsAggregator.init())
  }

  def update(batchIr: BatchIr, row: Row): BatchIr = {
    val rowTs = row.ts
    val updatedHop = Array.fill(hopSizes.length)(false)
    for (i <- 0 until windowedAggregator.length) {
      if (batchEndTs > rowTs && tailTs(i).forall(rowTs > _)) { // relevant for the window
        if (tailTs(i).forall(rowTs >= _ + tailBufferMillis)) { // update collapsed part
          windowedAggregator.columnAggregators(i).update(batchIr.collapsed, row)
        } else { // update tailHops part
          val hopIndex = tailHopIndices(i)
          // eg., 7d, 8d windows shouldn't update the same 1hr tail hop twice
          // so update a hop only once
          if (!updatedHop(hopIndex)) {
            updatedHop.update(hopIndex, true)
            val hopStart = TsUtils.round(rowTs, hopSizes(hopIndex))
            val hopIr = batchIr.tailHops(hopIndex).computeIfAbsent(hopStart, hopsAggregator.javaBuildHop)
            val baseIrIndex = windowMappings(i).baseIrIndex
            baseAggregator.columnAggregators(baseIrIndex).update(hopIr, row)
          }
        }
      }
    }
    batchIr
  }

  def merge(batchIr1: BatchIr, batchIr2: BatchIr): BatchIr =
    BatchIr(windowedAggregator.merge(batchIr1.collapsed, batchIr2.collapsed),
            hopsAggregator.merge(batchIr1.tailHops, batchIr2.tailHops))

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
  //        2. just update the cumulative part and sum with batch IR again
  // TODO: Account for deletions
  //    - row needs to have a concept of "reversal_flag"
  //    - call delete instead of update when reversal_flag is true.
  def lambdaAggregateIr(finalBatchIr: FinalBatchIr, streamingRows: Iterator[Row], queryTs: Long): Array[Any] = {
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
    for (row <- headRows) {
      val rowTs = row.ts // unbox long only once
      if (queryTs > rowTs && rowTs >= batchEndTs) {
        for (i <- windowedAggregator.indices) {
          val window = windowMappings(i).aggregationPart.window
          val hopIndex = tailHopIndices(i)
          if (window == null || rowTs >= TsUtils.round(queryTs - window.millis, hopSizes(hopIndex))) {
            windowedAggregator(i).update(resultIr, row)
          }
        }
      }
    }

    // add tail hopIrs
    for (i <- 0 until windowedAggregator.length) {
      val window = windowMappings(i).aggregationPart.window
      if (window != null) { // no hops for unwindowed
        val hopIndex = tailHopIndices(i)
        val queryTail = TsUtils.round(queryTs - window.millis, hopSizes(hopIndex))
        val hopIrs = batchIr.tailHops(hopIndex)
        for (hopIr <- hopIrs) {
          val hopStart = hopIr.last.asInstanceOf[Long]
          if ((batchEndTs - window.millis) + tailBufferMillis > hopStart && hopStart >= queryTail) {
            val merged = windowedAggregator(i).merge(resultIr(i), hopIr(baseIrIndices(i)))
            resultIr.update(i, merged)
          }
        }
      }
    }

    resultIr
  }

  def lambdaAggregateFinalized(finalBatchIr: FinalBatchIr, streamingRows: Iterator[Row], ts: Long): Array[Any] = {
    windowedAggregator.finalize(lambdaAggregateIr(finalBatchIr, streamingRows, ts))
  }

}
