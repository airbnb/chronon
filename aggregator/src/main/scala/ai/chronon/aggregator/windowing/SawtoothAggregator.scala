package ai.chronon.aggregator.windowing

import ai.chronon.aggregator.row.RowAggregator
import ai.chronon.api.Extensions.{UnpackedAggregations, WindowMapping, WindowOps}
import ai.chronon.api.{Aggregation, AggregationPart, DataType, Row}

import java.util
import scala.collection.Seq

// Head Sliding, Tail Hopping Window - effective window size when plotted against query timestamp
// will look the edge of sawtooth - instead of like a straight line.
//
// There are three major steps in the strategy for realtime accuracy
// 1. Roll up raw events into hops - using HopsAggregator - see buildHopsAggregator
//      Output data will look like `key -> [[IR_hop1], [IR_hop2], [IR_hop3] ... ]`
// 2. Use the hops to construct windows - see `computeWindows`.
//       We consume the hops and construct the full window of IR - but without head accuracy
//       Output data will look like `(key, hopStart) -> IR`
//    At his point resolution of head of the window is the smallest hop - 5mins
// 3. To make the head realtime use the `cumulate` method
//       We JOIN the output of
//       a. `computeWindows` - `(key, hopStart) -> IR`
//       b. the raw events on the head by hopStart - `(key, hopStart) -> [Input]`
//       c. query_times by hopStart - `(key, hopStart) -> [query_ts]`
//      And produce `key -> [query_ts, IR]`
// NOTE: Not using the `cumulate` method will result in snapshot accuracy.
class SawtoothAggregator(aggregations: Seq[Aggregation], inputSchema: Seq[(String, DataType)], resolution: Resolution)
    extends Serializable {

  protected val hopSizes = resolution.hopSizes

  @transient lazy val unpackedAggs = UnpackedAggregations.from(aggregations)
  @transient lazy protected val tailHopIndices: Array[Int] = windowMappings.map { mapping =>
    hopSizes.indexOf(resolution.calculateTailHop(mapping.aggregationPart.window))
  }

  @transient lazy val windowMappings: Array[WindowMapping] = unpackedAggs.perWindow
  @transient lazy val perWindowAggs: Array[AggregationPart] = windowMappings.map(_.aggregationPart)
  @transient lazy val windowedAggregator = new RowAggregator(inputSchema, unpackedAggs.perWindow.map(_.aggregationPart))
  @transient lazy val baseAggregator = new RowAggregator(inputSchema, unpackedAggs.perBucket)
  @transient protected lazy val baseIrIndices = windowMappings.map(_.baseIrIndex)

  // the cache uses this space to work out the IRs for the whole window based on hops
  // we only create this arena once, so GC kicks in fewer times
  @transient private lazy val arena =
    Array.fill(resolution.hopSizes.length)(Array.fill[Entry](windowedAggregator.length)(null))

  // The endTimes are sorted, and they should all align with the window right boundaries.
  // The function generate the sawtooth tailing windows -> the windows to be used for adding the
  // final heads.
  def computeWindows(hops: HopsAggregator.OutputArrayType, endTimes: Array[Long]): Array[Array[Any]] = {
    val result = Array.fill[Array[Any]](endTimes.length)(windowedAggregator.init)

    if (hops == null) return result

    val cache = new HopRangeCache(hops, windowedAggregator, baseIrIndices, arena)
    for (i <- endTimes.indices) {
      for (col <- windowedAggregator.indices) {
        // the cache here is provide the caching for the aggregation for the `col-th` window aggregation.
        // The idea is that, in the next end time, it might still either:
        // * share the same tail, so that there is no need to aggregate again.
        // * the events in the window are much later than the tail, so that the result can be reused.
        result(i).update(col, genIr(cache, col, endTimes(i)))
      }
    }
    cache.reset()
    result
  }

  // stitches multiple hops into a continuous window
  private def genIr(cache: HopRangeCache, col: Int, endTime: Long): Any = {
    val window = perWindowAggs(col).window
    var hopIndex = tailHopIndices(col)
    val hopMillis = hopSizes(hopIndex)
    var baseIr: Any = null
    var start = TsUtils.round(endTime - window.millis, hopMillis)
    while (hopIndex < hopSizes.length) {
      // The idea of the code here is to use different granularities of hops to calculate the window.
      // For example: to find a window of [6:00 - 8:15)
      // the logic will find [6:00 - 7:00) + [7:00 - 8:00) + [8:00-8:05) + [8:05-8:10) + [8:10-8:15)
      val end = TsUtils.round(endTime, hopSizes(hopIndex))
      baseIr = windowedAggregator(col).merge(baseIr, cache.merge(hopIndex, col, start, end))
      start = end
      hopIndex += 1
    }
    baseIr
  }

  // method is used to generate head-realtime ness on top of hops
  // But without the requirement that the input be sorted
  def cumulate(inputs: Iterator[Row], // don't need to be sorted
               sortedEndTimes: Array[Long], // sorted,
               baseIR: Array[Any]): Array[Array[Any]] = {
    if (sortedEndTimes == null || sortedEndTimes.isEmpty) return Array.empty[Array[Any]]
    if (inputs == null || inputs.isEmpty)
      return Array.fill[Array[Any]](sortedEndTimes.length)(baseIR)

    val result = Array.fill[Array[Any]](sortedEndTimes.length)(null)
    while (inputs.hasNext) {
      val row = inputs.next()
      val inputTs = row.ts
      var updateIndex = util.Arrays.binarySearch(sortedEndTimes, inputTs)
      if (updateIndex >= 0) { // we found an exact match so we need to search further to get updateIndex
        while (updateIndex < sortedEndTimes.length && sortedEndTimes(updateIndex) == inputTs)
          updateIndex += 1
      } else {
        // binary search didn't find an exact match
        updateIndex = math.abs(updateIndex) - 1
      }
      if (updateIndex < sortedEndTimes.length && updateIndex >= 0) {
        if (result(updateIndex) == null) {
          // This code here is very very very very very confusing.
          // it basically first initialize result[i] as [baseAggregator.length] to store per bucket (no window) result
          result.update(updateIndex, new Array[Any](baseAggregator.length))
        }
        baseAggregator.update(result(updateIndex), row)
      }
    }

    // at this point results aren't cumulated, and are one per spec, instead of one per window
    var currBase = baseIR
    for (i <- result.indices) {
      val binned = result(i)
      if (binned != null) {
        // The currBase stores the the result of aggregation up to the last event.
        // Note all events here shares the same start ts, so that the previous result can be reused.
        currBase = windowedAggregator.clone(currBase)
        for (col <- windowedAggregator.indices) {
          // the code here uses the result from line 112 - 114, to aggregate with the currBase
          val merged = windowedAggregator(col)
            .merge(currBase(col), binned(baseIrIndices(col)))
          currBase.update(col, merged)
        }
      }
      result.update(i, currBase) // now result[i] is changed from [baseAggregator.length] to [windowedAggregator.length], be careful.
    }
    result
  }
}

private class Entry(var startIndex: Int, var endIndex: Int, var ir: Any) {}

private[windowing] class HopRangeCache(hopsArrays: HopsAggregator.OutputArrayType,
                                       windowAggregator: RowAggregator,
                                       hopIrIndices: Array[Int],
                                       // arena is the memory buffer where cache entries live
                                       arena: Array[Array[Entry]]) {

  // without the reset method, recreating the arena would add to GC pressure
  def reset(): Unit = {
    for (i <- arena.indices) {
      for (j <- arena(i).indices) {
        arena(i).update(j, null)
      }
    }
  }

  @inline
  private def ts(hop: Array[Any]): Long = hop.last.asInstanceOf[Long]

  // start and end need to be multiples of hop-sizes for this to work
  // Every call to this method constructs a unique reference, but uses clone as
  // much as possible instead of
  def merge(hopIndex: Int, col: Int, start: Long, end: Long): Any = {
    val hops = hopsArrays(hopIndex)
    val cached: Entry = arena(hopIndex)(col)
    val agg = windowAggregator(col)
    val baseCol = hopIrIndices(col)

    var startIdx = if (cached == null) 0 else cached.startIndex
    while (startIdx < hops.length && ts(hops(startIdx)) < start) {
      startIdx += 1
    }

    var ir: Any = null
    var endIdx = startIdx
    if (cached != null && startIdx == cached.startIndex) {
      // un-windowed case will always degenerate to cumulative sum
      ir = agg.clone(cached.ir)
      endIdx = cached.endIndex
    }

    while (endIdx < hops.length && ts(hops(endIdx)) < end) {
      ir = agg.merge(ir, hops(endIdx)(baseCol)) // hops dont care about window, so that are all bucket based
      endIdx += 1
    }

    if (cached == null) {
      val newEntry = new Entry(startIdx, endIdx, ir)
      arena(hopIndex).update(col, newEntry)
    } else if (cached.startIndex != startIdx || cached.endIndex != endIdx) {
      // reusing the entry object to reduce GC pressure
      cached.startIndex = startIdx
      cached.endIndex = endIdx
      cached.ir = ir
    }

    ir
  }

}
