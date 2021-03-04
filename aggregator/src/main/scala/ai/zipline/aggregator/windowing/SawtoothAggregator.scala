package ai.zipline.aggregator.windowing

import java.util

import ai.zipline.aggregator.base.DataType
import ai.zipline.aggregator.row.{Row, RowAggregator}
import ai.zipline.api.{Aggregation, AggregationPart}
import ai.zipline.api.Extensions._

// Head Sliding, Tail Hopping Window - windowed counters will look the edge of sawtooth
// The hops on the tail are computed automatically to guarantee <10% variance
// There are three major steps in the strategy for realtime accuracy
// 1. Roll up raw events into hops - using HopsAggregator - see buildHopsAggregator
// 2. Use the hops to construct windows - see computeWindows.
//    At his point resolution of head of the window is the smallest hop - 5mins
// 3. To make the head realtime use the cumulate method
//
// NOTE: Not using the "cumulate" method will result in snapshot accuracy.
class SawtoothAggregator(aggregations: Seq[Aggregation], inputSchema: Seq[(String, DataType)], resolution: Resolution)
    extends Serializable {

  private val hopSizes = resolution.hopSizes

  // HopIrs have no notion of windows, and are computed once per `Aggregation`
  // This class maps a single hopIr column into as many window IR columns as specified
  case class WindowMapping(baseIrIndex: Int, spec: AggregationPart) extends Serializable

  private val windowMappings: Array[WindowMapping] = aggregations.zipWithIndex.flatMap {
    case (aggregation, hopIrIndex) =>
      aggregation.unpack.map(WindowMapping(hopIrIndex, _))
  }.toArray
  private val tailHopIndices: Array[Int] = windowMappings.map { mapping =>
    hopSizes.indexOf(resolution.calculateTailHop(mapping.spec.window))
  }

  @transient lazy val windowedAggregator = new RowAggregator(inputSchema, aggregations.flatMap(_.unpack))
  @transient private lazy val baseAggregator = new RowAggregator(inputSchema, aggregations.map(_.unWindowed))
  @transient private lazy val baseIrIndices = windowMappings.map(_.baseIrIndex)

  def computeWindows(hops: HopsAggregator.OutputArrayType, endTimes: Array[Long]): Array[Array[Any]] = {
    val result = Array.fill[Array[Any]](endTimes.length)(windowedAggregator.init)

    if (hops == null) return result
    val arena = Array.fill(hops.length)(Array.fill[Entry](windowedAggregator.length)(null))
    val cache = new HopRangeCache(hops, windowedAggregator, baseIrIndices, arena)
    for (i <- endTimes.indices) {
      for (col <- windowedAggregator.indices) {
        result(i).update(col, genIr(cache, col, endTimes(i)))
        cache.reset()
      }
    }
    result
  }

  // stitches multiple hops into a continuous window
  private def genIr(cache: HopRangeCache, col: Int, endTime: Long): Any = {
    val window = windowMappings(col).spec.window
    var hopIndex = tailHopIndices(col)
    val hopMillis = hopSizes(hopIndex)
    var baseIr: Any = null
    var start = TsUtils.start(endTime, hopMillis, window.millis)
    while (hopIndex < hopSizes.length) {
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
        currBase = windowedAggregator.clone(currBase)
        for (col <- windowedAggregator.indices) {
          val merged = windowedAggregator(col)
            .merge(currBase(col), binned(baseIrIndices(col)))
          currBase.update(col, merged)
        }
      }
      result.update(i, currBase)
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

  // without the reset method, recreating the arena would add to GC presure
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
      ir = agg.merge(ir, hops(endIdx)(baseCol))
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
