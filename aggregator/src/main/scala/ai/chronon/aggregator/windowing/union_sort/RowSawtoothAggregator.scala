package ai.chronon.aggregator.windowing.union_sort

import ai.chronon.aggregator.base.{ApproxDistinctCount, BaseAggregator}
import ai.chronon.api.Extensions.{AggregationOps, AggregationsOps, WindowOps, WindowUtils}
import ai.chronon.api.{Aggregation, AggregationPart, DataType, Extensions, Operation, Row, Window}
import ai.chronon.aggregator.base._
import ai.chronon.aggregator.row.{ColumnAggregator, ColumnIndices, RowAggregator}
import ai.chronon.aggregator.windowing.FiveMinuteResolution

import java.util
import java.util.ArrayDeque


/*
  Just an idea for now. Make sure all aggregation parts have the same window length
  and therefore hop length. Build a queue of row IRs, let Chronon's existing RowAggregator
  take care of actually combining them.

  For the general case, we'll need to handle the gnarly logic of grouping aggregations by hop length, feeding a subset
  of Aggregations/AggregationParts into RowAggregators, pointing each internal RowAggregator at the correct input
  indices, and then rearranging the outputs
 */
class WindowAlignedRowSawtoothAggregator(val inputSchema: Seq[(String, DataType)], val aggregationParts: Seq[AggregationPart])
  extends Serializable
  {
    private val uniqueWindows = aggregationParts.map(_.window.millis).toSet
    assert(uniqueWindows.size == 1, s"expected one window length, got $uniqueWindows")
    val window: Window = aggregationParts.map(_.window).head
    val hopLength: Long = FiveMinuteResolution.calculateTailHop(window)
    // this ain't pretty
    private val unwindowedAggs: Seq[AggregationPart] = aggregationParts.map(_.deepCopy().setWindow(WindowUtils.Unbounded))
    val internalRowAggregator = new RowAggregator(inputSchema, unwindowedAggs)

    val hopSums: ArrayDeque[(Long, Array[Any])] = new util.ArrayDeque[(Long, Array[Any])]()
    var currTs: Long = 0L
    private def prune(): Unit = {
      val currHopTs = currTs / hopLength * hopLength
      while (!(hopSums.peekFirst() eq null) && hopSums.peekFirst()._1 < (currHopTs - window.millis)) {
        hopSums.removeFirst()
      }
    }

    def update(row: Row): Unit = {
      assert(row.ts >= currTs, s"updates and queries must be sorted by ts! got an update with row with ts ${row.ts} but last seen query/update was $currTs")
      currTs = row.ts
      prune()

      val inputHopTs = row.ts / hopLength * hopLength
      // if the queue is empty, or if the row's hopTs is larger than the head's, add to the queue
      if (hopSums.isEmpty || inputHopTs > hopSums.peekLast()._1) {
        val newHopSum = internalRowAggregator.updateWithReturn(
          internalRowAggregator.init,
          row
        )
        hopSums.addLast((inputHopTs, newHopSum))
      } else {
        // otherwise, pop, add, and reinsert
        val (lastHopTs, headHopIr) = hopSums.pollLast()
        assert(inputHopTs == lastHopTs)
        hopSums.addLast(
          (lastHopTs, internalRowAggregator.updateWithReturn(headHopIr, row))
        )

      }
    }
    def query(ts: Long): Array[Any] = {
      assert(ts >= currTs, s"updates and queries must be sorted by ts! got a query for $ts but last seen query/update was $currTs")
      currTs = ts
      prune()
      val mergeIr = internalRowAggregator.init
      hopSums.forEach { tsAndHopSum =>
        internalRowAggregator.merge(mergeIr, tsAndHopSum._2) // in-place mutates left argument
      }
      internalRowAggregator.finalize(mergeIr)
    }
  }
