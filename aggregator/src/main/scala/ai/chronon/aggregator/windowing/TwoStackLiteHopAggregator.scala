package ai.chronon.aggregator.windowing

import scala.collection.Seq

import ai.chronon.aggregator.row.RowAggregator
import ai.chronon.api.Extensions.{AggregationOps, AggregationPartOps, WindowOps}
import ai.chronon.api._

// This implements the two-stack-lite algorithm
// To understand the intuition behind the algorithm I highly recommend reading the intuition text in the end of this file
class TwoStackLiteHopAggregator(inputSchema: StructType,
                                aggregations: Seq[Aggregation],
                                resolution: Resolution = FiveMinuteResolution)
    extends TwoStackLiteAggregator(inputSchema, aggregations, resolution) {
  private class TwoStackHopIterator(
      queries: Iterator[Long],
      inputs: Iterator[Row],
      inputSize: Int = 1000,
      shouldFinalize: Boolean = true
  ) extends TwoStackIterator(queries, inputs, inputSize, shouldFinalize) {

    private val currentHops = new Array[BankersEntry[Array[Any]]](perWindowAggregators.length)
    override def evictStaleEntry(idx: Int, queryTs: Long): Unit = {
      super.evictStaleEntry(idx, queryTs)

      val perWindowAggregator = perWindowAggregators(idx)
      val queryTail = perWindowAggregator.tailTs(queryTs)
      if (currentHops(idx) != null && queryTail > currentHops(idx).ts) {
        currentHops(idx) = null
      }
    }

    override def update(idx: Int, row: Row, queryTs: Long): Unit = {
      val perWindowAggregator = perWindowAggregators(idx)
      val buffer = buffers(idx)
      if (row.ts >= perWindowAggregator.tailTs(queryTs)) {
        val hopStart = perWindowAggregator.hopStart(row.ts)
        if (currentHops(idx) == null) {
          currentHops(idx) = BankersEntry(perWindowAggregator.agg.prepare(row), hopStart)
        } else if (hopStart == currentHops(idx).ts) {
          perWindowAggregator.agg.update(currentHops(idx).value, row)
        } else {
          buffer.extend(currentHops(idx).value, currentHops(idx).ts)
          currentHops(idx) = BankersEntry(perWindowAggregator.agg.prepare(row), hopStart)
        }
      }
    }

    override def aggregate(idx: Int): Array[Any] = {
      val perWindowAggregator = perWindowAggregators(idx)
      val buffer = buffers(idx)
      val bufferIr = buffer.query
      val finalIr =
        if (currentHops(idx) == null)
          bufferIr
        else
          perWindowAggregator.agg.merge(bufferIr, currentHops(idx).value)

      if (finalIr == null) {
        perWindowAggregator.init
      } else if (shouldFinalize) {
        perWindowAggregator.agg.finalize(finalIr)
      } else {
        finalIr
      }
    }
  }
  // inputs and queries are both assumed to be sorted by time in ascending order
  // all timestamps should be in milliseconds
  // iterator api to reduce memory pressure
  override def slidingSawtoothWindow(queries: Iterator[Long],
                                     inputs: Iterator[Row],
                                     inputSize: Int = 1000,
                                     shouldFinalize: Boolean = true): Iterator[Array[Any]] = {
    new TwoStackHopIterator(queries, inputs.buffered, inputSize, shouldFinalize)
  }
}
