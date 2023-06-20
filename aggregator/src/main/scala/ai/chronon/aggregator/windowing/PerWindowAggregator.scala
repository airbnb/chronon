package ai.chronon.aggregator.windowing

import ai.chronon.aggregator.row.RowAggregator
import ai.chronon.api.Extensions.WindowOps
import ai.chronon.api.{Row, Window}

// create row aggregator per window - we will loop over data as many times as there are unique windows
// we will use different row aggregators to do so
case class PerWindowAggregator(window: Window, resolution: Resolution, agg: RowAggregator, indexMapping: Array[Int]) {
  private val windowLength: Long = window.millis
  private val tailHopSize = resolution.calculateTailHop(window)

  def tailTs(queryTs: Long): Long = TsUtils.round(queryTs - windowLength, tailHopSize)

  def hopStart(queryTs: Long): Long = TsUtils.round(queryTs, tailHopSize)

  def bankersBuffer(inputSize: Int) = new TwoStackLiteAggregationBuffer[Row, Array[Any], Array[Any]](agg, inputSize)

  def init = new Array[Any](agg.length)
}
