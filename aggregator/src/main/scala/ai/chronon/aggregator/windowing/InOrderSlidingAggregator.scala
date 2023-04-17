package ai.chronon.aggregator.windowing

import ai.chronon.aggregator.row.RowAggregator
import ai.chronon.api.Extensions.{AggregationOps, AggregationPartOps, WindowOps}
import ai.chronon.api._
import scala.collection.Seq

import ai.chronon.aggregator.base.SimpleAggregator

class InOrderSlidingAggregator(
    inputSchema: StructType,
    aggregations: Seq[Aggregation],
    resolution: Resolution = FiveMinuteResolution,
    bufferFactory: SimpleAggregator[Row, Array[Any], Array[Any]] => InOrderAggregationBuffer[Row,
                                                                                             Array[Any],
                                                                                             Array[Any]]) {

  private val allParts = aggregations.flatMap(_.unpack)
  // create row aggregator per window - we will loop over data as many times as there are unique windows
  // we will use different row aggregators to do so
  private case class PerWindowAggregator(window: Window, agg: RowAggregator, indexMapping: Array[Int]) {
    private val windowLength: Long = window.millis
    private val tailHopSize = resolution.calculateTailHop(window)
    def tailTs(queryTs: Long): Long = ((queryTs - windowLength) / tailHopSize) * tailHopSize
    def createBuffer(): InOrderAggregationBuffer[Row, Array[Any], Array[Any]] = bufferFactory(agg)
    def init = new Array[Any](agg.length)
  }

  private val inputSchemaTuples: Array[(String, DataType)] = inputSchema.fields.map(f => f.name -> f.fieldType)
  private val perWindowAggregators: Array[PerWindowAggregator] = allParts.iterator.zipWithIndex.toArray
    .filter { case (p, _) => p.window != null }
    .groupBy { case (p, _) => p.window }
    .map {
      case (w, ps) =>
        val parts = ps.map(_._1)
        val idxs = ps.map(_._2)
        PerWindowAggregator(w, new RowAggregator(inputSchemaTuples, parts), idxs)
    }
    .toArray

  // lifetime aggregations don't need bankers buffer, simple unWindowed sum is good enough
  private val unWindowedParts = allParts.filter(_.window == null).toArray
  private val unWindowedAggregator: Option[RowAggregator] =
    if (unWindowedParts.isEmpty) None
    else
      Some(new RowAggregator(inputSchemaTuples, unWindowedParts))
  private val unWindowedIndexMapping: Array[Int] =
    unWindowedParts.map(c => allParts.indexWhere(c.outputColumnName == _.outputColumnName))

  // inputs and queries are both assumed to be sorted by time in ascending order
  // all timestamps should be in milliseconds
  // iterator api to reduce memory pressure
  def slidingSawtoothWindow(queries: Iterator[Long],
                            inputs: Iterator[Row],
                            shouldFinalize: Boolean = true): Iterator[Array[Any]] = {
    val inputsBuffered = inputs.buffered
    val buffers = perWindowAggregators.map(_.createBuffer())
    var unWindowedAgg = if (unWindowedParts.isEmpty) null else new Array[Any](unWindowedParts.length)

    new Iterator[Array[Any]] {
      override def hasNext: Boolean = queries.hasNext

      override def next(): Array[Any] = {
        val queryTs = queries.next()

        // remove all unwanted entries before adding new entries - to keep memory low
        var i = 0
        while (i < perWindowAggregators.length) {
          val perWindowAggregator = perWindowAggregators(i)
          val buffer = buffers(i)
          val queryTail = perWindowAggregator.tailTs(queryTs)
          while (buffer.peekBack() != null && buffer.peekBack().ts < queryTail) {
            buffer.pop()
          }
          i += 1
        }

        // add all new inputs
        while (inputsBuffered.hasNext && inputsBuffered.head.ts < queryTs) {
          val row = inputsBuffered.next()

          // add to windowed
          i = 0
          while (i < perWindowAggregators.length) { // for each unique window length
            val perWindowAggregator = perWindowAggregators(i)
            val buffer = buffers(i)
            if (row.ts >= perWindowAggregator.tailTs(queryTs)) {
              buffer.push(row, row.ts)
            }
            i += 1
          }

          // add to unWindowed
          unWindowedAggregator.foreach { agg =>
            unWindowedAgg = agg.update(unWindowedAgg, row)
          }
        }

        // buffer contains only relevant events now - query the buffer and update the result
        val result = new Array[Any](allParts.length)
        i = 0
        while (i < perWindowAggregators.length) {
          val perWindowAggregator = perWindowAggregators(i)
          val buffer = buffers(i)
          val indexMapping = perWindowAggregator.indexMapping
          val bufferIr = buffer.query()

          val perWindowOutput = if (bufferIr == null) {
            perWindowAggregator.init
          } else if (shouldFinalize) {
            perWindowAggregator.agg.finalize(bufferIr)
          } else {
            bufferIr
          }

          // arrange the perWindowOutput into the indices expected by final output
          if (perWindowOutput != null) {
            var j = 0
            while (j < indexMapping.length) {
              result.update(indexMapping(j), perWindowOutput(j))
              j += 1
            }
          }
          i += 1
        }

        // incorporate unWindowedAggregations
        unWindowedAggregator.foreach { agg =>
          val cAgg = if (shouldFinalize) {
            agg.finalize(unWindowedAgg)
          } else {
            unWindowedAgg
          }

          i = 0
          while (i < unWindowedIndexMapping.length) {
            result.update(unWindowedIndexMapping(i), cAgg(i))
            i += 1
          }
        }

        result
      }
    }
  }
}
