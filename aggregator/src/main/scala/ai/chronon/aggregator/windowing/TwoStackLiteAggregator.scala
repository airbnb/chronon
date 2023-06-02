package ai.chronon.aggregator.windowing

import ai.chronon.aggregator.row.RowAggregator
import ai.chronon.api.Extensions.{AggregationOps, AggregationPartOps, WindowOps}
import ai.chronon.api._
import scala.collection.Seq

// This implements the two-stack-lite algorithm
// To understand the intuition behind the algorithm I highly recommend reading the intuition text in the end of this file
class TwoStackLiteAggregator(inputSchema: StructType,
                             aggregations: Seq[Aggregation],
                             resolution: Resolution = FiveMinuteResolution) {

  protected val allParts: Seq[AggregationPart] = aggregations.flatMap(_.unpack)
  // create row aggregator per window - we will loop over data as many times as there are unique windows
  // we will use different row aggregators to do so
  case class PerWindowAggregator(window: Window, agg: RowAggregator, indexMapping: Array[Int]) {
    private val windowLength: Long = window.millis
    private val tailHopSize = resolution.calculateTailHop(window)
    def tailTs(queryTs: Long): Long = TsUtils.round(queryTs - windowLength, tailHopSize)
    def hopStart(queryTs: Long): Long = TsUtils.round(queryTs, tailHopSize)
    def bankersBuffer(inputSize: Int) = new TwoStackLiteAggregationBuffer[Row, Array[Any], Array[Any]](agg, inputSize)
    def init = new Array[Any](agg.length)
  }

  val inputSchemaTuples: Array[(String, DataType)] = inputSchema.fields.map(f => f.name -> f.fieldType)
  val perWindowAggregators: Array[PerWindowAggregator] = allParts.iterator.zipWithIndex.toArray
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
  protected val unWindowedParts: Array[AggregationPart] = allParts.filter(_.window == null).toArray

  val unWindowedAggregator: Option[RowAggregator] =
    if (unWindowedParts.isEmpty) None
    else Some(new RowAggregator(inputSchemaTuples, unWindowedParts))

  val unWindowedIndexMapping: Array[Int] =
    unWindowedParts.map(c => allParts.indexWhere(c.outputColumnName == _.outputColumnName))

  protected class TwoStackIterator(queries: Iterator[Long],
                         inputs: Iterator[Row],
                         inputSize: Int = 1000,
                         shouldFinalize: Boolean = true)
      extends Iterator[Array[Any]] {

    protected val inputsBuffered = inputs.buffered
    protected val buffers = perWindowAggregators.map(_.bankersBuffer(inputSize))
    protected var unWindowedAgg = if (unWindowedParts.isEmpty) null else new Array[Any](unWindowedParts.length)
    override def hasNext: Boolean = queries.hasNext

    def evictStaleEntry(idx: Int, queryTs: Long): Unit = {
      val perWindowAggregator = perWindowAggregators(idx)
      val buffer = buffers(idx)
      val queryTail = perWindowAggregator.tailTs(queryTs)
      while (buffer.peekBack() != null && buffer.peekBack().ts < queryTail) {
        buffer.pop()
      }
    }

    def update(idx: Int, row: Row, queryTs: Long): Unit = {
      val perWindowAggregator = perWindowAggregators(idx)
      val buffer = buffers(idx)
      if (row.ts >= perWindowAggregator.tailTs(queryTs)) {
        buffer.push(row, row.ts)
      }
    }

    def aggregate(idx: Int): Array[Any] = {
      val perWindowAggregator = perWindowAggregators(idx)
      val buffer = buffers(idx)
      val bufferIr = buffer.query

      if (bufferIr == null) {
        perWindowAggregator.init
      } else if (shouldFinalize) {
        perWindowAggregator.agg.finalize(bufferIr)
      } else {
        bufferIr
      }
    }

    override def next(): Array[Any] = {
      val queryTs = queries.next()

      var i = 0

      // remove all unwanted entries before adding new entries - to keep memory low
      while (i < perWindowAggregators.length) {
        evictStaleEntry(i, queryTs)
        i += 1
      }

      // add all new inputs
      while (inputsBuffered.hasNext && inputsBuffered.head.ts < queryTs) {
        val row = inputsBuffered.next()

        // add to windowed
        i = 0
        while (i < perWindowAggregators.length) { // for each unique window length
          update(i, row, queryTs)
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
        val perWindowOutput = aggregate(i)

        // arrange the perWindowOutput into the indices expected by final output
        if (perWindowOutput != null) {
          val perWindowAggregator = perWindowAggregators(i)
          val indexMapping = perWindowAggregator.indexMapping
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

  // inputs and queries are both assumed to be sorted by time in ascending order
  // all timestamps should be in milliseconds
  // iterator api to reduce memory pressure
  def slidingSawtoothWindow(queries: Iterator[Long],
                            inputs: Iterator[Row],
                            inputSize: Int = 1000,
                            shouldFinalize: Boolean = true): Iterator[Array[Any]] = {
    new TwoStackIterator(queries, inputs.buffered, inputSize, shouldFinalize)
  }
}

/**
    A sliding window is basically a queue. Whenever a new element is added
    to its tail, an older element is to be removed from its head.

    We don't know an easy O(1) way of maintaining maximum in a queue.
    BUT: we _do_ know:

        1. An easy O(1) way to maintain maximum in a _stack_:

            Whenever we pop, we do it as usual for a stack.

            Whenever we push, we push not just a value but a Pair(value, currentMaxValue).
            whereas currentMaxValue is the maximum of value and currentMaxValue
            of the stack's previous top element.

            This ensures that currentMaxValue on the stack's top always contains maximum
            across the whole stack. And that maximum is always at our service (at the top).

            Example:

                Push(3):    {3,3}

                Push(1):    {1,3}
                            {3,3}

                Push(5):    {5,5}
                            {1,3}
                            {3,3}

                Pop():      {1,3}
                            {3,3}

                Pop():      {3,3}

                Push(6):    {6,6}
                            {3,3}

        2. An easy way of building a queue out of two stacks.

            We create stack1 and stack2.

            Whenever we enqueue, we always enqueue to stack2 (and maintain maximum in it,
            as described above).

            Whenever we dequeue, we first check if stack1 is empty.
                If it is empty, then we put everything from stack2 into stack1 (while again
                maintaining maximum in it). This process reverses the order of elements
                ("the last shall be first and the first last") - and that's what we need.
            Then we pop an element from stack1 and return it.

            Whenever we need to know the current maximum in the queue,
            we simply return maximum of both stacks.
Courtesy:
https://leetcode.com/problems/sliding-window-maximum/solutions/2029522/C-or-Elegant-solution-with-_two_-stacks-or-O(n)-or-Detailed-explanation-or-Easy-to-remember/
  */
