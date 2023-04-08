package ai.chronon.aggregator.windowing

import ai.chronon.aggregator.row.RowAggregator
import ai.chronon.api.Extensions.{AggregationOps, AggregationPartOps, WindowOps}
import ai.chronon.api._
import com.google.common.collect.Iterators
import scala.collection.Seq
import scala.util.ScalaJavaConversions.JIteratorOps

// DABALite - De-Amortized Bankers Aggregator for sliding windows
// This is based on "In-Order Sliding-Window Aggregation in Worst-Case Constant Time" by Tangwongsan et. al
// see: https://arxiv.org/pdf/2009.13768.pdf
// To understand the intuition behind the algorithm I highly recommend reading the intuition text in the end of this file
class BankerSawtoothAggregator(inputSchema: StructType, aggregations: Seq[Aggregation], resolution: Resolution = FiveMinuteResolution) {

  private val allParts = aggregations.flatMap(_.unpack)
  private val outputColumnNames = allParts.map(_.outputColumnName)
  // create row aggregator per window - we will loop over data as many times as there are unique windows
  // we will use different row aggregators to do so
  case class PerWindowAggregator(window: Window, agg: RowAggregator) {
    private val windowLength: Long = window.millis
    private val tailHopSize = resolution.calculateTailHop(window)
    def tailTs(queryTs: Long): Long = ((queryTs - windowLength)/tailHopSize) * tailHopSize
    def bankersBuffer() = new BankersAggregationBuffer[Row, Array[Any], Array[Any]](agg)
    def init = new Array[Any](agg.length)
    val indexMapping: Array[Int] = agg
      .aggregationParts
      .iterator
      .map(part => outputColumnNames.indexWhere(_ == part.outputColumnName))
      .toArray
  }

  val perWindowAggregators : Array[PerWindowAggregator] = allParts.groupBy(_.window).toArray.map{
    case (window, parts) => PerWindowAggregator(
      window, new RowAggregator(inputSchema.fields.map(f => f.name -> f.fieldType), parts)
    )
  }

  // inputs and queries are both assumed to be sorted by time in ascending order
  // all timestamps should be in milliseconds
  // iterator api to reduce memory pressure
  def slidingSawtoothWindow(queries: Iterator[Long], inputs: Iterator[Row], shouldFinalize: Boolean = true): Iterator[Array[Any]] = {
    val inputsPeeking = Iterators.peekingIterator(inputs.toJava)
    val buffers = perWindowAggregators.map(_.bankersBuffer())
    new Iterator[Array[Any]] {
      override def hasNext: Boolean = queries.hasNext

      override def next(): Array[Any] = {
        val queryTs = queries.next()

        // remove all unwanted entries before adding new entries - to keep memory low
        var i = 0
        while(i < perWindowAggregators.length) {
          val perWindowAggregator = perWindowAggregators(i)
          val buffer = buffers(i)
          val queryTail = perWindowAggregator.tailTs(queryTs)
          while(buffer.peekBack() != null && buffer.peekBack().ts < queryTail) {
            buffer.pop()
          }
          i += 1
        }

        // add all new inputs
        while(inputsPeeking.hasNext && inputsPeeking.peek().ts < queryTs) {
          val row = inputsPeeking.next()
          i = 0
          while(i < perWindowAggregators.length) { // for each unique window length
            val perWindowAggregator = perWindowAggregators(i)
            val buffer = buffers(i)
            if(row.ts >= perWindowAggregator.tailTs(queryTs)) {
              buffer.push(row, row.ts)
            }
            i += 1
          }
        }

        // buffer contains only relevant events now - query the buffer and update the result
        val result = new Array[Any](allParts.length)
        i = 0
        while(i < perWindowAggregators.length) {
          val perWindowAggregator = perWindowAggregators(i)
          val buffer = buffers(i)
          val indexMapping = perWindowAggregator.indexMapping
          val bufferIr = buffer.query

          val perWindowOutput = if(bufferIr == null) {
            perWindowAggregator.init
          } else if(shouldFinalize) {
            perWindowAggregator.agg.finalize(bufferIr)
          } else {
            bufferIr
          }

          // arrange the perWindowOutput into the indices expected by final output
          if(perWindowOutput != null) {
            var j = 0
            while(j < indexMapping.length) {
              result.update(indexMapping(j), perWindowOutput(j))
              j += 1
            }
          }
          i += 1
        }

        result
      }
    }
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