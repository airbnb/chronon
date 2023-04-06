package ai.chronon.aggregator.windowing

import ai.chronon.aggregator.base.{SimpleAggregator, Sum}
import ai.chronon.aggregator.row.RowAggregator
import ai.chronon.api.{Aggregation, AggregationPart, IntType, Row, StructType, Window}
import ai.chronon.api.Extensions.{AggregationOps, WindowOps}
import com.google.common.collect.{Iterators, PeekingIterator}

import java.util
import scala.jdk.CollectionConverters.IteratorHasAsJava

// DABALite - De-Amortized Bankers Aggregator for sliding windows
// This is based on "In-Order Sliding-Window Aggregation in Worst-Case Constant Time" by Tangwongsan et. al
// see: https://arxiv.org/pdf/2009.13768.pdf
// To understand the intuition behind the algorithm I highly recommend reading the intuition text in the end of this file
class TwoStacksLiteAggregator(inputSchema: StructType, aggregations: Seq[Aggregation], resolution: Resolution = FiveMinuteResolution) {

  // create row aggregator per window - we will loop over data as many times as there are unique windows
  // we will use different row aggregators to do so
  case class PerWindowAggregator(window: Window, agg: RowAggregator) {
    private val windowLength: Long = window.millis
    private val tailHopSize = resolution.calculateTailHop(window)
    def tailTs(queryTs: Long): Long = ((queryTs - windowLength)/tailHopSize) * tailHopSize
    val bankersBuffer = new BankersAggregationBuffer[Row, Array[Any], Array[Any]](agg)

  }

  private val allParts = aggregations.flatMap(_.unpack)
  val windowToAggregator : Array[PerWindowAggregator] = allParts.groupBy(_.window).toArray.map{
    case (window, parts) => PerWindowAggregator(
      window, new RowAggregator(inputSchema.fields.map(f => f.name -> f.fieldType), parts)
    )
  }

  // inputs and queries are both assumed to be sorted by time in ascending order
  // all timestamps should be in milliseconds
  def slidingSawtoothWindow(queries: Iterator[Long], inputs: Iterator[Row]): Array[Array[Any]] = {
    val inputsPeekable = Iterators.peekingIterator(inputs.asJava)
    val result = Array.fill[Any](queries.length, allParts.length)(null)
    while (queries.hasNext) { // for each left query
      val queryTs = queries.next()
      while(inputsPeekable.hasNext && inputsPeekable.peek().ts < queryTs) { // for each right datum
        while() { // for each unique window length

        }
      }
    }
    result
  }
}

// ported from: https://github.com/IBM/sliding-window-aggregators/blob/master/rust/src/two_stacks_lite/mod.rs
class BankersAggregationBuffer[Input, IR >: Null, Output >: Null](aggregator: SimpleAggregator[Input, IR, Output]) {
  case class Entry(var value: IR, ts: Option[Long])
  val deque = new util.ArrayDeque[Entry]()
  var aggBack: IR = null
  var frontLen = 0

  def reset(): Unit = {
    deque.clear()
    aggBack = null
    frontLen = 0
  }

  def push(input: Input, ts: Option[Long] = None): Unit = {
    val ir = aggregator.prepare(input)
    deque.addLast(Entry(aggregator.clone(ir), ts))
    aggBack = if(aggBack == null) ir else aggregator.update(aggBack, input)
  }

  def pop(): (Option[Long], IR) = {
    if(!deque.isEmpty) {
      val end = deque.size() - 1
      if (frontLen == 0) {
        var accum: IR = null
        val it = deque.descendingIterator()
        while(it.hasNext) {
          val entry = it.next()
          accum = if(accum == null) entry.value else aggregator.merge(entry.value, accum)
          entry.value = aggregator.clone(accum)
        }
        frontLen = end + 1
        aggBack = null
      }
    }
    frontLen -= 1
    val e = deque.removeFirst()
    e.ts -> e.value
  }

  def query: Output = {
    val front = if (frontLen == 0 || deque.isEmpty) {
       null
    } else {
       aggregator.clone(deque.getFirst.value)
    }
    val ir = if(front == null) {
      aggBack
    } else if(aggBack == null) {
      front
    } else {
      aggregator.merge(front, aggBack)
    }
    if (ir == null) null else aggregator.finalize(ir)
  }
}

object BankersTest {
  object IntegerNumeric extends Numeric[Integer] {
    def plus(x: Integer, y: Integer): Integer = x.intValue() + y.intValue()
    def minus(x: Integer, y: Integer): Integer = x.intValue() - y.intValue()
    def times(x: Integer, y: Integer): Integer = x.intValue() * y.intValue()
    def negate(x: Integer): Integer = -x.intValue()
    def fromInt(x: Int): Integer = Integer.valueOf(x)
    def toInt(x: Integer): Int = x.intValue()
    def toLong(x: Integer): Long = x.longValue()
    def toFloat(x: Integer): Float = x.floatValue()
    def toDouble(x: Integer): Double = x.doubleValue()
    def compare(x: Integer, y: Integer): Int = x.compareTo(y)
    override def parseString(str: String): Option[Integer] = None
  }

  def main(args: Array[String]): Unit = {
    val summer = new Sum[Integer](IntType)(IntegerNumeric)
    val bankersBuffer = new BankersAggregationBuffer(summer)
    println(bankersBuffer.query) // null
    Seq(7, 8, 9).map(x => new Integer(x)).foreach(i => bankersBuffer.push(i))
    println(bankersBuffer.query) // 24
    bankersBuffer.pop()  // pops 7
    println(bankersBuffer.query) // 17
    bankersBuffer.pop() // pops 8
    println(bankersBuffer.query) // 9
    bankersBuffer.pop() // pops 9
    println(bankersBuffer.query) // null
    bankersBuffer.push(new Integer(10))
    println(bankersBuffer.query) // 10
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