package ai.chronon.aggregator.windowing

import ai.chronon.aggregator.base.SimpleAggregator

import scala.collection.mutable

// This implements the two-stack-lite algorithm
// To understand the intuition behind the algorithm I highly recommend reading the intuition text in the end of this file
// ported from: https://github.com/IBM/sliding-window-aggregators/blob/master/rust/src/two_stacks_lite/mod.rs with some
// modification to work with simple aggregator
class TwoStackLiteBuffer[Input, IR >: Null, Output >: Null](aggregator: SimpleAggregator[Input, IR, Output])
    extends InOrderAggregationBuffer[Input, IR, Output] {

  // last is where new events go, first is where old events get popped from
  private val deque = new mutable.Queue[InOrderAggregationEntry[IR]]
  private var aggBack: IR = null
  private var frontLen = 0

  def reset(): Unit = {
    deque.clear()
    aggBack = null
    frontLen = 0
  }

  def push(input: Input, ts: Long = -1): Unit = {
    val ir = aggregator.prepare(input)
    deque.append(InOrderAggregationEntry(aggregator.clone(ir), ts))
    aggBack = if (aggBack == null) ir else aggregator.update(aggBack, input)
  }

  def pop(): InOrderAggregationEntry[IR] = {
    if (!deque.isEmpty) {
      if (frontLen == 0) {
        var accum: IR = null
        val it = deque.reverseIterator
        while (it.hasNext) {
          val entry = it.next()
          accum = if (accum == null) entry.value else aggregator.merge(entry.value, accum)
          entry.value = aggregator.clone(accum)
        }
        frontLen = deque.length
        aggBack = null
      }
    }
    frontLen -= 1
    deque.dequeue()
  }

  def peekBack(): InOrderAggregationEntry[IR] = if (deque.isEmpty) null else deque.head

  def query(): IR = {
    val front = if (frontLen == 0 || deque.isEmpty) {
      null
    } else {
      aggregator.clone(deque.head.value)
    }
    val ir = if (front == null && aggBack == null) {
      null
    } else if (front == null) {
      aggregator.clone(aggBack)
    } else if (aggBack == null) {
      front
    } else {
      aggregator.merge(front, aggregator.clone(aggBack))
    }
    ir
  }
}

/*
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
