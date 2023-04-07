package ai.chronon.aggregator.windowing

import ai.chronon.aggregator.base.SimpleAggregator

import java.util

case class BankersEntry[IR](var value: IR, ts: Long)

// ported from: https://github.com/IBM/sliding-window-aggregators/blob/master/rust/src/two_stacks_lite/mod.rs with some
// modification to work with simple aggregator
class BankersAggregationBuffer[Input, IR >: Null, Output >: Null](aggregator: SimpleAggregator[Input, IR, Output]) {

  // last is where new events go, first is where old events get popped from
  val deque = new util.ArrayDeque[BankersEntry[IR]]()
  var aggBack: IR = null
  var frontLen = 0

  def reset(): Unit = {
    deque.clear()
    aggBack = null
    frontLen = 0
  }

  def push(input: Input, ts: Long = -1): Unit = {
    val ir = aggregator.prepare(input)
    deque.addLast(BankersEntry(aggregator.clone(ir), ts))
    aggBack = if (aggBack == null) ir else aggregator.update(aggBack, input)
  }

  def pop(): BankersEntry[IR] = {
    if (!deque.isEmpty) {
      if (frontLen == 0) {
        var accum: IR = null
        val it = deque.descendingIterator()
        while (it.hasNext) {
          val entry = it.next()
          accum = if (accum == null) entry.value else aggregator.merge(entry.value, accum)
          entry.value = aggregator.clone(accum)
        }
        frontLen = deque.size()
        aggBack = null
      }
    }
    frontLen -= 1
    deque.removeFirst()
  }

  def peekBack(): BankersEntry[IR] = deque.peekFirst()

  def query: IR = {
    val front = if (frontLen == 0 || deque.isEmpty) {
      null
    } else {
      aggregator.clone(deque.getFirst.value)
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
