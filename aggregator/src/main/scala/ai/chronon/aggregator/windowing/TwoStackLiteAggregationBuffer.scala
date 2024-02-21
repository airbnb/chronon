/*
 *    Copyright (C) 2023 The Chronon Authors.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package ai.chronon.aggregator.windowing

import ai.chronon.aggregator.base.SimpleAggregator

import java.util

case class BankersEntry[IR](var value: IR, ts: Long)

// ported from: https://github.com/IBM/sliding-window-aggregators/blob/master/rust/src/two_stacks_lite/mod.rs with some
// modification to work with simple aggregator
class TwoStackLiteAggregationBuffer[Input, IR >: Null, Output >: Null](aggregator: SimpleAggregator[Input, IR, Output],
                                                                       maxSize: Int) {

  // last is where new events go, first is where old events get popped from
  val deque = new util.ArrayDeque[BankersEntry[IR]](maxSize)
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
