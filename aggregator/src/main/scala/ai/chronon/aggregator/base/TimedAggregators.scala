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

package ai.chronon.aggregator.base

import ai.chronon.aggregator.base.TimeTuple.typ
import ai.chronon.api._

import java.util

object TimeTuple extends Ordering[util.ArrayList[Any]] {
  type typ = util.ArrayList[Any]

  def `type`(inputType: DataType): DataType =
    StructType(
      s"TimePair_${DataType.toString(inputType)}",
      Array(
        StructField("epochMillis", LongType),
        StructField("payload", inputType)
      )
    )

  def make(ts: Long, payload: Any): util.ArrayList[Any] = {
    val ir = new util.ArrayList[Any](2)
    ir.add(ts)
    ir.add(payload)
    ir
  }

  def reset(ts: Long, payload: Any, tup: util.ArrayList[Any]): Unit = {
    tup.set(0, ts)
    tup.set(1, payload)
  }

  def getTs(tup: util.ArrayList[Any]): Long = tup.get(0).asInstanceOf[Long]

  override def compare(x: util.ArrayList[Any], y: util.ArrayList[Any]): Int = {
    java.lang.Long.compare(getTs(x), getTs(y))
  }
}

abstract class TimeOrdered(inputType: DataType) extends TimedAggregator[Any, TimeTuple.typ, Any] {
  override def outputType: DataType = inputType

  override def irType: DataType = TimeTuple.`type`(inputType)

  override def prepare(input: Any, ts: Long): TimeTuple.typ =
    TimeTuple.make(ts, input)

  override def finalize(ir: util.ArrayList[Any]): Any = ir.get(1)

  override def normalize(ir: TimeTuple.typ): Array[Any] = ArrayUtils.toArray(ir)

  override def denormalize(ir: Any): TimeTuple.typ =
    ArrayUtils.fromArray(ir.asInstanceOf[Array[Any]])

  override def clone(ir: TimeTuple.typ): TimeTuple.typ = ir.clone().asInstanceOf[TimeTuple.typ]

}

class First(inputType: DataType) extends TimeOrdered(inputType) {
  //mutating
  override def update(
      ir: util.ArrayList[Any],
      input: Any,
      ts: Long
  ): util.ArrayList[Any] = {
    if (TimeTuple.getTs(ir) > ts) {
      TimeTuple.reset(ts, input, ir)
    }
    ir
  }

  override def merge(
      ir1: util.ArrayList[Any],
      ir2: util.ArrayList[Any]
  ): util.ArrayList[Any] =
    TimeTuple.min(ir1, ir2)
}

class Last(inputType: DataType) extends TimeOrdered(inputType) {
  //mutating
  override def update(
      ir: util.ArrayList[Any],
      input: Any,
      ts: Long
  ): util.ArrayList[Any] = {
    if (TimeTuple.getTs(ir) < ts) {
      TimeTuple.reset(ts, input, ir)
    }
    ir
  }

  override def merge(
      ir1: util.ArrayList[Any],
      ir2: util.ArrayList[Any]
  ): util.ArrayList[Any] =
    TimeTuple.max(ir1, ir2)
}

// FIRSTK LASTK ==============================================================

// FIRSTK LASTK ==============================================================
class OrderByLimitTimed(
    inputType: DataType,
    limit: Int,
    ordering: Ordering[TimeTuple.typ]
) extends TimedAggregator[Any, util.ArrayList[TimeTuple.typ], util.ArrayList[Any]] {
  type Container = util.ArrayList[TimeTuple.typ]
  private val minHeap = new MinHeap[TimeTuple.typ](limit, ordering)

  override def outputType: DataType = ListType(inputType)

  override def irType: DataType = ListType(TimeTuple.`type`(inputType))

  override final def prepare(input: Any, ts: Long): Container = {
//    val gson = new Gson()
    val tuple = TimeTuple.make(ts, input)
//    println(s"init: ${gson.toJson(tuple)}")
    val arr = new Container()
    arr.add(tuple)
    arr
  }

  override final def update(state: Container, input: Any, ts: Long): Container =
    // TODO: tuple making is unnecessary if we are not going to insert
    // one idea is to make insert return a boolean instead, and use that as a cue to reset a tuple object
    // which is a class member
    minHeap.insert(state, TimeTuple.make(ts, input))

  override final def merge(state1: Container, state2: Container): Container =
    minHeap.merge(state1, state2)

  override def finalize(state: Container): util.ArrayList[Any] = {
    val sorted = minHeap.sort(state)
    val result = new util.ArrayList[Any](state.size())
    val it = sorted.iterator
    while (it.hasNext) {
      result.add(it.next().get(1))
    }
    result
  }

  override def normalize(
      ir: util.ArrayList[TimeTuple.typ]
  ): util.ArrayList[Array[Any]] = {
    val result = new util.ArrayList[Array[Any]](ir.size())
    val it = ir.iterator()
    while (it.hasNext) {
      result.add(ArrayUtils.toArray(it.next))
    }
    result
  }

  override def denormalize(ir: Any): util.ArrayList[TimeTuple.typ] = {
    val irCast = ir.asInstanceOf[util.ArrayList[Array[Any]]]
    val result = new util.ArrayList[TimeTuple.typ](irCast.size())
    val it = irCast.iterator()
    while (it.hasNext) {
      result.add(ArrayUtils.fromArray(it.next()))
    }
    result
  }

  override def clone(ir: util.ArrayList[typ]): util.ArrayList[typ] = ir.clone().asInstanceOf[util.ArrayList[typ]]
}

class LastK(inputType: DataType, k: Int) extends OrderByLimitTimed(inputType, k, TimeTuple.reverse)

class FirstK(inputType: DataType, k: Int) extends OrderByLimitTimed(inputType, k, TimeTuple)
