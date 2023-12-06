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

package ai.chronon.aggregator.row

import ai.chronon.aggregator.base.BaseAggregator
import ai.chronon.api.{DataType, Row}

class DirectColumnAggregator[Input, IR, Output](agg: BaseAggregator[Input, IR, Output],
                                                columnIndices: ColumnIndices,
                                                dispatcher: Dispatcher[Input, Any])
    extends ColumnAggregator {
  override def outputType: DataType = agg.outputType
  override def irType: DataType = agg.irType

  override def merge(ir1: Any, ir2: Any): Any = {
    if (ir2 == null) return ir1
    // we need to clone here because the contract is to only mutate ir1
    // ir2 can it self be expected to mutate later - and hence has to retain it's value
    // this is a critical assumption of the rest of the code
    if (ir1 == null) return agg.clone(ir2.asInstanceOf[IR])
    agg.merge(ir1.asInstanceOf[IR], ir2.asInstanceOf[IR])
  }

  override def bulkMerge(irs: Iterator[Any]): Any = {
    if (irs == null || !irs.hasNext) return null
    val nonNullIrs = irs.filter(_ != null)
    if (!nonNullIrs.hasNext) return null

    agg.bulkMerge(nonNullIrs.map(_.asInstanceOf[IR]))
  }

  // non bucketed update
  override def update(ir: Array[Any], inputRow: Row): Unit = {
    val inputVal = inputRow.get(columnIndices.input)
    if (inputVal == null) return // null inputs are ignored
    val previousVal = ir(columnIndices.output)
    if (previousVal == null) { // value is absent - so init
      ir.update(columnIndices.output, dispatcher.prepare(inputRow))
      return
    }
    val previous = previousVal.asInstanceOf[IR]
    val updated = dispatcher.updateColumn(previous, inputRow)
    ir.update(columnIndices.output, updated)
  }

  override def delete(ir: Array[Any], inputRow: Row): Unit = {
    if (!agg.isDeletable) return
    val inputVal = inputRow.get(columnIndices.input)
    if (inputVal == null) return
    val previousVal = ir(columnIndices.output)
    if (previousVal == null) {
      ir.update(columnIndices.output, dispatcher.inversePrepare(inputRow))
      return
    }
    val previous = previousVal.asInstanceOf[IR]
    val deleted = dispatcher.deleteColumn(previous, inputRow)
    ir.update(columnIndices.output, deleted)
  }

  override def finalize(ir: Any): Any = numberSanityCheck(guardedApply(agg.finalize, ir))
  override def normalize(ir: Any): Any = guardedApply(agg.normalize, ir)
  override def denormalize(ir: Any): Any = if (ir == null) null else agg.denormalize(ir)
  override def clone(ir: Any): Any = guardedApply(agg.clone, ir)
  private def guardedApply[ValueType, NewValueType](f: ValueType => NewValueType, ir: Any): Any = {
    if (ir == null) null else f(ir.asInstanceOf[ValueType])
  }

  override def isDeletable: Boolean = agg.isDeletable

  def numberSanityCheck(value: Any): Any = {
    value match {
      case i: java.lang.Float  => if (i.isNaN || i.isInfinite) null else i
      case i: java.lang.Double => if (i.isNaN || i.isInfinite) null else i
      case _                   => value
    }
  }
}
