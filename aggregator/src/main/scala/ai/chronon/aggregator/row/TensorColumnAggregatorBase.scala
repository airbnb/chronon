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
import ai.chronon.api.{DataType, ListType}

import java.util

abstract class TensorColumnAggregatorBase[Input, IR, Output](agg: BaseAggregator[Input, IR, Output])
    extends ColumnAggregator {
  type IrList = util.ArrayList[IR]
  protected def castIr(ir: Any): IrList = ir.asInstanceOf[IrList]

  final override def merge(ir1: Any, ir2: Any): Any = {
    if (ir2 == null) return ir1
    // we need to clone here because the contract is to only mutate ir1
    // ir2 can itself be expected to mutate later - and hence has to retain its value
    val rightList = castIr(ir2)

    def clone(ir: IR): IR = if (ir == null) ir else agg.clone(ir)

    if (ir1 == null) {
      val rightClone = new IrList(rightList.size())
      var i = 0
      while (i < rightList.size()) {
        rightClone.add(clone(rightList.get(i)))
        i += 1
      }
      return rightClone
    }

    val leftList = castIr(ir1)
    if (leftList.size() != rightList.size()) {
      throw new IllegalStateException(
        s"Tensor dimensions must match for merge: left=${leftList.size()}, right=${rightList.size()}"
      )
    }

    var i = 0
    while (i < leftList.size()) {
      val rightIr = rightList.get(i)
      if (rightIr != null) {
        val leftIr = leftList.get(i)
        if (leftIr == null) {
          leftList.set(i, clone(rightIr))
        } else {
          leftList.set(i, agg.merge(leftIr, rightIr))
        }
      }
      i += 1
    }
    leftList
  }

  private def guardedApply[ValueType, NewValueType](f: ValueType => NewValueType, ir: Any): Any = {
    if (ir == null) return null
    val list = ir.asInstanceOf[util.ArrayList[ValueType]]
    val result = new util.ArrayList[NewValueType](list.size())
    var i = 0
    while (i < list.size()) {
      result.add(f(list.get(i)))
      i += 1
    }
    result
  }

  final override def finalize(ir: Any): Any = guardedApply(agg.finalize, ir)

  final override def normalize(ir: Any): Any = guardedApply(agg.normalize, ir)

  final override def denormalize(ir: Any): Any = guardedApply(agg.denormalize, ir)

  final override def clone(ir: Any): Any = guardedApply(agg.clone, ir)

  final override def outputType: DataType = ListType(agg.outputType)

  final override def irType: DataType = ListType(agg.irType)

  final override def isDeletable: Boolean = agg.isDeletable
}
