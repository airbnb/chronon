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
import ai.chronon.api.{DataType, MapType, StringType}

import java.util

abstract class MapColumnAggregatorBase[Input, IR, Output](agg: BaseAggregator[Input, IR, Output])
    extends ColumnAggregator {
  type IrMap = util.HashMap[String, IR]
  protected def castIr(ir: Any): IrMap = ir.asInstanceOf[IrMap]
  final override def merge(ir1: Any, ir2: Any): Any = {
    if (ir2 == null) return ir1
    // we need to clone here because the contract is to only mutate ir1
    // ir2 can itself be expected to mutate later - and hence has to retain its value
    val rightMap = castIr(ir2)
    val rightIter = rightMap.entrySet().iterator()

    def clone(ir: IR): IR = if (ir == null) ir else agg.clone(ir)

    if (ir1 == null) {
      val rightClone = new IrMap
      while (rightIter.hasNext) {
        val entry = rightIter.next()
        rightClone.put(entry.getKey, clone(entry.getValue))
      }
      return rightClone
    }

    val leftMap = castIr(ir1)
    while (rightIter.hasNext) {
      val entry = rightIter.next()
      val bucket = entry.getKey
      val rightIr = entry.getValue
      if (rightIr != null) {
        val leftIr = leftMap.get(bucket)
        if (leftIr == null) {
          leftMap.put(bucket, clone(rightIr))
        } else {
          leftMap.put(bucket, agg.merge(leftIr, rightIr))
        }
      }
    }
    leftMap
  }

  private def guardedApply[ValueType, NewValueType](f: ValueType => NewValueType, ir: Any): Any = {
    if (ir == null) return null
    val iter = ir.asInstanceOf[util.HashMap[String, ValueType]].entrySet().iterator()
    val result = new util.HashMap[String, NewValueType]()
    while (iter.hasNext) {
      val entry = iter.next()
      result.put(entry.getKey, f(entry.getValue))
    }
    result
  }

  final override def finalize(ir: Any): Any = guardedApply(agg.finalize, ir)

  final override def normalize(ir: Any): Any = guardedApply(agg.normalize, ir)

  final override def denormalize(ir: Any): Any = guardedApply(agg.denormalize, ir)

  final override def clone(ir: Any): Any = guardedApply(agg.clone, ir)

  final override def outputType: DataType = MapType(StringType, agg.outputType)

  final override def irType: DataType = MapType(StringType, agg.irType)

  final override def isDeletable: Boolean = agg.isDeletable
}
