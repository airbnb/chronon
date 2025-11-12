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

import ai.chronon.aggregator.base.SimpleAggregator
import ai.chronon.api.Row

import java.util
import scala.util.ScalaJavaConversions.IteratorOps

class ElementWiseAggregator[Input, IR, Output](agg: SimpleAggregator[Input, IR, Output],
                                               columnIndices: ColumnIndices,
                                               toTypedInput: Any => Input)
    extends ElementWiseAggregatorBase(agg) {

  def tensorIterator(inputRow: Row): Iterator[(Int, Input)] = {
    val inputVal = inputRow.get(columnIndices.input)
    if (inputVal == null) return null
    inputVal match {
      case inputJList: util.ArrayList[Any] =>
        inputJList
          .iterator()
          .toScala
          .zipWithIndex
          .map {
            case (value, idx) =>
              if (value == null)
                throw new IllegalArgumentException("Null values are not allowed within tensors")
              idx -> toTypedInput(value)
          }
      case inputSeq: collection.Seq[Any] =>
        inputSeq.iterator.zipWithIndex.map {
          case (value, idx) =>
            if (value == null)
              throw new IllegalArgumentException("Null values are not allowed within tensors")
            idx -> toTypedInput(value)
        }
    }
  }

  def guardedApply(inputRow: Row, prepare: Input => IR, update: (IR, Input) => IR, irRow: Any = null): Any = {
    val it = tensorIterator(inputRow)
    if (it == null) return irRow
    assert(irRow != null, "The IR row cannot be null when it reaches column aggregator")
    val typedRow = irRow.asInstanceOf[Array[Any]]
    val irVal = typedRow(columnIndices.output)

    // Determine the size needed for the result list
    val inputVal = inputRow.get(columnIndices.input)
    val tensorSize = inputVal match {
      case jList: util.ArrayList[_] => jList.size()
      case seq: collection.Seq[_]   => seq.size
      case _                        => 0
    }

    val resultList = if (irVal == null) {
      val ir = new util.ArrayList[Any](tensorSize)
      // Initialize with nulls to match tensor size
      var i = 0
      while (i < tensorSize) {
        ir.add(null)
        i += 1
      }
      typedRow.update(columnIndices.output, ir)
      ir
    } else {
      val existing = irVal.asInstanceOf[util.ArrayList[Any]]
      // Verify dimensions match
      if (existing.size() != tensorSize) {
        throw new IllegalStateException(
          s"Tensor dimensions must match: existing=${existing.size()}, input=$tensorSize"
        )
      }
      existing
    }

    while (it.hasNext) {
      val entry = it.next()
      val idx = entry._1
      val value = entry._2
      val ir = resultList.get(idx)
      if (ir == null) {
        resultList.set(idx, prepare(value))
      } else {
        resultList.set(idx, update(ir.asInstanceOf[IR], value))
      }
    }
    resultList
  }

  override def update(ir: Array[Any], inputRow: Row): Unit = guardedApply(inputRow, agg.prepare, agg.update, ir)

  override def delete(ir: Array[Any], inputRow: Row): Unit = guardedApply(inputRow, agg.inversePrepare, agg.delete, ir)
}
