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

  // Process tensor elements with a consumer function to avoid tuple allocation from zipWithIndex.
  // This avoids creating tuple objects on each iteration.
  private def processTensorElements(inputRow: Row, consumer: (Int, Input) => Unit): Boolean = {
    val inputVal = inputRow.get(columnIndices.input)
    if (inputVal == null) return false

    inputVal match {
      case inputJList: util.ArrayList[Any] =>
        var idx = 0
        val size = inputJList.size()
        while (idx < size) {
          val value = inputJList.get(idx)
          if (value == null)
            throw new IllegalArgumentException("Null values are not allowed within tensors")
          consumer(idx, toTypedInput(value))
          idx += 1
        }
        true
      case inputSeq: collection.Seq[Any] =>
        var idx = 0
        val size = inputSeq.size
        while (idx < size) {
          val value = inputSeq(idx)
          if (value == null)
            throw new IllegalArgumentException("Null values are not allowed within tensors")
          consumer(idx, toTypedInput(value))
          idx += 1
        }
        true
      case _ => false
    }
  }

  def guardedApply(inputRow: Row, prepare: Input => IR, update: (IR, Input) => IR, irRow: Any = null): Any = {
    val inputVal = inputRow.get(columnIndices.input)
    if (inputVal == null) return irRow

    assert(irRow != null, "The IR row cannot be null when it reaches column aggregator")
    val typedRow = irRow.asInstanceOf[Array[Any]]
    val irVal = typedRow(columnIndices.output)

    // Determine the size needed for the result list
    val tensorSize = inputVal match {
      case jList: util.ArrayList[_] => jList.size()
      case seq: collection.Seq[_]   => seq.size
      case _                        => return irRow
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

    // Process elements without tuple allocation
    processTensorElements(
      inputRow,
      (idx: Int, value: Input) => {
        val ir = resultList.get(idx)
        if (ir == null) {
          resultList.set(idx, prepare(value))
        } else {
          resultList.set(idx, update(ir.asInstanceOf[IR], value))
        }
      }
    )

    resultList
  }

  override def update(ir: Array[Any], inputRow: Row): Unit = guardedApply(inputRow, agg.prepare, agg.update, ir)

  override def delete(ir: Array[Any], inputRow: Row): Unit = guardedApply(inputRow, agg.inversePrepare, agg.delete, ir)
}
