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

class MapColumnAggregator[Input, IR, Output](agg: SimpleAggregator[Input, IR, Output],
                                             columnIndices: ColumnIndices,
                                             toTypedInput: Any => Input)
    extends MapColumnAggregatorBase(agg) {

  def mapIterator(inputRow: Row): Iterator[(String, Input)] = {
    val inputVal = inputRow.get(columnIndices.input)
    if (inputVal == null) return null
    inputVal match {
      case inputJMap: util.Map[String, Any] =>
        inputJMap
          .entrySet()
          .iterator()
          .toScala
          .filter(_.getValue != null)
          .map(e => e.getKey -> toTypedInput(e.getValue))
      case inputMap: Map[String, Any] =>
        inputMap.iterator.filter(_._2 != null).map(e => e._1 -> toTypedInput(e._2))
    }
  }

  def guardedApply(inputRow: Row, prepare: Input => IR, update: (IR, Input) => IR, irRow: Any = null): Any = {
    val it = mapIterator(inputRow)
    if (it == null) return irRow
    assert(irRow != null, "The IR row cannot be null when it reaches column aggregator")
    val typedRow = irRow.asInstanceOf[Array[Any]]
    val irVal = typedRow(columnIndices.output)
    val resultMap = if (irVal == null) {
      val ir = new util.HashMap[String, Any]()
      typedRow.update(columnIndices.output, ir)
      ir
    } else {
      irVal.asInstanceOf[util.Map[String, Any]]
    }
    while (it.hasNext) {
      val entry = it.next()
      val key = entry._1
      val value = entry._2
      val ir = resultMap.get(entry._1)
      if (ir == null) {
        resultMap.put(key, prepare(value))
      } else {
        resultMap.put(key, update(ir.asInstanceOf[IR], value))
      }
    }
    resultMap
  }
  override def update(ir: Array[Any], inputRow: Row): Unit = guardedApply(inputRow, agg.prepare, agg.update, ir)

  override def delete(ir: Array[Any], inputRow: Row): Unit = guardedApply(inputRow, agg.inversePrepare, agg.delete, ir)
}
