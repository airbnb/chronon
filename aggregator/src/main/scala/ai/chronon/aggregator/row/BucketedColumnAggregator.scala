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
import ai.chronon.api.Row

import scala.collection.mutable

class BucketedColumnAggregator[Input, IR, Output](agg: BaseAggregator[Input, IR, Output],
                                                  columnIndices: ColumnIndices,
                                                  bucketIndex: Int,
                                                  rowUpdater: Dispatcher[Input, Any])
    extends MapColumnAggregatorBase(agg) {

  override def update(ir: Array[Any], inputRow: Row): Unit = {
    val inputVal = inputRow.get(columnIndices.input)
    if (inputVal == null) return // null inputs are ignored

    val bucketVal = inputRow.get(bucketIndex)
    if (bucketVal == null) return // null buckets are ignored

    val bucket = bucketVal.asInstanceOf[String]
    val previousMap = ir(columnIndices.output)

    if (previousMap == null) { // map is absent
      val prepared = rowUpdater.prepare(inputRow)
      if (prepared != null) {
        val newMap = new IrMap()
        newMap.put(bucket, prepared.asInstanceOf[IR])
        ir.update(columnIndices.output, newMap)
      }
      return
    }

    val map = castIr(previousMap)
    if (!map.containsKey(bucket)) { // bucket is absent - so init
      val ir = rowUpdater.prepare(inputRow)
      if (ir != null) {
        map.put(bucket, ir.asInstanceOf[IR])
      }
      return
    }

    val updated = rowUpdater.updateColumn(map.get(bucket), inputRow)
    if (updated != null)
      map.put(bucket, updated.asInstanceOf[IR])
  }

  override def delete(ir: Array[Any], inputRow: Row): Unit = {
    if (!agg.isDeletable) return

    val inputVal = inputRow.get(columnIndices.input)
    if (inputVal == null) return // null inputs are ignored

    val bucketVal = inputRow.get(bucketIndex)
    if (bucketVal == null) return // null buckets are ignored

    val bucket = bucketVal.asInstanceOf[String]
    val previousMap = ir(columnIndices.output)
    lazy val prepared = rowUpdater.inversePrepare(inputRow)
    if (previousMap == null) { // map is absent
      if (prepared != null) {
        val map = mutable.HashMap[String, IR](bucket -> prepared.asInstanceOf[IR])
        ir.update(columnIndices.output, map)
      }
      return
    }

    val map = castIr(previousMap)
    if (!map.containsKey(bucket)) { // bucket is absent - so init
      if (prepared != null)
        map.put(bucket, prepared.asInstanceOf[IR])
      return
    }

    val updated = rowUpdater.deleteColumn(map.get(bucket), inputRow)
    if (updated != null)
      map.put(bucket, updated.asInstanceOf[IR])
  }
}
