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

import ai.chronon.api.Extensions.WindowOps
import ai.chronon.api._

import java.util
import scala.collection.{Seq, mutable}

case class BatchIr(collapsed: Array[Any], tailHops: HopsAggregator.IrMapType)
case class FinalBatchIr(collapsed: Array[Any], tailHops: HopsAggregator.OutputArrayType)

/**
  * Mutations processing starts with an end of the day snapshot FinalBatchIR.
  * On top of this FinalBatchIR mutations are processed.
  *
  *
  * update/merge/finalize are related to snapshot data. As such they follow the snapshot Schema
  * and aggregators.
  * However mutations come into play later in the group by and a finalized version of the snapshot
  * data is created to be processed with the mutations rows.
  * Since the dataframe inputs are aligned between mutations and snapshot (input) no additional schema is needed.
  *
  */
class SawtoothMutationAggregator(aggregations: Seq[Aggregation],
                                 inputSchema: Seq[(String, DataType)],
                                 resolution: Resolution = FiveMinuteResolution,
                                 tailBufferMillis: Long = new Window(2, TimeUnit.DAYS).millis)
    extends SawtoothAggregator(aggregations: Seq[Aggregation],
                               inputSchema: Seq[(String, DataType)],
                               resolution: Resolution) {

  val hopsAggregator = new HopsAggregatorBase(aggregations, inputSchema, resolution)

  def batchIrSchema: Array[(String, DataType)] = {
    val collapsedSchema = windowedAggregator.irSchema
    val hopFields = baseAggregator.irSchema :+ ("ts", LongType)
    Array("collapsedIr" -> StructType.from("WindowedIr", collapsedSchema),
          "tailHopIrs" -> ListType(ListType(StructType.from("HopIr", hopFields))))
  }

  def tailTs(batchEndTs: Long): Array[Option[Long]] =
    windowMappings.map { mapping => Option(mapping.aggregationPart.window).map { batchEndTs - _.millis } }

  def init: BatchIr = BatchIr(Array.fill(windowedAggregator.length)(null), hopsAggregator.init())

  def update(batchEndTs: Long, batchIr: BatchIr, row: Row): BatchIr = {
    val batchTails = tailTs(batchEndTs)
    update(batchEndTs: Long, batchIr: BatchIr, row: Row, batchTails)
  }

  def update(batchEndTs: Long, batchIr: BatchIr, row: Row, batchTails: Array[Option[Long]]): BatchIr = {
    val rowTs = row.ts
    // To track if a tail value for a particular (hopIndex, baseIrIndex) is updated
    val updatedFlagsBitset = new util.BitSet(hopSizes.length * baseAggregator.length)
    def setIfNot(hopIndex: Int, baseIrIndex: Int): Boolean = {
      val flatIndex = (baseIrIndex * hopSizes.length) + hopIndex
      val isSet = updatedFlagsBitset.get(flatIndex)
      if (!isSet) { updatedFlagsBitset.set(flatIndex, true) }
      isSet
    }

    var i = 0
    while (i < windowedAggregator.length) {
      if (batchEndTs > rowTs && batchTails(i).forall(rowTs > _)) { // relevant for the window
        if (batchTails(i).forall(rowTs >= _ + tailBufferMillis)) { // update collapsed part
          windowedAggregator.columnAggregators(i).update(batchIr.collapsed, row)
        } else { // update tailHops part
          val hopIndex = tailHopIndices(i)
          // eg., 7d, 8d windows shouldn't update the same 1hr tail hop twice
          // so update a hop only once
          val baseIrIndex = baseIrIndices(i)
          if (!setIfNot(hopIndex, baseIrIndex)) {
            val hopStart = TsUtils.round(rowTs, hopSizes(hopIndex))
            val hopIr = batchIr.tailHops(hopIndex).computeIfAbsent(hopStart, hopsAggregator.javaBuildHop)
            baseAggregator.columnAggregators(baseIrIndex).update(hopIr, row)
          }
        }
      }
      i += 1
    }
    batchIr
  }

  def merge(batchIr1: BatchIr, batchIr2: BatchIr): BatchIr =
    BatchIr(windowedAggregator.merge(batchIr1.collapsed, batchIr2.collapsed),
            hopsAggregator.merge(batchIr1.tailHops, batchIr2.tailHops))

  // Ready the snapshot aggregated data to be merged with mutations data.
  def finalizeSnapshot(batchIr: BatchIr): FinalBatchIr =
    FinalBatchIr(batchIr.collapsed, Option(batchIr.tailHops).map(hopsAggregator.toTimeSortedArray).orNull)

  /**
    * Go through the aggregators and update or delete the intermediate with the information of the row if relevant.
    * Useful for both online and mutations
    */
  def updateIr(ir: Array[Any], row: Row, queryTs: Long, hasReversal: Boolean = false) = {
    var i: Int = 0
    while (i < windowedAggregator.length) {
      val window = windowMappings(i).aggregationPart.window
      val hopIndex = tailHopIndices(i)
      val rowInWindow = (row.ts >= TsUtils.round(queryTs - window.millis, hopSizes(hopIndex)) && row.ts < queryTs)
      if (window == null || rowInWindow) {
        if (hasReversal && row.isBefore) {
          windowedAggregator(i).delete(ir, row)
        } else {
          windowedAggregator(i).update(ir, row)
        }
      }
      i += 1
    }
  }

  def updateIrTiled(ir: Array[Any], otherIr: TiledIr, queryTs: Long) = {
    val otherIrTs = otherIr.ts
    var i: Int = 0
    while (i < windowedAggregator.length) {
      val window = windowMappings(i).aggregationPart.window
      val hopIndex = tailHopIndices(i)
      val irInWindow =
        (otherIrTs >= TsUtils.round(queryTs - window.millis, hopSizes(hopIndex)) && otherIrTs < queryTs)
      if (window == null || irInWindow) {
        ir(i) = windowedAggregator(i).merge(ir(i), otherIr.ir(i))
      }
      i += 1
    }
  }

  /**
    * Update the intermediate results with tail hops data from a FinalBatchIr.
    */
  def mergeTailHops(ir: Array[Any], queryTs: Long, batchEndTs: Long, batchIr: FinalBatchIr): Array[Any] = {
    var i: Int = 0
    while (i < windowedAggregator.length) {
      val window = windowMappings(i).aggregationPart.window
      if (window != null) { // no hops for unwindowed
        val hopIndex = tailHopIndices(i)
        val queryTail = TsUtils.round(queryTs - window.millis, hopSizes(hopIndex))
        val hopIrs = batchIr.tailHops(hopIndex)
        val relevantHops = mutable.ArrayBuffer[Any](ir(i))
        var idx: Int = 0
        while (idx < hopIrs.length) {
          val hopIr = hopIrs(idx)
          val hopStart = hopIr.last.asInstanceOf[Long]
          if ((batchEndTs - window.millis) + tailBufferMillis > hopStart && hopStart >= queryTail) {
            relevantHops += hopIr(baseIrIndices(i))
          }
          idx += 1
        }
        val merged = windowedAggregator(i).bulkMerge(relevantHops.iterator)
        ir.update(i, merged)
      }
      i += 1
    }
    ir
  }

  /**
    * Given aggregations FinalBatchIRs at the end of the Snapshot (batchEndTs) and mutation and query times,
    * determine the values at the query times for the aggregations.
    * This is pretty much a mix of online with extra work for multiple queries ts support.
    */
  def lambdaAggregateIrMany(batchEndTs: Long,
                            finalBatchIr: FinalBatchIr,
                            sortedInputs: Array[Row],
                            sortedEndTimes: Array[Long]): Array[Array[Any]] = {
    if (sortedEndTimes == null) return null
    val batchIr = Option(finalBatchIr).getOrElse(finalizeSnapshot(init))
    val result = Array.fill[Array[Any]](sortedEndTimes.length)(null)
    // single-pass two cursors across sorted queries & sorted inputs
    var queryIdx = 0
    var inputIdx = 0

    var cumulatedIr = windowedAggregator.clone(batchIr.collapsed)
    val inputLength = if (sortedInputs == null) 0 else sortedInputs.length

    // forward scan inputIdx until mutationTs becomes relevant
    while (inputIdx < inputLength && batchEndTs > sortedInputs(inputIdx).mutationTs) { inputIdx += 1 }

    while (queryIdx < sortedEndTimes.length) {
      val queryTs = sortedEndTimes(queryIdx)
      while (inputIdx < inputLength && sortedInputs(inputIdx).mutationTs < queryTs) {
        val row = sortedInputs(inputIdx)
        updateIr(cumulatedIr, row, queryTs, true)
        inputIdx += 1
      }
      result.update(queryIdx, cumulatedIr)
      cumulatedIr = windowedAggregator.clone(cumulatedIr)
      queryIdx += 1
    }

    // Tail hops contain the window information that needs to be merged to the result.
    var i: Int = 0
    while (i < sortedEndTimes.length) {
      val queryTs = sortedEndTimes(i)
      mergeTailHops(result(i), queryTs, batchEndTs, batchIr)
      i += 1
    }
    result
  }
}
