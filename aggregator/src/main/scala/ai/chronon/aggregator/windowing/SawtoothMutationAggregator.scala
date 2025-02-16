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
                                 val tailBufferMillis: Long = new Window(2, TimeUnit.DAYS).millis)
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


  def updateIrTiledWithTileLayering(ir: Array[Any],
                                    headStreamingTiledIrs: Seq[TiledIr],
                                    queryTs: Long,
                                    batchEndTs: Long) = {
    // This works as a sliding window over the sorted list of tiles, where the sort is done by
    // timestamp in ascending fashion (oldest -> newest tiles), and then secondarily done by
    // tile size. So for example this becomes:
    //   [ [12:00, 1:00), [12:00, 12:05), [12:05, 12:10), ... , [12:55, 1:00), [1:00, 2:00), [1:00, 1:05), ... ]
    // And when a tile is selected, we increment our index pointer to the first tile where the start is greater
    // than or equal to the end of the tile we just selected. So if we select the [12:00, 1:00) tile, then the pointer
    // will move to the [1:00, 2:00) tile.

    // fill 2D array where each entry corresponds to the windowedAggregator column aggregators, and contains an array of
    // the streaming tiles we need to aggregate
    // ex) mergeBuffers = [ [1,2,3], [2,4] ]
    //     windowedAggregators.columnAggregators = [ Sum, Average ]
    //     resultIr = [ 6, 3 ]

    val sortedStreamingTiles = headStreamingTiledIrs.sortBy { tile => (tile.ts, -tile.size) }
    val mergeBuffers = Array.fill(windowedAggregator.length)(mutable.ArrayBuffer.empty[Any])

    var i: Int = 0
    while (i < windowedAggregator.length) {
      val window = windowMappings(i).aggregationPart.window
      // tailHopIndices is the list of indices for the hopSizes array for which hopSize a given aggregator is using.
      // I.e., At tailHopIndices(i) we will see a number that is the index in hopSizes for the aggregator's hop size.
      // For example, if we are using a Five Minute Resolution window then for this instance of SawtoothAggregator,
      // the hopSizes(tailHopIndices(i)), where i = 0 is one day, i = 1 is one hour, and i = 2 is five minutes.
      // In practice, when we have a window with 5 minute tile sizes, then this is always 5 minutes.
      // If we have a window with 1 hour tile sizes, then this is always 1 hour. And if we have a window with 1 day
      // tile sizes, then this is always 1 day.
      // However, we still need to use this hop size instead of the tile size because we want to correctly exclude 1 hour tiles
      // that we may have in addition to 5 minute tiles.
      // For example, if we have a window with 5 minute resolution that starts at 12:06pm, then when considering
      // the 5 minute tiles, we round down to 12:05pm and will correctly include the 12:05pm 5 minute tile. And when
      // considering the 1 hour tiles, we round down to 12:05pm and will correctly exclude the 12:00pm 1 hour tile if it exists.
      val hopIndex = tailHopIndices(i)

      var tilePtr: Int = 0
      while (tilePtr < sortedStreamingTiles.length) {
        val tile = sortedStreamingTiles(tilePtr)
        val tileTs = tile.ts

        if (tileTs >= batchEndTs) {
          // If the tile's timestamp is after the start of the window rounded to the nearest tile border (e.g., five minutes, one hour, one day),
          // and if the tile's timestamp is before the query timestamp, we should include this tile in this aggregator.
          // Or if the aggregator has no window (infinite), we should include this tile.
          val irInWindow = (tileTs >= TsUtils.round(queryTs - window.millis, hopSizes(hopIndex)) && tileTs < queryTs)
          if (window == null || irInWindow) {
            mergeBuffers(i) += tile.ir(i)

            // Adjust tilePtr to next window. E.g., if we just selected a tile covering [12:00, 1:00), the next tile we
            // select will start at 1:00 and we will not consider any other tiles between [12:00, 1:00).
            while (tilePtr < sortedStreamingTiles.length && sortedStreamingTiles(tilePtr).ts < (tileTs + tile.size)) {
              tilePtr += 1
            }
          } else {
            // If the current tile was not in the window for the aggregator, just continue to the next tile.
            tilePtr += 1
          }
        } else {
          // If the current tile came before batchEndTs, just continue to the next tile.
          tilePtr += 1
        }
      }

      i += 1
    }

    var idx: Int = 0
    while (idx < mergeBuffers.length) {
      // include collapsed batchIr in bulkMerge computation
      mergeBuffers(idx) += ir(idx)
      ir(idx) = windowedAggregator(idx).bulkMerge(mergeBuffers(idx).iterator)
      idx += 1
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
