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

package ai.chronon.spark.serde

import ai.chronon.aggregator.row.ColumnAggregator
import ai.chronon.aggregator.windowing.HopsAggregator.{HopIr, IrMapType}
import ai.chronon.online.serde.SparkConversions
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.apache.spark.sql.types.{ArrayType, DataType => SparkDataType, LongType, StructField, StructType}

import java.util
import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

/**
  * Hop-level serializer for HopsAggregator IR structures to/from Spark types.
  *
  * This handles serialization of temporal hop data during shuffle operations. Hops are the
  * intermediate temporal buckets that get composed into final windows. Each hop contains
  * aggregated IR (Intermediate Representation) state for a specific time granularity.
  *
  * == Input Structure (IrMapType) ==
  *
  * IrMapType = Array[HashMap[Long, HopIr]]
  *
  * Where:
  * - Outer Array: one HashMap per hop size (e.g., daily, hourly, 5-minute)
  * - HashMap key: hop start timestamp in milliseconds
  * - HopIr: Array[Any] = [IR1, IR2, ..., IRN, timestamp]
  *   - Elements 0 to N-1: IR values for each aggregation (SUM IR, AVG IR, etc.)
  *   - Element N: hop start timestamp (duplicated for sorting purposes)
  *
  * Example with 2 aggregations (SUM, AVERAGE) and 2 hop sizes:
  * {{{
  * Array(
  *   HashMap(  // Daily hops (86,400,000 ms)
  *     1704067200000L → [sumIR, avgIR, 1704067200000L],  // Jan 1, 2024
  *     1704153600000L → [sumIR, avgIR, 1704153600000L]   // Jan 2, 2024
  *   ),
  *   HashMap(  // Hourly hops (3,600,000 ms)
  *     1704067200000L → [sumIR, avgIR, 1704067200000L],  // Jan 1, 00:00
  *     1704070800000L → [sumIR, avgIR, 1704070800000L]   // Jan 1, 01:00
  *   )
  * )
  * }}}
  *
  * == Output Structure (Flattened) ==
  *
  * Iterator[Row] where each Row is: (hop_size, timestamp, ir_0, ir_1, ..., ir_n)
  *
  * Fields:
  * - hop_size: Long - temporal granularity in milliseconds (86,400,000 = 1 day)
  * - timestamp: Long - hop start timestamp in milliseconds
  * - ir_0 to ir_n: IR values serialized using ColumnIRSerializer
  *
  * The same example serialized (as Iterator):
  * {{{
  * Iterator(
  *   Row(86400000L, 1704067200000L, sumIR_spark, avgIR_spark),  // Day 1 daily hop
  *   Row(86400000L, 1704153600000L, sumIR_spark, avgIR_spark),  // Day 2 daily hop
  *   Row(3600000L,  1704067200000L, sumIR_spark, avgIR_spark),  // Day 1, hour 0
  *   Row(3600000L,  1704070800000L, sumIR_spark, avgIR_spark)   // Day 1, hour 1
  * )
  * }}}
  *
  * == Why Flatten? ==
  *
  * The flattened structure with explicit hop_size makes data self-describing:
  * - Can inspect serialized data and immediately understand temporal granularity
  * - Deserializer validates hop sizes match expected Resolution
  * - Independent of array index coordination between contexts
  * - More natural for Spark DataFrame operations (filtering, grouping by hop_size)
  *
  * == Spark Schema ==
  *
  * {{{
  * ArrayType(
  *   StructType(
  *     StructField("hop_size", LongType, nullable = false),
  *     StructField("timestamp", LongType, nullable = false),
  *     StructField("ir_0", <IR type>, nullable = true),
  *     StructField("ir_1", <IR type>, nullable = true),
  *     ...
  *   )
  * )
  * }}}
  */
object HopsIRSerializer {

  /**
    * Serialize HopsAggregator's IrMapType to flattened Spark row iterator.
    *
    * Lazily flattens the nested structure into self-describing rows where each row contains:
    * (hop_size, timestamp, ir_0, ir_1, ..., ir_n)
    *
    * No intermediate collections are created - rows are produced on-demand as the iterator
    * is consumed. This is critical for shuffle performance where serialization is a bottleneck.
    *
    * @param irMapType The IrMapType from HopsAggregator (Array[HashMap[Long, HopIr]])
    * @param hopSizes The hop sizes array from Resolution (makes hop size explicit)
    * @param columnAggs The ColumnAggregators that define the IR types
    * @return Iterator of Spark Rows, consumed lazily during DataFrame/shuffle operations
    */
  def toSparkRows(irMapType: IrMapType, hopSizes: Array[Long], columnAggs: Array[ColumnAggregator]): Iterator[Row] = {
    if (irMapType == null) return null

    require(irMapType.length == hopSizes.length,
            s"IrMapType length ${irMapType.length} must match hop sizes length ${hopSizes.length}")

    irMapType.iterator.zip(hopSizes.iterator).flatMap {
      case (hopMap, hopSize) =>
        if (hopMap == null || hopMap.isEmpty) {
          Iterator.empty
        } else {
          // For each timestamp in this hop size
          hopMap.asScala.iterator.map {
            case (timestamp, hopIr) =>
              // HopIr = [IR1, IR2, ..., IRN, timestamp]
              // Extract IR values (all except last element which is timestamp)
              val irValues = hopIr.take(columnAggs.length)

              // Serialize IR values
              val sparkIRValues = ColumnIRSerializer.toSparkValues(irValues, columnAggs)

              // Build row: (hop_size, timestamp, ir_0, ir_1, ..., ir_n)
              val rowValues = new Array[Any](sparkIRValues.length + 2)
              rowValues(0) = hopSize.asInstanceOf[Any]
              rowValues(1) = timestamp.asInstanceOf[Any]
              System.arraycopy(sparkIRValues, 0, rowValues, 2, sparkIRValues.length)
              new GenericRow(rowValues)
          }
        }
    }
  }

  /**
    * Deserialize flattened Spark rows back to HopsAggregator's IrMapType.
    *
    * Reconstructs the nested structure by grouping rows by hop_size field.
    * Accepts any traversable collection of rows (Iterator, Array, Seq, etc.)
    *
    * @param sparkRows The traversable collection of Spark Rows (Iterator, Array, Seq, etc.)
    * @param hopSizes The expected hop sizes from Resolution (for validation)
    * @param columnAggs The ColumnAggregators that define the IR types
    * @return IrMapType that can be used by HopsAggregator
    */
  def fromSparkRows(sparkRows: TraversableOnce[Row],
                    hopSizes: Array[Long],
                    columnAggs: Array[ColumnAggregator]): IrMapType = {
    if (sparkRows == null) return null

    // Initialize IrMapType structure
    val irMapType = Array.fill(hopSizes.length)(new util.HashMap[Long, HopIr]())

    // Group rows by hop size and reconstruct structure
    sparkRows.foreach { row =>
      val hopSize = row.getLong(0)
      val timestamp = row.getLong(1)

      // Find the hop size index. hopSizes is currently size 3 (daily, hourly, 5-min), so
      // linear indexOf is fine. Sub-hourly windows will increase this to 5 — may warrant
      // revisiting if it grows further.
      val hopSizeIdx = hopSizes.indexOf(hopSize)
      require(hopSizeIdx >= 0, s"Hop size $hopSize not found in expected hop sizes: ${hopSizes.mkString(", ")}")

      // Extract IR values (skip first two fields: hop_size and timestamp)
      val sparkIRValues = (2 until row.length).map(row.get).toArray
      val irValues = ColumnIRSerializer.fromSparkValues(sparkIRValues, columnAggs)

      // Reconstruct HopIr = [IR1, IR2, ..., IRN, timestamp]
      val hopIr = new Array[Any](columnAggs.length + 1)
      Array.copy(irValues, 0, hopIr, 0, irValues.length)
      hopIr(columnAggs.length) = timestamp

      // Add to appropriate HashMap
      irMapType(hopSizeIdx).put(timestamp, hopIr)
    }

    irMapType
  }

  /**
    * Get the Spark schema for the flattened hop structure.
    *
    * Schema: ArrayType(StructType(hop_size, timestamp, ir_0, ir_1, ..., ir_n))
    *
    * @param columnAggs The ColumnAggregators that define the IR types
    * @return Spark ArrayType schema
    */
  def schema(columnAggs: Array[ColumnAggregator]): ArrayType = {
    val fields = ArrayBuffer[StructField](
      StructField("hop_size", LongType, nullable = false),
      StructField("timestamp", LongType, nullable = false)
    )

    // Add IR fields
    fields ++= columnAggs.zipWithIndex.map {
      case (agg, idx) =>
        StructField(s"ir_$idx", SparkConversions.fromChrononType(agg.irType), nullable = true)
    }

    ArrayType(StructType(fields.toSeq), containsNull = false)
  }

  /**
    * Get the full Spark StructType schema wrapping the hops array.
    *
    * Useful for DataFrame operations.
    *
    * @param columnAggs The ColumnAggregators that define the IR types
    * @return Spark StructType schema
    */
  def structSchema(columnAggs: Array[ColumnAggregator]): StructType = {
    StructType(Seq(StructField("hops", schema(columnAggs), nullable = true)))
  }
}
