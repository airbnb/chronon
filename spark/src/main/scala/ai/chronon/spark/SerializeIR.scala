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

package ai.chronon.spark

import ai.chronon.aggregator.row.RowAggregator
import ai.chronon.online.SparkConversions
import org.apache.spark.sql.Row

/**
  * Utilities for serializing/deserializing Chronon IR values to/from Spark DataFrames.
  *
  * Key concepts:
  * - IR tables have MULTIPLE COLUMNS, each with a different IR type
  * - Example: [count:Long, sum:Double, avg:Struct(sum, count)]
  * - Each column is converted independently based on its IR type
  *
  * This bridges between:
  * - Java IR types (internal aggregator state: HashSet, CpcSketch, etc.)
  * - Normalized IR types (serializable format: Array, ArrayList, bytes)
  * - Spark native types (DataFrame columns: primitives, Row, WrappedArray, Map)
  *
  * The conversion pipeline:
  * Writing: Java IR → normalize() → toSparkRow() → Spark Row → DataFrame
  * Reading: DataFrame → Spark Row → fromSparkRow() → denormalize() → Java IR
  */
object SerializeIR {

  /**
    * Converts a Chronon IR array to Spark-native format for DataFrame writing.
    *
    * Processes each column independently based on its IR type from
    * RowAggregator.incrementalOutputSchema.
    *
    * @param ir The IR array from RowAggregator (Java types)
    * @param rowAgg The RowAggregator with column schemas
    * @return Array where each element is in Spark-native format
    */
  def toSparkRow(ir: Array[Any], rowAgg: RowAggregator): Array[Any] = {
    // Step 1: Normalize (Java types → serializable types)
    val normalized = rowAgg.normalize(ir)

    // Step 2: Convert each column to Spark-native type
    val sparkColumns = new Array[Any](normalized.length)
    rowAgg.incrementalOutputSchema.zipWithIndex.foreach {
      case ((_, irType), idx) =>
        sparkColumns(idx) = SparkConversions.toSparkRow(normalized(idx), irType)
    }
    sparkColumns
  }

  /**
    * Converts Spark DataFrame Row to Chronon IR format for aggregation.
    *
    * Reads each IR column from the Spark Row by name and converts based on IR type.
    * Uses RowAggregator.incrementalOutputSchema to get both column names and types.
    *
    * @param sparkRow The Spark Row from DataFrame.read()
    * @param rowAgg The RowAggregator with IR schemas
    * @return Denormalized IR array ready for merge() (Java types)
    */
  def fromSparkRow(sparkRow: Row, rowAgg: RowAggregator): Array[Any] = {
    val normalized = new Array[Any](rowAgg.incrementalOutputSchema.length)

    // Step 1: Extract each IR column from Spark Row by name
    rowAgg.incrementalOutputSchema.zipWithIndex.foreach {
      case ((colName, irType), idx) =>
        // Get column from Spark Row by NAME
        val sparkValue = sparkRow.getAs[Any](colName)
        // Convert using IR type
        normalized(idx) = SparkConversions.fromSparkValue(sparkValue, irType)
    }

    // Step 2: Denormalize (serializable types → Java types)
    rowAgg.denormalize(normalized)
  }

  /**
    * Alternative: Extract IR columns by position instead of name.
    * Faster but requires column order to match exactly.
    *
    * @param sparkRow The Spark Row from DataFrame
    * @param rowAgg The RowAggregator with IR schemas
    * @param startIndex The starting index of IR columns in the Row
    * @return Denormalized IR array (Java types)
    */
  def fromSparkRowByPosition(
      sparkRow: Row,
      rowAgg: RowAggregator,
      startIndex: Int
  ): Array[Any] = {
    val normalized = new Array[Any](rowAgg.incrementalOutputSchema.length)

    // Step 1: Extract each IR column by position
    rowAgg.incrementalOutputSchema.zipWithIndex.foreach {
      case ((_, irType), idx) =>
        val sparkValue = sparkRow.get(startIndex + idx)
        normalized(idx) = SparkConversions.fromSparkValue(sparkValue, irType)
    }

    // Step 2: Denormalize
    rowAgg.denormalize(normalized)
  }
}
