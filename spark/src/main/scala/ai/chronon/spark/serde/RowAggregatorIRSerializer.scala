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

import ai.chronon.aggregator.row.{ColumnAggregator, RowAggregator}
import ai.chronon.online.serde.SparkConversions
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.apache.spark.sql.types.{StructField, StructType}

/**
  * Row-level serializer for RowAggregator IR arrays to/from Spark types.
  *
  * This handles straightforward serialization of the complete IR array from a RowAggregator,
  * delegating per-column conversion to ColumnIRSerializer.
  *
  * Architecture:
  * 1. Takes the full IR array from RowAggregator (one IR value per ColumnAggregator)
  * 2. Uses ColumnIRSerializer to convert each IR value to/from Spark types
  * 3. Packages the result as a Spark Row with appropriate schema
  *
  * No deduplication or storage optimization happens here—that belongs at the hop level
  * where temporal bucketing creates sharing opportunities (see HopsAggregator).
  * This serializer is used for non-windowed aggregations or post-window-composition data.
  */
object RowAggregatorIRSerializer {

  /**
    * Serialize a RowAggregator's IR array to a Spark Row.
    *
    * @param irArray The IR array from RowAggregator (one IR value per ColumnAggregator)
    * @param columnAggs The ColumnAggregators that define the types
    * @return Spark Row ready for DataFrame storage or shuffle
    */
  def toSparkRow(irArray: Array[Any], columnAggs: Array[ColumnAggregator]): Row = {
    val sparkValues = ColumnIRSerializer.toSparkValues(irArray, columnAggs)
    new GenericRow(sparkValues)
  }

  /**
    * Serialize a RowAggregator's IR array to a Spark Row.
    *
    * @param irArray The IR array from RowAggregator
    * @param rowAgg The RowAggregator that defines the schema
    * @return Spark Row ready for DataFrame storage or shuffle
    */
  def toSparkRow(irArray: Array[Any], rowAgg: RowAggregator): Row = {
    toSparkRow(irArray, rowAgg.columnAggregators)
  }

  /**
    * Deserialize a Spark Row back to a RowAggregator's IR array.
    *
    * @param row The Spark Row read from DataFrame or shuffle
    * @param columnAggs The ColumnAggregators that define the types
    * @return IR array that can be used by RowAggregator
    */
  def fromSparkRow(row: Row, columnAggs: Array[ColumnAggregator]): Array[Any] = {
    val sparkValues = Array.tabulate(row.length)(row.get)
    ColumnIRSerializer.fromSparkValues(sparkValues, columnAggs)
  }

  /**
    * Deserialize a Spark Row back to a RowAggregator's IR array (convenience method).
    *
    * Extracts the ColumnAggregators from the RowAggregator automatically.
    *
    * @param row The Spark Row read from DataFrame or shuffle
    * @param rowAgg The RowAggregator that defines the schema
    * @return IR array that can be used by RowAggregator
    */
  def fromSparkRow(row: Row, rowAgg: RowAggregator): Array[Any] = {
    fromSparkRow(row, rowAgg.columnAggregators)
  }

  /**
    * Get the Spark schema for a RowAggregator's IR array.
    *
    * @param columnAggs The ColumnAggregators that define the types
    * @return Spark StructType schema
    */
  def schema(columnAggs: Array[ColumnAggregator]): StructType = {
    val fields = columnAggs.zipWithIndex.map {
      case (agg, idx) =>
        StructField(s"col_$idx", SparkConversions.fromChrononType(agg.irType), nullable = true)
    }
    StructType(fields.toSeq)
  }

  /**
    * Get the Spark schema for a RowAggregator's IR array (convenience method).
    *
    * @param rowAgg The RowAggregator that defines the schema
    * @return Spark StructType schema
    */
  def schema(rowAgg: RowAggregator): StructType = {
    schema(rowAgg.columnAggregators)
  }
}
