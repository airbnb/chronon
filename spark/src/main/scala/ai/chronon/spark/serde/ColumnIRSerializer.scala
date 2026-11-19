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
import ai.chronon.api
import ai.chronon.api.{DataType, StructField}
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.GenericRow

import java.util
import java.util.concurrent.ConcurrentHashMap
import scala.collection.Seq

/**
  * Column-level serializer for IR (Intermediate Representation) values to/from Spark types.
  *
  * This handles per-column conversion between:
  * - Chronon's rich Java object world (MinHeaps, TreeMaps, Timestamps, Sketches, etc.)
  * - Spark's type system (primitives, arrays, maps, structs)
  *
  * Architecture (following AvroConversions and SparkConversions pattern):
  * 1. ColumnAggregator.normalize() converts IR objects to Java collections
  * 2. Row.to() handles recursive traversal with operation-aware callbacks
  * 3. ColumnAggregator.denormalize() converts back to IR objects
  *
  * This is a pure column-level converter - layout/sharing strategies (e.g., deduplicating
  * shared IR columns across windows) are handled at a higher level.
  */
object ColumnIRSerializer {

  // Cache for toSparkValue converters keyed by DataType.
  // All callbacks are static vals so we cache unconditionally.
  // DataType identity works because ColumnAggregator.irType returns stable references
  // (same pattern as SparkConversions.converterCache).
  private val toSparkConverterCache = new ConcurrentHashMap[api.DataType, Any => Any]()

  /** Build a reusable converter from normalized IR values to Spark types. */
  private def buildToSparkConverter(dataType: api.DataType): Any => Any = {
    api.Row.buildToConverter[Row, Array[Byte], Array[Any], Map[Any, Any], org.apache.spark.sql.types.StructType](
      dataType,
      composeStruct,
      identity[Array[Byte]],
      collectList,
      convertMap,
      extraneousRowHandler,
      None
    )
  }

  /**
    * Convert a single IR value to a Spark-compatible value.
    *
    * @param irValue The IR value from a single ColumnAggregator
    * @param columnAgg The ColumnAggregator that defines the type
    * @return Spark-compatible value ready for DataFrame storage
    */
  def toSparkValue(irValue: Any, columnAgg: ColumnAggregator): Any = {
    if (irValue == null) return null

    val normalizedValue = columnAgg.normalize(irValue)
    val converter = toSparkConverterCache.computeIfAbsent(
      columnAgg.irType,
      (dt: api.DataType) => buildToSparkConverter(dt)
    )
    converter(normalizedValue)
  }

  /**
    * Convert a Spark value back to an IR value.
    *
    * @param sparkValue The value read from a Spark DataFrame column
    * @param columnAgg The ColumnAggregator that defines the type
    * @return IR value that can be used by ColumnAggregator
    */
  def fromSparkValue(sparkValue: Any, columnAgg: ColumnAggregator): Any = {
    if (sparkValue == null) return null

    val convertedValue = convertScalaMapToJava(sparkValue)

    val normalizedValue = api.Row.from[Row, Array[Byte], Any, String](
      convertedValue,
      columnAgg.irType,
      decomposeStruct,
      identity[Array[Byte]],
      arrayToArrayList,
      identity[String]
    )

    columnAgg.denormalize(normalizedValue)
  }

  /**
    * Convert an array of IR values to Spark-compatible values.
    *
    * @param irArray The IR array from RowAggregator
    * @param columnAggs The column aggregators that define the types
    * @return Array of Spark-compatible values
    */
  def toSparkValues(irArray: Array[Any], columnAggs: Array[ColumnAggregator]): Array[Any] = {
    if (irArray == null) return null
    require(irArray.length == columnAggs.length,
            s"IR array length ${irArray.length} must match aggregator count ${columnAggs.length}")

    Array.tabulate(irArray.length) { i => toSparkValue(irArray(i), columnAggs(i)) }
  }

  /**
    * Convert an array of Spark values back to IR values.
    *
    * @param sparkValues The values read from a Spark DataFrame row
    * @param columnAggs The column aggregators that define the types
    * @return IR array that can be used by RowAggregator
    */
  def fromSparkValues(sparkValues: Array[Any], columnAggs: Array[ColumnAggregator]): Array[Any] = {
    if (sparkValues == null) return null
    require(sparkValues.length == columnAggs.length,
            s"Spark values length ${sparkValues.length} must match aggregator count ${columnAggs.length}")

    Array.tabulate(sparkValues.length) { i => fromSparkValue(sparkValues(i), columnAggs(i)) }
  }

  // ============================================================================
  // Operation-aware callback factories for Row.to()
  // ============================================================================

  /** Compose struct values into Spark Row. */
  private val composeStruct: (Array[Any], DataType, Option[org.apache.spark.sql.types.StructType]) => Row = {
    (arr, _, _) => new GenericRow(arr)
  }

  /** Collect list values into Spark Array. */
  private val collectList: (Array[Any], Int) => Array[Any] = { (arr, _) =>
    arr
  }

  /** Convert Java Map to Scala Map for Spark storage. */
  private val convertMap: util.Map[Any, Any] => Map[Any, Any] = { (javaMap: util.Map[Any, Any]) =>
    {
      import scala.jdk.CollectionConverters._
      javaMap.asScala.toMap
    }
  }

  /**
    * Convert Scala Maps to Java Maps recursively (Row.from expects java.util.Map).
    *
    * Recurses into Spark Rows and Seqs to find nested Scala Maps. Array[Byte] (binary types)
    * is safe because it does not extend Seq and falls through to the default case unchanged.
    */
  private def convertScalaMapToJava(value: Any): Any = {
    value match {
      case scalaMap: Map[_, _] =>
        val javaMap = new util.HashMap[Any, Any](scalaMap.size)
        scalaMap.foreach {
          case (k, v) =>
            javaMap.put(convertScalaMapToJava(k), convertScalaMapToJava(v))
        }
        javaMap
      case row: Row =>
        new GenericRow(Array.tabulate(row.size)(i => convertScalaMapToJava(row.get(i))))
      case seq: Seq[_] =>
        seq.map(convertScalaMapToJava)
      case other =>
        other
    }
  }

  // ============================================================================
  // Operation-aware callback factories for Row.from()
  // ============================================================================

  /** Decompose Spark Row into values. */
  private val decomposeStruct: (Row, Seq[StructField]) => Iterator[Any] = { (row: Row, _: Seq[StructField]) =>
    Iterator.tabulate(row.length)(row.get)
  }

  /** Convert Spark Array to ArrayList. */
  private val arrayToArrayList: Any => util.ArrayList[Any] = { (arr: Any) =>
    {
      // Convert to Seq to handle both Array and WrappedArray
      val seq = arr match {
        case a: Array[_] => a.toSeq
        case s: Seq[_]   => s
        case other =>
          throw new IllegalArgumentException(s"Expected Array or Seq but got ${other.getClass}")
      }
      val result = new util.ArrayList[Any](seq.size)
      seq.foreach(result.add)
      result
    }
  }

  /**
    * Handle Spark Row objects in normalized IR by converting to Array[Any].
    */
  private val extraneousRowHandler: Any => Array[Any] = {
    case row: Row =>
      (0 until row.length).map(row.get).toArray
    case other =>
      throw new IllegalArgumentException(s"Unexpected type in extraneous record handler: ${other.getClass}")
  }
}
