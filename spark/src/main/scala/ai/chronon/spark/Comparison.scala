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

import org.slf4j.LoggerFactory
import ai.chronon.online.Extensions.StructTypeOps
import com.google.gson.{Gson, GsonBuilder}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.{ArrayType, DecimalType, DoubleType, FloatType, MapType, StructType}
import org.apache.spark.sql.functions.col

import java.util
import scala.collection.mutable

object Comparison {
  @transient lazy val logger = LoggerFactory.getLogger(getClass)

  // Flatten struct columns into individual columns so nested double fields can be compared with tolerance
  private def flattenStructs(df: DataFrame): DataFrame = {
    val flattenedSelects = df.schema.fields.flatMap { field =>
      field.dataType match {
        case structType: StructType =>
          // Flatten struct fields: struct_name.field_name -> struct_name_field_name
          structType.fields.map { subField =>
            col(s"${field.name}.${subField.name}").alias(s"${field.name}_${subField.name}")
          }.toSeq
        case _ =>
          // Keep non-struct fields as-is
          Seq(col(field.name))
      }
    }
    df.select(flattenedSelects: _*)
  }

  // used for comparison
  def sortedJson(m: Map[String, Any]): String = {
    if (m == null) return null
    val tm = new util.TreeMap[String, Any]()
    m.iterator.foreach { case (key, value) => tm.put(key, value) }
    val gson = new GsonBuilder()
      .serializeSpecialFloatingPointValues()
      .create()
    gson.toJson(tm)
  }

  // Convert element to simple representation (Row → Array)
  private def simplifyElement(x: Any): Any = {
    if (x == null) return null
    x match {
      case row: org.apache.spark.sql.Row =>
        // Extract just the values from Row, without Spark schema metadata
        (0 until row.length).map(i => if (row.isNullAt(i)) null else row.get(i)).toArray
      case other => other
    }
  }

  // Convert element to string for sorting
  private def elementToString(x: Any): String = {
    if (x == null) return ""
    val simplified = simplifyElement(x)
    simplified match {
      case arr: Array[_] => arr.mkString("[", ",", "]")
      case other         => other.toString
    }
  }

  // Sort lists/arrays for comparison (order shouldn't matter for sets)
  def sortedList(list: mutable.WrappedArray[Any]): String = {
    if (list == null) return null
    // Sort using clean string representation
    val sorted = list.sorted(Ordering.by[Any, String](elementToString))
    val gson = new GsonBuilder()
      .serializeSpecialFloatingPointValues()
      .create()
    // Simplify Row objects to plain arrays before JSON serialization
    val simplified = sorted.map(simplifyElement)
    gson.toJson(simplified.toArray)
  }

  def stringifyMaps(df: DataFrame): DataFrame = {
    try {
      df.sparkSession.udf.register("sorted_json", (m: Map[String, Any]) => sortedJson(m))
    } catch {
      case e: Exception => e.printStackTrace()
    }
    val selects = for (field <- df.schema.fields) yield {
      if (field.dataType.isInstanceOf[MapType]) {
        s"sorted_json(${field.name}) as `${field.name}`"
      } else {
        s"${field.name} as `${field.name}`"
      }
    }
    df.selectExpr(selects: _*)
  }

  def sortLists(df: DataFrame): DataFrame = {
    try {
      df.sparkSession.udf.register("sorted_list", (list: mutable.WrappedArray[Any]) => sortedList(list))
    } catch {
      case e: Exception => e.printStackTrace()
    }
    val selects = for (field <- df.schema.fields) yield {
      if (field.dataType.isInstanceOf[ArrayType]) {
        s"sorted_list(${field.name}) as `${field.name}`"
      } else {
        s"${field.name} as `${field.name}`"
      }
    }
    df.selectExpr(selects: _*)
  }

  // Produces a "comparison" dataframe - given two dataframes that are supposed to have same data
  // The result contains the differing rows of the same key
  def sideBySide(a: DataFrame,
                 b: DataFrame,
                 keys: List[String],
                 aName: String = "a",
                 bName: String = "b"): DataFrame = {

    logger.info(
      s"""
        |====== side-by-side comparison ======
        |keys: $keys\na_schema:\n${a.schema.pretty}\nb_schema:\n${b.schema.pretty}
        |""".stripMargin
    )

    // Flatten structs so nested double fields can be compared with tolerance
    // Sort lists so order doesn't matter for comparison (e.g., UNIQUE_COUNT arrays)
    val aFlattened = flattenStructs(sortLists(stringifyMaps(a)))
    val bFlattened = flattenStructs(sortLists(stringifyMaps(b)))

    val prefixedExpectedDf = prefixColumnName(aFlattened, s"${aName}_")
    val prefixedOutputDf = prefixColumnName(bFlattened, s"${bName}_")

    val joinExpr = keys
      .map(key => prefixedExpectedDf(s"${aName}_$key") <=> prefixedOutputDf(s"${bName}_$key"))
      .reduce((col1, col2) => col1.and(col2))
    val joined = prefixedExpectedDf.join(
      prefixedOutputDf,
      joinExpr,
      joinType = "full_outer"
    )

    var finalDf = joined
    // Use flattened schema for comparison
    val comparisonColumns =
      aFlattened.schema.fieldNames.toSet.diff(keys.toSet).toList.sorted
    val colOrder =
      keys.map(key => { finalDf(s"${aName}_$key").as(key) }) ++
        comparisonColumns.flatMap { col =>
          List(finalDf(s"${aName}_$col"), finalDf(s"${bName}_$col"))
        }
    // double columns need to be compared approximately (now includes flattened struct fields)
    val doubleCols = aFlattened.schema.fields
      .filter(field =>
        field.dataType == DoubleType || field.dataType == FloatType || field.dataType.isInstanceOf[DecimalType])
      .map(_.name)
      .toSet
    finalDf = finalDf.select(colOrder: _*)
    val comparisonFilters = comparisonColumns
      .flatMap { col =>
        val left = s"${aName}_$col"
        val right = s"${bName}_$col"
        val compareExpression =
          if (doubleCols.contains(col)) {
            s"($left is NOT NULL) AND ($right is NOT NULL) and (abs($left - $right) > 0.00001)"
          } else { s"($left <> $right)" }
        Seq(s"(($left IS NULL AND $right IS NOT NULL) OR ($right IS NULL AND $left IS NOT NULL) OR $compareExpression)")
      }
    logger.info(s"Using comparison filter:\n  ${comparisonFilters.mkString("\n  ")}")
    if (comparisonFilters.nonEmpty) {
      finalDf.filter(comparisonFilters.mkString(" or "))
    } else {
      // all rows are good
      finalDf.filter("false")
    }
  }

  private def prefixColumnName(df: DataFrame, prefix: String): DataFrame = {
    val renamedColumns = df.columns.map(c => { df(c).as(s"$prefix$c") })
    df.select(renamedColumns: _*)
  }
}
