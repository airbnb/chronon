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

package ai.chronon.spark.stats

import ai.chronon.aggregator.row.RowAggregator
import ai.chronon.api.Extensions.{AggregationPartOps, WindowUtils}
import ai.chronon.api._
import ai.chronon.online.{DataMetrics, SparkConversions}
import ai.chronon.spark.{Comparison, TableUtils, TimedKvRdd}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.{Column, DataFrame, functions, types, Row => SparkRow}

import scala.collection.immutable.SortedMap
import scala.util.ScalaJavaConversions.JMapOps

object CompareMetrics {
  val leftSuffix = "_left"
  val rightSuffix = "_right"
  val comparisonViewNameSuffix = "_comparison"
  val bins = 41
  val percentilesArgMap: Map[String, String] =
    Map("k" -> "128", "percentiles" -> s"[${(0 to bins).map(i => i * 1.0 / bins).mkString(",")}]")

  case class MetricTransform(name: String,
                             expr: Column,
                             operation: Operation,
                             argMap: Map[String, String] = null,
                             additionalExprs: Seq[(String, String)] = null)

  private def edit_distance: UserDefinedFunction =
    functions.udf((left: Object, right: Object) => EditDistance.between(left, right))

  def buildMetrics(valueFields: Array[StructField], mapping: Map[String, String] = Map.empty): Seq[MetricTransform] =
    valueFields.flatMap { field =>
      val left = functions.col(field.name + leftSuffix)
      val right = if (mapping.contains(field.name)) {
        functions.col(mapping.get(field.name).get + rightSuffix)
      } else {
        functions.col(field.name + rightSuffix)
      }
      val universalMetrics = Seq(
        MetricTransform("both_null", left.isNull.and(right.isNull), Operation.SUM),
        MetricTransform("left_null", left.isNull.and(right.isNotNull), Operation.SUM),
        MetricTransform("right_null", left.isNotNull.and(right.isNull), Operation.SUM)
      )
      val smape_denom = functions.abs(left) + functions.abs(right)
      val numericMetrics = Seq(
        MetricTransform(
          "smape",
          functions
            .when(
              smape_denom.notEqual(0.0),
              (functions.abs(left - right) * 2).cast(types.DoubleType) / smape_denom
            )
            .otherwise(0.0),
          Operation.AVERAGE
        ),
        MetricTransform("left_minus_right", left - right, Operation.APPROX_PERCENTILE, argMap = percentilesArgMap),
        MetricTransform("left", left, Operation.APPROX_PERCENTILE, argMap = percentilesArgMap),
        MetricTransform("right", right, Operation.APPROX_PERCENTILE, argMap = percentilesArgMap)
      )

      val sequenceMetrics = Seq(
        MetricTransform(
          "edit_distance",
          edit_distance(left, right),
          Operation.APPROX_PERCENTILE,
          percentilesArgMap,
          additionalExprs = Seq(
            "insert" -> ".insert",
            "delete" -> ".delete"
          )
        ),
        MetricTransform("left_length", functions.size(left), Operation.APPROX_PERCENTILE, percentilesArgMap),
        MetricTransform("right_length", functions.size(right), Operation.APPROX_PERCENTILE, percentilesArgMap),
        MetricTransform("mismatch_length",
                        left.isNotNull.and(right.isNotNull).and(functions.size(left).notEqual(functions.size(right))),
                        Operation.SUM)
      )

      val equalityMetric =
        if (!DataType.isMap(field.fieldType))
          Some(
            MetricTransform("mismatch", left.isNotNull.and(right.isNotNull).and(left.notEqual(right)), Operation.SUM))
        else None

      val typeSpecificMetrics = if (DataType.isNumeric(field.fieldType)) {
        numericMetrics
      } else if (DataType.isList(field.fieldType)) {
        sequenceMetrics
      } else {
        Seq.empty[MetricTransform]
      }

      val allMetrics =
        (universalMetrics ++ typeSpecificMetrics ++ equalityMetric :+ MetricTransform("total",
                                                                                      functions.lit(true),
                                                                                      Operation.COUNT))
          .map { m =>
            val fullName = field.name + "_" + m.name
            m.copy(
              name = fullName,
              expr = m.expr.as(fullName),
              additionalExprs = Option(m.additionalExprs)
                .map(_.map { case (name, expr) => (fullName + "_" + name, fullName + expr) })
                .orNull
            )
          }
      allMetrics
    }

  def buildRowAggregator(metrics: Seq[MetricTransform], inputDf: DataFrame): RowAggregator = {
    val schema = SparkConversions.toChrononSchema(inputDf.schema)
    val aggParts = metrics.flatMap { m =>
      def buildAggPart(name: String): AggregationPart = {
        val aggPart = new AggregationPart()
        aggPart.setInputColumn(name)
        aggPart.setOperation(m.operation)
        if (m.argMap != null)
          aggPart.setArgMap(m.argMap.toJava)
        aggPart.setWindow(WindowUtils.Unbounded)
        aggPart
      }
      if (m.additionalExprs == null) {
        Seq(buildAggPart(m.name))
      } else {
        m.additionalExprs.map { case (name, _) => buildAggPart(name) }
      }
    }
    new RowAggregator(schema, aggParts)
  }

  def compute(valueFields: Array[StructField],
              inputDf: DataFrame,
              keys: Seq[String],
              name: String,
              mapping: Map[String, String] = Map.empty,
              timeBucketMinutes: Long = 60): (TimedKvRdd, DataMetrics) = {
    val tableUtils = TableUtils(inputDf.sparkSession)
    // spark maps cannot be directly compared, for now we compare the string representation
    // TODO 1: For Maps, we should find missing keys, extra keys and mismatched keys
    // TODO 2: Values should have type specific comparison
    val valueSchema = valueFields.map(field =>
      field.fieldType match {
        case MapType(_, _) => StructField(field.name, StringType)
        case _             => field
      })
    val metrics = buildMetrics(valueSchema, mapping)
    val timeColumn: String = if (keys.contains(Constants.TimeColumn)) {
      Constants.TimeColumn
    } else if (keys.contains(tableUtils.partitionColumn)) {
      tableUtils.partitionColumn
    } else {
      throw new IllegalArgumentException("Keys doesn't contain the time column")
    }

    val selectedDf = Comparison
      .stringifyMaps(inputDf)
      .select(metrics.map(_.expr) :+ functions.col(timeColumn): _*)
    val secondPassSelects = metrics.flatMap { metric =>
      if (metric.additionalExprs != null) {
        metric.additionalExprs.map { case (name, expr) => (s"$expr as $name") }
      } else {
        Seq(metric.name)
      }
    }
    // TODO: We are currently bucketing based on the time but we should extend it to support other bucketing strategy.
    val secondPassDf = selectedDf.selectExpr(secondPassSelects :+ timeColumn: _*)
    val rowAggregator = buildRowAggregator(metrics, secondPassDf)
    val bucketMs = 1000 * 60 * timeBucketMinutes
    val timeIndex = secondPassDf.schema.fieldIndex(timeColumn)
    val outputColumns = rowAggregator.aggregationParts.map(_.outputColumnName).toArray
    def sortedMap(vals: Seq[(String, Any)]) = SortedMap.empty[String, Any] ++ vals
    val resultRdd = secondPassDf.rdd
      .keyBy(row => {
        val timeValue = if (timeColumn == tableUtils.partitionColumn) {
          tableUtils.partitionSpec.epochMillis(row.getString(timeIndex))
        } else {
          row.getLong(timeIndex)
        }
        (timeValue / bucketMs) * bucketMs
      }) // bin
      .mapValues(SparkConversions.toChrononRow(_, -1))
      .aggregateByKey(rowAggregator.init)(rowAggregator.updateWithReturn, rowAggregator.merge) // aggregate
      .mapValues(rowAggregator.finalize)
    val uploadRdd = resultRdd.map { case (k, v) => (Array(name.asInstanceOf[Any]), v, k) }
    implicit val sparkSession = inputDf.sparkSession
    val timedKvRdd = TimedKvRdd(
      uploadRdd,
      SparkConversions.fromChrononSchema(Constants.StatsKeySchema),
      SparkConversions.fromChrononSchema(rowAggregator.outputSchema),
      storeSchemasPrefix = Some(name)
    )
    val result = resultRdd
      .collect()
      .sortBy(_._1)
      .map { case (bucketStart, vals) => bucketStart -> sortedMap(outputColumns.zip(vals)) }
    (timedKvRdd -> DataMetrics(result))
  }
}
