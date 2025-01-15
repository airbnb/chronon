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

import ai.chronon.api
import ai.chronon.api.Extensions._
import com.yahoo.memory.Memory
import com.yahoo.sketches.kll.KllFloatsSketch

import java.util
import scala.collection.Seq
import scala.util.ScalaJavaConversions.JMapOps

/**
  * Module managing FeatureStats Schema, Aggregations to be used by type and aggregator construction.
  *
  * Stats Aggregation has an offline/ batch component and an online component.
  * The metrics defined for stats depend on the schema of the join. The dataTypes and column names.
  * For the online side, we obtain this information from the JoinCodec/valueSchema
  * For the offline side, we obtain this information directly from the outputTable.
  * To keep the schemas consistent we sort the metrics in the schema by name. (one column can have multiple metrics).
  */
object StatsGenerator {

  val nullSuffix = "__null"
  val nullRateSuffix = "__null_rate"
  val totalColumn = "total"
  // Leveraged to build a CDF. Consider using native KLLSketch CDF/PMF methods.
  val finalizedPercentilesMerged: Array[Double] = Array(0.01) ++ (5 until 100 by 5).map(_.toDouble / 100) ++ Array(0.99)
  // Leveraged to build candlestick time series.
  val finalizedPercentilesSeries: Array[Double] = Array(0.05, 0.25, 0.5, 0.75, 0.95)
  val ignoreColumns = Seq(api.Constants.TimeColumn, "ds", "date_key", "date", "datestamp")

  /**
    * InputTransform acts as a signal of how to process the metric.
    *
    * IsNull: Check if the input is null.
    *
    * Raw: Operate in the input column.
    *
    * One: lit(true) in spark. Used for row counts leveraged to obtain null rate values.
    * */
  object InputTransform extends Enumeration {
    type InputTransform = Value
    val IsNull, Raw, One = Value
  }
  import InputTransform._

  /**
    * MetricTransform represents a single statistic built on top of an input column.
    */
  case class MetricTransform(name: String,
                             expression: InputTransform,
                             operation: api.Operation,
                             suffix: String = "",
                             argMap: util.Map[String, String] = null)

  /**
    * Post processing for finalized values or IRs when generating a time series of stats.
    * In the case of percentiles for examples we reduce to 5 values in order to generate candlesticks.
    */
  def SeriesFinalizer(key: String, value: AnyRef): AnyRef = {
    (key, value) match {
      case (k, sketch: Array[Byte]) if k.endsWith("percentile") =>
        val sketch = KllFloatsSketch.heapify(Memory.wrap(value.asInstanceOf[Array[Byte]]))
        sketch.getQuantiles(finalizedPercentilesSeries).asInstanceOf[AnyRef]
      case _ => value
    }
  }

  def buildAggPart(m: MetricTransform): api.AggregationPart = {
    val aggPart = new api.AggregationPart()
    aggPart.setInputColumn(s"${m.name}${m.suffix}")
    aggPart.setOperation(m.operation)
    if (m.argMap != null)
      aggPart.setArgMap(m.argMap)
    aggPart.setWindow(WindowUtils.Unbounded)
    aggPart
  }

  /** Build RowAggregator to use for computing stats on a dataframe based on metrics */
  def buildAggregator(metrics: Seq[MetricTransform], selectedSchema: api.StructType): RowAggregator = {
    val aggParts = metrics.flatMap { m => Seq(buildAggPart(m)) }
    new RowAggregator(selectedSchema.unpack, aggParts)
  }

  /** Stats applied to any column */
  def anyTransforms(column: String): Seq[MetricTransform] =
    Seq(MetricTransform(column, InputTransform.IsNull, operation = api.Operation.SUM, suffix = nullSuffix))

  /** Stats applied to numeric columns */
  def numericTransforms(column: String): Seq[MetricTransform] =
    anyTransforms(column) ++ Seq(
      MetricTransform(
        column,
        InputTransform.Raw,
        operation = api.Operation.APPROX_PERCENTILE,
        argMap = Map("percentiles" -> s"[${finalizedPercentilesMerged.mkString(", ")}]").toJava
      ))

  /** For the schema of the data define metrics to be aggregated */
  def buildMetrics(fields: Seq[(String, api.DataType)]): Seq[MetricTransform] = {
    val metrics = fields
      .flatMap {
        case (name, dataType) =>
          if (ignoreColumns.contains(name)) {
            Seq.empty
          } else if (api.DataType.isNumeric(dataType) && dataType != api.ByteType) {
            // ByteTypes are not supported due to Avro Encodings and limited support on aggregators.
            // Needs to be casted on source if required.
            numericTransforms(name)
          } else {
            anyTransforms(name)
          }
      }
      .sortBy(_.name)
    metrics :+ MetricTransform(totalColumn, InputTransform.One, api.Operation.COUNT)
  }

  def lInfKllSketch(sketch1: AnyRef, sketch2: AnyRef, bins: Int = 128): AnyRef = {
    if (sketch1 == null || sketch2 == null) return None
    val sketchIr1 = KllFloatsSketch.heapify(Memory.wrap(sketch1.asInstanceOf[Array[Byte]]))
    val sketchIr2 = KllFloatsSketch.heapify(Memory.wrap(sketch2.asInstanceOf[Array[Byte]]))
    val keySet = sketchIr1.getQuantiles(bins).union(sketchIr2.getQuantiles(bins))
    var linfSimple = 0.0
    keySet.foreach { key =>
      val cdf1 = sketchIr1.getRank(key)
      val cdf2 = sketchIr2.getRank(key)
      val cdfDiff = Math.abs(cdf1 - cdf2)
      linfSimple = Math.max(linfSimple, cdfDiff)
    }
    linfSimple.asInstanceOf[AnyRef]
  }

  /**
    * PSI is a measure of the difference between two probability distributions.
    * However, it's not defined for cases where a bin can have zero elements in either distribution
    * (meant for continuous measures). In order to support PSI for discrete measures we add a small eps value to
    * perturb the distribution in bins.
    *
    * Existing rules of thumb are: PSI < 0.10 means "little shift", .10<PSI<.25 means "moderate shift",
    * and PSI>0.25 means "significant shift, action required"
    * https://scholarworks.wmich.edu/dissertations/3208
    */
  def PSIKllSketch(reference: AnyRef, comparison: AnyRef, bins: Int = 128, eps: Double = 0.000001): AnyRef = {
    if (reference == null || comparison == null) return None
    val referenceSketch = KllFloatsSketch.heapify(Memory.wrap(reference.asInstanceOf[Array[Byte]]))
    val comparisonSketch = KllFloatsSketch.heapify(Memory.wrap(comparison.asInstanceOf[Array[Byte]]))
    val keySet = referenceSketch.getQuantiles(bins).union(comparisonSketch.getQuantiles(bins)).toSet.toArray.sorted
    val referencePMF = regularize(referenceSketch.getPMF(keySet), eps)
    val comparisonPMF = regularize(comparisonSketch.getPMF(keySet), eps)
    var psi = 0.0
    for (i <- 0 until referencePMF.length) {
      psi += (referencePMF(i) - comparisonPMF(i)) * Math.log(referencePMF(i) / comparisonPMF(i))
    }
    psi.asInstanceOf[AnyRef]
  }

  /** Given a PMF add and substract small values to keep a valid probability distribution without zeros */
  def regularize(doubles: Array[Double], eps: Double): Array[Double] = {
    val countZeroes = doubles.count(_ == 0.0)
    if (countZeroes == 0) {
      doubles // If there are no zeros, return the original array
    } else {
      val nonZeroCount = doubles.length - countZeroes
      val replacement = eps * nonZeroCount / countZeroes
      doubles.map {
        case 0.0 => replacement
        case x   => x - eps
      }
    }
  }
}
