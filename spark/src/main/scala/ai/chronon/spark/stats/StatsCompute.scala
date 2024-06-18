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

import ai.chronon.aggregator.row.{RowAggregator, StatsGenerator}
import ai.chronon.api
import ai.chronon.api.Extensions._
import ai.chronon.spark.Extensions._
import ai.chronon.online.SparkConversions
import ai.chronon.spark.TableUtils
import com.yahoo.memory.Memory
import com.yahoo.sketches.kll.KllFloatsSketch
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{Column, DataFrame, functions}

import scala.util.Try
import ai.chronon.spark.TimedKvRdd

class StatsCompute(inputDf: DataFrame, keys: Seq[String], name: String) extends Serializable {

  private val noKeysDf: DataFrame = inputDf.select(
    inputDf.columns
      .filter(colName => !keys.contains(colName))
      .map(colName => new Column(colName)): _*)
  implicit val tableUtils = TableUtils(inputDf.sparkSession)

  val timeColumns =
    if (inputDf.columns.contains(api.Constants.TimeColumn)) Seq(api.Constants.TimeColumn, tableUtils.partitionColumn)
    else Seq(tableUtils.partitionColumn)
  val metrics = StatsGenerator.buildMetrics(SparkConversions.toChrononSchema(noKeysDf.schema))
  lazy val selectedDf: DataFrame = noKeysDf
    .select(timeColumns.map(col) ++ metrics.map(m =>
      m.expression match {
        case StatsGenerator.InputTransform.IsNull => functions.col(m.name).isNull
        case StatsGenerator.InputTransform.Raw    => functions.col(m.name)
        case StatsGenerator.InputTransform.One    => functions.lit(true)
      }): _*)
    .toDF(timeColumns ++ metrics.map(m => s"${m.name}${m.suffix}"): _*)

  /** Given a summary Dataframe that computed the stats. Add derived data (example: null rate, median, etc) */
  def addDerivedMetrics(df: DataFrame, aggregator: RowAggregator): DataFrame = {
    val nullColumns = df.columns.filter(p => p.startsWith(StatsGenerator.nullSuffix))
    val withNullRatesDF = nullColumns.foldLeft(df) { (tmpDf, column) =>
      tmpDf.withColumn(
        s"${StatsGenerator.nullRateSuffix}${column.stripPrefix(StatsGenerator.nullSuffix)}",
        tmpDf.col(column) / tmpDf.col(Seq(StatsGenerator.totalColumn, api.Operation.COUNT).mkString("_"))
      )
    }

    val percentiles = aggregator.aggregationParts.filter(_.operation == api.Operation.APPROX_PERCENTILE)
    val percentileColumns = percentiles.map(_.outputColumnName)
    import org.apache.spark.sql.functions.udf
    val percentileFinalizerUdf = udf((s: Array[Byte]) =>
      Try(
        KllFloatsSketch
          .heapify(Memory.wrap(s))
          .getQuantiles(StatsGenerator.finalizedPercentilesMerged)
          .zip(StatsGenerator.finalizedPercentilesMerged)
          .map(f => f._2.toString -> f._1.toString)
          .toMap).toOption)
    val addedPercentilesDf = percentileColumns.foldLeft(withNullRatesDF) { (tmpDf, column) =>
      tmpDf.withColumn(s"${column}_finalized", percentileFinalizerUdf(col(column)))
    }
    addedPercentilesDf.withTimeBasedColumn(tableUtils.partitionColumn)
  }

  /** Navigate the dataframe and compute statistics partitioned by date stamp
    *
    * Partitioned by day version of the normalized summary. Useful for scheduling a job that computes daily stats.
    * Returns a KvRdd to be able to be pushed into a KvStore for fetching and merging. As well as a dataframe for
    * storing in hive.
    *
    * For entity on the left we use daily partition as the key. For events we bucket by timeBucketMinutes (def. 1 hr)
    * Since the stats are mergeable coarser granularities can be obtained through fetcher merging.
    */
  def dailySummary(aggregator: RowAggregator, sample: Double = 1.0, timeBucketMinutes: Long = 60): TimedKvRdd = {
    val partitionIdx = selectedDf.schema.fieldIndex(tableUtils.partitionColumn)
    val partitionSpec = tableUtils.partitionSpec
    val bucketMs = timeBucketMinutes * 1000 * 60
    val tsIdx =
      if (selectedDf.columns.contains(api.Constants.TimeColumn)) selectedDf.schema.fieldIndex(api.Constants.TimeColumn)
      else -1
    val isTimeBucketed = tsIdx >= 0 && timeBucketMinutes > 0
    val keyName: Any = name
    val result = selectedDf
      .sample(sample)
      .rdd
      .map(SparkConversions.toChrononRow(_, tsIdx))
      .keyBy(row =>
        if (isTimeBucketed) (row.ts / bucketMs) * bucketMs
        else partitionSpec.epochMillis(row.getAs[String](partitionIdx)))
      .aggregateByKey(aggregator.init)(seqOp = aggregator.updateWithReturn, combOp = aggregator.merge)
      .mapValues(aggregator.normalize(_))
      .map { case (k, v) => (Array(keyName), v, k) } // To use KvRdd
    implicit val sparkSession = inputDf.sparkSession
    TimedKvRdd(
      result,
      SparkConversions.fromChrononSchema(api.Constants.StatsKeySchema),
      SparkConversions.fromChrononSchema(aggregator.irSchema),
      storeSchemasPrefix = Some(name)
    )
  }
}
