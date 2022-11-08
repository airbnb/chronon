package ai.chronon.spark.stats

import ai.chronon.aggregator.base.ApproxPercentiles
import ai.chronon.aggregator.row.RowAggregator
import ai.chronon.spark.Extensions._
import ai.chronon.api.Extensions._
import ai.chronon.api
import ai.chronon.spark.{Conversions, KvRdd, RowWrapper}
import com.yahoo.memory.Memory
import com.yahoo.sketches.kll.KllFloatsSketch
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.{Column, DataFrame, functions, Row => SparkRow}

import java.util
import collection.JavaConversions._
import scala.util.Try

/**
  * StatsGenerator takes care of computation of metadata for dataframes as well as
  * measuring differences between two dataframes.
  * This applies to drifts as well between two dataframes.
  */
object StatsGenerator {

  // TODO: unify with OOC since has the same metric transform.
  case class MetricTransform(name: String, expression: Column, operation: api.Operation, argMap: util.Map[String, String] = null)

  val nullPrefix = "null__"
  val nullRatePrefix = "null_rate__"
  val totalColumn = "total"
  val ignoreColumns = Seq(api.Constants.TimeColumn, api.Constants.PartitionColumn)
  val finalizedPercentiles = Seq(0.01, 0.05, 0.1, 0.25, 0.5, 0.75, 0.9, 0.95, 0.99)

  /** Stats applied to any column */
  def anyTransforms(column: Column): Seq[MetricTransform] = Seq(
    MetricTransform(s"$nullPrefix$column", column.isNull, operation = api.Operation.SUM)
    //, MetricTransform(s"$column", column, operation = api.Operation.APPROX_UNIQUE_COUNT)
  )

  /** Stats applied to numeric columns */
  def numericTransforms(column: Column): Seq[MetricTransform] = anyTransforms(column) ++ Seq(
    MetricTransform(column.toString(), column, operation = api.Operation.APPROX_PERCENTILE, argMap = Map("percentiles" -> s"[${finalizedPercentiles.mkString(", ")}]")))

  /** For the schema of the data define metrics to be aggregated */
  def buildMetrics(fields: Array[(String, api.DataType)]): Seq[MetricTransform] = {
    val metrics = fields.flatMap {
      case (name, dataType) =>
        if (ignoreColumns.contains(name)) {
          Seq.empty
        } else if (api.DataType.isNumeric(dataType)) {
          numericTransforms(functions.col(name))
        } else {
          anyTransforms(functions.col(name))
        }
    }
    metrics :+ MetricTransform(totalColumn, lit(true), api.Operation.COUNT)
  }

  /** Build RowAggregator to use for computing stats on a dataframe based on metrics */
  def buildAggregator(metrics: Seq[MetricTransform], inputDf: DataFrame): RowAggregator = {
    val schema = Conversions.toChrononSchema(inputDf.schema)
    val aggParts = metrics.flatMap { m =>
      def buildAggPart(name: String): api.AggregationPart = {
        val aggPart = new api.AggregationPart()
        aggPart.setInputColumn(name)
        aggPart.setOperation(m.operation)
        if (m.argMap != null)
          aggPart.setArgMap(m.argMap)
        aggPart.setWindow(WindowUtils.Unbounded)
        aggPart
      }

      Seq(buildAggPart(m.name))
    }
    new RowAggregator(schema, aggParts)
  }

}

class StatsCompute(inputDf: DataFrame, keys: Seq[String]) extends Serializable {

  private val noKeysDf: DataFrame = inputDf.select(inputDf.columns.filter(colName => !keys.contains(colName))
    .map(colName => new Column(colName)): _*)

  val keyColumns = if (inputDf.columns.contains(api.Constants.TimeColumn)) Seq(api.Constants.TimeColumn, api.Constants.PartitionColumn) else Seq(api.Constants.PartitionColumn)
  val metrics = StatsGenerator.buildMetrics(Conversions.toChrononSchema(noKeysDf.schema))
  val selectedDf: DataFrame = noKeysDf.select(keyColumns.map(col) ++ metrics.map(m => m.expression): _*).toDF(keyColumns ++ metrics.map(m => m.name): _*)

  /** Given a summary Dataframe that computed the stats. Add derived data (example: null rate, median, etc) */
  def addDerivedMetrics(df: DataFrame, aggregator: RowAggregator): DataFrame = {
    val nullColumns = df.columns.filter(p => p.startsWith(StatsGenerator.nullPrefix))
    val withNullRatesDF = nullColumns.foldLeft(df) {
      (tmpDf, column) =>
        tmpDf.withColumn(s"${StatsGenerator.nullRatePrefix}${column.stripPrefix(StatsGenerator.nullPrefix)}",
          tmpDf.col(column) / tmpDf.col(Seq(StatsGenerator.totalColumn, api.Operation.COUNT).mkString("_")))
    }
    
    val percentiles = aggregator
      .aggregationParts.filter(_.operation == api.Operation.APPROX_PERCENTILE)
    val percentileColumns = percentiles.map(_.outputColumnName)
    import org.apache.spark.sql.functions.udf
    val percentileFinalizerUdf = udf(
      (s: Array[Byte]) =>
        Try(KllFloatsSketch.heapify(Memory.wrap(s))
          .getQuantiles(StatsGenerator.finalizedPercentiles.toArray)
          .zip(StatsGenerator.finalizedPercentiles).map(f => f._2.toString -> f._1.toString).toMap).toOption
    )
    percentileColumns.foldLeft(withNullRatesDF) {
      (tmpDf, column) =>
        tmpDf.withColumn(s"${column}_finalized", percentileFinalizerUdf(col(column)))
    }
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
  def dailySummary(aggregator: RowAggregator, sample: Double = 1.0, timeBucketMinutes: Long = 60): KvRdd = {
    val partitionIdx = selectedDf.schema.fieldIndex(api.Constants.PartitionColumn)
    val bucketMs = timeBucketMinutes * 1000 * 60
    val tsIdx = if(selectedDf.columns.contains(api.Constants.TimeColumn)) selectedDf.schema.fieldIndex(api.Constants.TimeColumn) else -1
    val hourlyCompute = tsIdx >= 0 && timeBucketMinutes > 0
    val keyField = if (hourlyCompute) StructField(api.Constants.TimeColumn, LongType) else StructField(api.Constants.PartitionColumn, StringType)
    val result = selectedDf
      .sample(sample)
      .rdd
      .map(Conversions.toChrononRow(_, tsIdx))
      .keyBy(row => if(hourlyCompute) ((row.ts / bucketMs) * bucketMs).asInstanceOf[Any] else row.get(partitionIdx))
      .aggregateByKey(aggregator.init)(seqOp = aggregator.updateWithReturn, combOp = aggregator.merge)
      .mapValues(aggregator.normalize(_))
      .map{ case (k, v) => (Array(k), v) }  // To use KvRdd

    implicit val sparkSession = inputDf.sparkSession
    KvRdd(
      result,
      StructType(Array(keyField)),
      Conversions.fromChrononSchema(aggregator.irSchema)
    )
  }
}