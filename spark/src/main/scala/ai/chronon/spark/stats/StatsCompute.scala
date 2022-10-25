package ai.chronon.spark.stats

import ai.chronon.aggregator.base.ApproxPercentiles
import ai.chronon.aggregator.row.RowAggregator
import ai.chronon.spark.Extensions._
import ai.chronon.api.Extensions._
import ai.chronon.api
import ai.chronon.spark.{Conversions, KvRdd}
import com.yahoo.sketches.kll.KllFloatsSketch
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.{Column, DataFrame, functions, Row => SparkRow}

import java.util
import collection.JavaConversions._

/**
  * StatsGenerator takes care of computation of metadata for dataframes as well as
  * measuring differences between two dataframes.
  * This applies to drifts as well between two dataframes.
  */
object StatsGenerator {

  // TODO: unify with OOC since has the same metric transform.
  case class MetricTransform(name: String, expression: Column, operation: api.Operation, argMap: util.Map[String, String] = null)

  private val nullPrefix = "null__"
  private val nullRatePrefix = "null_rate__"
  private val totalColumn = "total"

  /** Stats applied to any column */
  def anyTransforms(column: Column): Seq[MetricTransform] = Seq(
      MetricTransform(s"$nullPrefix$column", column.isNull, operation = api.Operation.SUM)
    //, MetricTransform(s"$column", column, operation = api.Operation.APPROX_UNIQUE_COUNT)
  )

  /** Stats applied to numeric columns */
  def numericTransforms(column: Column): Seq[MetricTransform] = anyTransforms(column) ++ Seq(
    MetricTransform(column.toString(), column, operation = api.Operation.APPROX_PERCENTILE, argMap = Map("percentiles" -> "[0.5, 0.95, 0.99]")))

  /** Given a summary Dataframe that computed the stats. Add derived data (example: null rate, median, etc) */
  def addDerivedMetrics(df: DataFrame): DataFrame = {
    val nullColumns = df.columns.filter(p => p.startsWith(nullPrefix))
    nullColumns.foldLeft(df) {
      (tmpDf, column) =>
        tmpDf.withColumn(s"$nullRatePrefix${column.stripPrefix(nullPrefix)}",
          tmpDf.col(column) / tmpDf.col(Seq(totalColumn, api.Operation.COUNT).mkString("_")))
    }
  }

  /** For the schema of the data define metrics to be aggregated */
  def buildMetrics(fields: Array[(String, api.DataType)]): Seq[MetricTransform] = {
    val metrics = fields.flatMap {
      case (name, dataType) =>
        if (api.DataType.isNumeric(dataType)) {
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

  /** Navigate the dataframe and compute statistics partitioned by date stamp
    *
    * Partitioned by day version of the normalized summary. Useful for scheduling a job that computes daily stats.
    * Returns a KvRdd to be able to be pushed into a KvStore for fetching and merging. As well as a dataframe for
    * storing in hive.
    */
  def dailySummary(inputDf: DataFrame, keys: Seq[String], sample: Double = 1.0): KvRdd = {
    val noKeysDf = inputDf.select(inputDf.columns.filter(colName => !keys.contains(colName))
      .map(colName => new Column(colName)): _*)
    val metrics = buildMetrics(Conversions.toChrononSchema(noKeysDf.schema))
    val selectedDf = noKeysDf.select(col(api.Constants.PartitionColumn) +: metrics.map(m => m.expression): _*).toDF(api.Constants.PartitionColumn +: metrics.map(m => m.name): _*)
    val partitionIdx = selectedDf.schema.fieldIndex(api.Constants.PartitionColumn)
    val aggregator = buildAggregator(metrics, selectedDf)
    val result = selectedDf
      .sample(sample)
      .rdd
      .map(Conversions.toChrononRow(_, -1))
      .keyBy(row => row.get(partitionIdx))
      .aggregateByKey(aggregator.init)(seqOp = aggregator.updateWithReturn, combOp = aggregator.merge)
      .mapValues(aggregator.normalize(_))
      .map{ case (k, v) => (Array(k), v)}  // To use KvRdd

    implicit val sparkSession = inputDf.sparkSession
    KvRdd(
      result,
      StructType(Array(StructField(api.Constants.PartitionColumn, StringType))),
      Conversions.fromChrononSchema(aggregator.irSchema)
    )
  }

  /** Navigate the dataframe and compute the statistics.
    *
    * Main job to produce a normalized DataFrame with Statistics for the columns.
    * Uses tree aggregate and does not partition, useful for multiple partition computation.
    */
  def normalizedSummary(inputDf: DataFrame, keys: Seq[String], sample: Double = 1.0): DataFrame = {
    val noKeysDf = inputDf.select(inputDf.columns.filter(colName => !keys.contains(colName))
      .map(colName => new Column(colName)): _*)
    val metrics = buildMetrics(Conversions.toChrononSchema(noKeysDf.schema))
    val selectedDf = noKeysDf.select(metrics.map(m => m.expression): _*).toDF(metrics.map(m => m.name): _*)
    val aggregator = buildAggregator(metrics, selectedDf)
    val result = selectedDf
      .sample(sample)
      .rdd
      .map(Conversions.toChrononRow(_, -1))
      .treeAggregate(aggregator.init)(seqOp = aggregator.updateWithReturn, combOp = aggregator.merge)
    val finalized = aggregator.normalize(result)

    // Pack and send as a dataframe.
    val resultRow = Conversions.toSparkRow(finalized, api.StructType.from("", aggregator.irSchema)).asInstanceOf[GenericRow]
    val rdd: RDD[SparkRow] = inputDf.sparkSession.sparkContext.parallelize(Seq(resultRow))
    val df = inputDf.sparkSession.createDataFrame(rdd, Conversions.fromChrononSchema(aggregator.irSchema))
    addDerivedMetrics(df)
  }

  /** Run summaries in the dataframes, and enrich with drifts on percentiles.
    *
    * Eventually set it such that drift can be calculated from normalized DataFrame.
    * The idea is leverage stored DataFrame against a computed DataFrame and produce the stats.
    * Then it can leverage partial summaries to identify drifts and values to trigger alerting.
    */
  def summaryWithDrift(baseDf: DataFrame, compareDf: DataFrame, keys: Seq[String]): Array[Double] = {
    val df = Seq(baseDf, compareDf)
    val summaries = df.map(normalizedSummary(_, keys))
    val data = summaries.map(_.first())
    val percentileIndices = summaries(0).columns
      .filter(_.endsWith(api.Operation.APPROX_PERCENTILE.toString.toLowerCase()))
      .map(summaries(0).schema.fieldIndex)
    val linfs = new Array[Double](percentileIndices.length)
    val agg = new ApproxPercentiles()
    var i = 0
    while (i < linfs.length) {
      val sketches = data.map{ row => agg.denormalize(row.get(percentileIndices(i)))}
      linfs(i) = KllSketchDistance.numericalDistance(sketches(0), sketches(1))
      i += 1
    }
    linfs
  }
}
