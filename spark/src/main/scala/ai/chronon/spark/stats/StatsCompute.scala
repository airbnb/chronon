package ai.chronon.spark.stats

import ai.chronon.aggregator.base.ApproxPercentiles
import ai.chronon.aggregator.row.RowAggregator
import ai.chronon.api.Extensions.WindowUtils
import ai.chronon.api._
import ai.chronon.spark.Conversions
import com.yahoo.sketches.kll.KllFloatsSketch
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.{Column, DataFrame, functions, Row => SparkRow}

import java.util
import collection.JavaConversions._

object StatsGenerator {

  // TODO: unify with OOC since has the same metric transform.
  case class MetricTransform(name: String, expression: Column, operation: Operation, argMap: util.Map[String, String] = null)

  private val nullPrefix = "null__"
  private val nullRatePrefix = "null_rate__"
  private val totalColumn = "total"

  /** Stats applied to any column */
  def anyTransforms(column: Column): Seq[MetricTransform] = Seq(
      MetricTransform(s"$nullPrefix$column", column.isNull, operation = Operation.SUM)
    , MetricTransform(s"$column", column, operation = Operation.APPROX_UNIQUE_COUNT)
  )

  /** Stats applied to numeric columns */
  def numericTransforms(column: Column): Seq[MetricTransform] = anyTransforms(column) ++ Seq(
    MetricTransform(column.toString(), column, operation = Operation.APPROX_PERCENTILE, argMap = Map("percentiles" -> "[0.5, 0.95, 0.99]")))

  /** Given a summary Dataframe that computed the stats. Add derived data (example: null rate, median, etc) */
  def addDerivedMetrics(df: DataFrame): DataFrame = {
    val nullColumns = df.columns.filter(p => p.startsWith(nullPrefix))
    nullColumns.foldLeft(df) {
      (tmpDf, column) =>
        tmpDf.withColumn(s"$nullRatePrefix${column.stripPrefix(nullPrefix)}",
          tmpDf.col(column) / tmpDf.col(Seq(totalColumn, Operation.COUNT).mkString("_")))
    }
  }

  /** For the schema of the data define metrics to be aggregated */
  def buildMetrics(fields: Array[(String, DataType)]): Seq[MetricTransform] = {
    val metrics = fields.flatMap {
      case (name, dataType) =>
        if (DataType.isNumeric(dataType)) {
          numericTransforms(functions.col(name))
        } else {
          anyTransforms(functions.col(name))
        }
    }
    metrics :+ MetricTransform(totalColumn, lit(true), Operation.COUNT)
  }

  /** Build RowAggregator to use for computing stats on a dataframe based on metrics */
  def buildAggregator(metrics: Seq[MetricTransform], inputDf: DataFrame): RowAggregator = {
    val schema = Conversions.toChrononSchema(inputDf.schema)
    val aggParts = metrics.flatMap { m =>
      def buildAggPart(name: String): AggregationPart = {
        val aggPart = new AggregationPart()
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

  /** Navigate the dataframe and compute the statistics. */
  def normalizedSummary(inputDf: DataFrame, keys: Seq[String]): DataFrame = {
    val noKeysDf = inputDf.select(inputDf.columns.filter(colName => !keys.contains(colName))
      .map(colName => new Column(colName)): _*)
    val metrics = buildMetrics(Conversions.toChrononSchema(noKeysDf.schema))
    val selectedDf = noKeysDf.select(metrics.map(m => m.expression): _*).toDF(metrics.map(m => m.name): _*)
    val aggregator = buildAggregator(metrics, selectedDf)
    val result = selectedDf
      .rdd
      .map(Conversions.toChrononRow(_, -1))
      .treeAggregate(aggregator.init)(seqOp = aggregator.updateWithReturn, combOp = aggregator.merge)
    val finalized = aggregator.normalize(result)

    // Pack and send as a dataframe.
    val resultRow = Conversions.toSparkRow(finalized, StructType.from("", aggregator.irSchema)).asInstanceOf[GenericRow]
    val rdd: RDD[SparkRow] = inputDf.sparkSession.sparkContext.parallelize(Seq(resultRow))
    val df = inputDf.sparkSession.createDataFrame(rdd, Conversions.fromChrononSchema(aggregator.irSchema))
    addDerivedMetrics(df)
  }

  /** Run both summaries, find the percentiles  */
    // TODO: Build Dataframe as response. Map Keys and values.
    // TODO: Offer to bucket by time if time column is present.
  def drift(baseDf: DataFrame, compareDf: DataFrame, keys: Seq[String]): Array[Double] = {
    val df = Seq(baseDf, compareDf)
    val summaries = df.map(normalizedSummary(_, keys))
    val data = summaries.map(_.first())
    val percentileIndices = summaries(0).columns
      .filter(_.endsWith(Operation.APPROX_PERCENTILE.toString.toLowerCase()))
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
