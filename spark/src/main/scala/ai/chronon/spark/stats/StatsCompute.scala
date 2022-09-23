package ai.chronon.spark.stats

import ai.chronon.aggregator.row.RowAggregator
import ai.chronon.api.Extensions.WindowUtils
import ai.chronon.api._
import ai.chronon.spark.Conversions
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.{Column, DataFrame, functions, Row => SparkRow}

import java.util
import collection.JavaConversions._

object StatsGenerator {

  // TODO: unify with OOC
  case class MetricTransform(name: String, expression: Column, operation: Operation, argMap: util.Map[String, String] = null)

  private val nullPrefix = "nulls__"
  private val nullRatePrefix = "null_rates__"
  private val totalColumn = "total"
  // Stats applied to any column
  def anyTransforms(column: Column): Seq[MetricTransform] = Seq(
      MetricTransform(s"$nullPrefix$column", column.isNull, operation = Operation.SUM)
    , MetricTransform(s"$column", column, operation = Operation.APPROX_UNIQUE_COUNT)
  )

  // Stats applied to numeric columns
  def numericTransforms(column: Column): Seq[MetricTransform] = anyTransforms(column) ++ Seq(
    MetricTransform(s"$column", column, operation = Operation.APPROX_PERCENTILE))

  def addDerivedMetrics(df: DataFrame): DataFrame = {
    val nullColumns = df.columns.filter(p => p.startsWith(nullPrefix))
    val withNullRates = nullColumns.foldLeft(df){
      (tmpDf, column) =>
        tmpDf.withColumn(s"${nullRatePrefix}_${column.stripPrefix(nullPrefix)}",
          tmpDf.col(column) / tmpDf.col(Seq(totalColumn, Operation.COUNT).mkString("_")))}
    val percentileColumns = df.columns.filter(p => p.endsWith(Operation.APPROX_PERCENTILE.toString.toLowerCase()))
    val withPercentiles = percentileColumns.foldLeft(withNullRates) {
      (tmpDf, column) =>
        tmpDf.withColumn(s"p50_$column", tmpDf.col(column)(0))
    }
    withPercentiles
  }

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

  // Navigate the dataframe and compute the statistics.
  def summary(inputDf: DataFrame, keys: Seq[String]): DataFrame = {
    val noKeysDf = inputDf.select(inputDf.columns.filter(colName => !keys.contains(colName))
      .map(colName => new Column(colName)): _*)
    val metrics = buildMetrics(Conversions.toChrononSchema(noKeysDf.schema))
    val selectedDf = noKeysDf.select(metrics.map(m => m.expression): _*).toDF(metrics.map(m => m.name): _*)
    val aggregator = buildAggregator(metrics, selectedDf)
    val result = selectedDf
      .rdd
      .map(Conversions.toChrononRow(_, -1))
      .treeAggregate(aggregator.init)(seqOp = aggregator.updateWithReturn, combOp = aggregator.merge)
    val finalized = aggregator.finalize(result)

    // Pack and send as a dataframe.
    val resultRow = Conversions.toSparkRow(finalized, StructType.from("", aggregator.outputSchema)).asInstanceOf[GenericRow]
    val rdd: RDD[SparkRow] = inputDf.sparkSession.sparkContext.parallelize(Seq(resultRow))
    val df = inputDf.sparkSession.createDataFrame(rdd, Conversions.fromChrononSchema(aggregator.outputSchema))
    addDerivedMetrics(df)
  }
}
