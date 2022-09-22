package ai.chronon.spark.stats

import ai.chronon.aggregator.row.RowAggregator
import ai.chronon.api.Extensions.WindowUtils
import ai.chronon.api._
import ai.chronon.spark.Conversions
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.{Column, DataFrame, SparkSession, functions, Row => SparkRow}

import scala.collection.JavaConverters._

object StatsGenerator {
  /*
   * Base definition for stats metrics.
   */
  case class MetricTransform(name: String, expression: Column, operation: Operation)

  /*
   * Stats applied to any column
   */
  def anyTransforms(column: Column): Seq[MetricTransform] = Seq(
      MetricTransform(s"null_count_$column", column.isNull, operation = Operation.SUM)
    // TODO: Figure out serialization issue.
    //, MetricTransform(s"approx_unique_count_$column", column, operation = Operation.APPROX_UNIQUE_COUNT)
  )

  /*
   * Stats applied to numeric columns
   */
  def numericTransforms(column: Column): Seq[MetricTransform] = anyTransforms(column) ++ Seq(
    //MetricTransform(s"tdigest_$column", column, operation = Operation.APPROX_PERCENTILE)
    // T-Digest placeholder MetricTransform...
  )

  def buildMetrics(fields: Array[(String, DataType)]): Seq[MetricTransform] = {
    val metrics = fields.flatMap {
      case (name, dataType) =>
        if (DataType.isNumeric(dataType)) {
          numericTransforms(functions.col(name))
        } else {
          anyTransforms(functions.col(name))
        }
    }
    metrics :+ MetricTransform("total", lit(true), Operation.COUNT)
  }

  def buildAggregator(metrics: Seq[MetricTransform], inputDf: DataFrame): RowAggregator = {
    val schema = Conversions.toChrononSchema(inputDf.schema)
    val aggParts = metrics.flatMap { m =>
      def buildAggPart(name: String): AggregationPart = {
        val aggPart = new AggregationPart()
        aggPart.setInputColumn(name)
        aggPart.setOperation(m.operation)
        aggPart.setWindow(WindowUtils.Unbounded)
        aggPart
      }
      Seq(buildAggPart(m.name))
    }
    new RowAggregator(schema, aggParts)
  }

  /*
   * Navigate the dataframe and compute the statistics.
   */
  def summary(inputDf: DataFrame, keys: Seq[String], sparkSession: SparkSession): Array[Any] = {
    val noKeysDf = inputDf.select(inputDf.columns.filter(colName => !keys.contains(colName))
      .map(colName => new Column(colName)): _*)
    val metrics = buildMetrics(Conversions.toChrononSchema(noKeysDf.schema))
    val selectedDf = noKeysDf.select(metrics.map(m => m.expression): _*).toDF(metrics.map(m => m.name): _*)
    selectedDf.printSchema()
    val aggregator = buildAggregator(metrics, selectedDf)
    val result = selectedDf
      .rdd
      .map(Conversions.toChrononRow(_, -1))
      .treeAggregate(aggregator.init)(seqOp = aggregator.updateWithReturn, combOp = aggregator.merge)

    result
    // Having the result of the aggregations we could do post process to return null rates for example.
    //sparkSession.createDataFrame(List(new GenericRow(resultRdd)).map(a => a.asInstanceOf[SparkRow]).asJava, Conversions.fromChrononSchema(aggregator.outputSchema))
  }
}
