package ai.chronon.spark.consistency

import ai.chronon.aggregator.row.RowAggregator
import ai.chronon.api.Extensions.{AggregationPartOps, WindowUtils}
import ai.chronon.api._
import ai.chronon.online.DataMetrics
import ai.chronon.spark.{Comparison, Conversions}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.{Column, DataFrame, functions, types, Row => SparkRow}

import java.util
import scala.collection.immutable.SortedMap
import collection.JavaConversions._

object ConsistencyMetrics {
  val loggedSuffix = "_logged"
  val backfilledSuffix = "_backfilled"
  val bins = 41
  val percentilesArgMap: util.Map[String,String] = Map(
    "k" -> "128",
    "percentiles" -> (0 to bins).map(i => i * 1.0 / bins).mkString(","))

  case class MetricTransform(name: String,
                             expr: Column,
                             operation: Operation,
                             argMap: util.Map[String, String] = null,
                             additionalExprs: Seq[(String, String)] = null)

  private def edit_distance: UserDefinedFunction =
    functions.udf((logged: Object, backfilled: Object) => EditDistance.between(logged, backfilled))

  def buildMetrics(valueFields: Array[StructField]): Seq[MetricTransform] =
    valueFields.flatMap { field =>
      val logged = functions.col(field.name + loggedSuffix)
      val backfilled = functions.col(field.name + backfilledSuffix)
      val universalMetrics = Seq(
        MetricTransform("both_null", logged.isNull.and(backfilled.isNull), Operation.SUM),
        MetricTransform("logged_null", logged.isNull.and(backfilled.isNotNull), Operation.SUM),
        MetricTransform("backfilled_null", logged.isNotNull.and(backfilled.isNull), Operation.SUM)
      )
      val smape_denom = functions.abs(logged) + functions.abs(backfilled)
      val numericMetrics = Seq(
        MetricTransform(
          "smape",
          functions
            .when(
              smape_denom.notEqual(0.0),
              (functions.abs(logged - backfilled) * 2).cast(types.DoubleType) / smape_denom
            )
            .otherwise(0.0),
          Operation.AVERAGE
        ),
        MetricTransform("logged_minus_backfilled", logged - backfilled, Operation.APPROX_PERCENTILE, argMap = percentilesArgMap),
        MetricTransform("logged", logged, Operation.APPROX_PERCENTILE, argMap = percentilesArgMap),
        MetricTransform("backfilled", backfilled, Operation.APPROX_PERCENTILE, argMap = percentilesArgMap)
      )

      val sequenceMetrics = Seq(
        MetricTransform(
          "edit_distance",
          edit_distance(logged, backfilled),
          Operation.APPROX_PERCENTILE,
          percentilesArgMap,
          additionalExprs = Seq(
            "insert" -> ".insert",
            "delete" -> ".delete"
          )
        ),
        MetricTransform("logged_length", functions.size(logged), Operation.APPROX_PERCENTILE, percentilesArgMap),
        MetricTransform("backfilled_length", functions.size(backfilled), Operation.APPROX_PERCENTILE, percentilesArgMap)
      )

      val equalityMetric =
        if (!DataType.isMap(field.fieldType))
          Some(
            MetricTransform("mismatch",
                            logged.isNotNull.and(backfilled.isNotNull).and(logged.notEqual(backfilled)),
                            Operation.SUM))
        else None

      val typeSpecificMetrics = if (DataType.isNumeric(field.fieldType)) {
        numericMetrics
      } else if (DataType.isList(field.fieldType)) {
        sequenceMetrics
      } else {
        Seq.empty[MetricTransform]
      }

      val allMetrics = (universalMetrics ++ typeSpecificMetrics ++ equalityMetric)
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
              timeBucketMinutes: Long = 60): (DataFrame, DataMetrics) = {
    // spark maps cannot be directly compared, for now we compare the string representation
    // TODO 1: For Maps, we should find missing keys, extra keys and mismatched keys
    // TODO 2: Values should have type specific comparison
    val valueSchema = valueFields.map(field =>
      field.fieldType match {
        case MapType(_, _) => StructField(field.name, StringType)
        case _             => field
      })
    val metrics = buildMetrics(valueSchema)
    val selectedDf = Comparison
      .stringifyMaps(inputDf)
      .select(metrics.map(_.expr) :+ functions.col(Constants.TimeColumn): _*)
    val secondPassSelects = metrics.flatMap { metric =>
      if (metric.additionalExprs != null) {
        metric.additionalExprs.map { case (name, expr) => (s"$expr as $name") }
      } else {
        Seq(metric.name)
      }
    }
    val secondPassDf = selectedDf.selectExpr(secondPassSelects :+ Constants.TimeColumn: _*)
    val rowAggregator = buildRowAggregator(metrics, secondPassDf)
    val bucketMs = 1000 * 60 * timeBucketMinutes
    val tsIndex = secondPassDf.schema.fieldIndex(Constants.TimeColumn)
    val outputColumns = rowAggregator.aggregationParts.map(_.outputColumnName).toArray
    def sortedMap(vals: Seq[(String, Any)]) = SortedMap.empty[String, Any] ++ vals
    val resultRdd = secondPassDf.rdd
      .keyBy(row => (row.getLong(tsIndex) / bucketMs) * bucketMs) // bin
      .mapValues(Conversions.toChrononRow(_, -1))
      .aggregateByKey(rowAggregator.init)(rowAggregator.updateWithReturn, rowAggregator.merge) // aggregate
      .mapValues(rowAggregator.finalize)

    val resultRowRdd: RDD[SparkRow] = resultRdd.map {
      case (bucketStart, metrics) => new GenericRow(bucketStart +: metrics)
    }
    val resultChrononSchema = StructType.from("ooc_metrics", ("ts", LongType) +: rowAggregator.outputSchema)
    val resultSparkSchema = Conversions.fromChrononSchema(resultChrononSchema)
    val resultDf = inputDf.sparkSession.createDataFrame(resultRowRdd, resultSparkSchema)

    val result = resultRdd
      .collect()
      .sortBy(_._1)
      .map { case (bucketStart, vals) => bucketStart -> sortedMap(outputColumns.zip(vals)) }
    (resultDf -> DataMetrics(result))
  }
}
