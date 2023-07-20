package ai.chronon.aggregator.row

import ai.chronon.api
import ai.chronon.api.Extensions._
import com.yahoo.memory.Memory
import com.yahoo.sketches.kll.KllFloatsSketch

import scala.collection.Seq
import scala.util.ScalaVersionSpecificCollectionsConverter
import java.util

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
    * A valueSchema (for join) and Metric list define uniquely the IRSchema to be used for the statistics.
    * In order to support custom storage for statistic percentiles this method would need to be modified.
    * IR Schemas are used to decode streaming partial aggregations as well as KvStore partial stats.
    */
  def statsIrSchema(valueSchema: api.StructType): api.StructType = {
    val metrics: Seq[MetricTransform] = buildMetrics(valueSchema.unpack)
    val schemaMap = valueSchema.unpack.map { v =>
      v._1 -> v._2
    }.toMap
    api.StructType.from(
      "IrSchema",
      metrics.map { m =>
        m.expression match {
          case InputTransform.IsNull =>
            (s"${m.name}${m.suffix}_sum", api.LongType)
          case InputTransform.One =>
            (s"${m.name}${m.suffix}_count", api.LongType)
          case InputTransform.Raw =>
            val aggPart = buildAggPart(m)
            val colAggregator = ColumnAggregator.construct(schemaMap(m.name), aggPart, ColumnIndices(1, 1), None)
            (s"${m.name}_${aggPart.operation.name().toLowerCase()}", colAggregator.irType)
        }

      }.toArray
    )
  }

  /**
    * Post processing for IRs when generating a time series of stats.
    * In the case of percentiles for examples we reduce to 5 values in order to generate candlesticks.
    */
  def SeriesFinalizer(key: String, value: AnyRef): AnyRef = {
    if (key.endsWith("percentile") && value != null) {
      val sketch = KllFloatsSketch.heapify(Memory.wrap(value.asInstanceOf[Array[Byte]]))
      return sketch.getQuantiles(finalizedPercentilesSeries).asInstanceOf[AnyRef]
    }
    value
  }

  /**
    * Input schema is the data required to update partial aggregations / stats.
    *
    * Given a valueSchema and a metric transform list, defines the schema expected by the Stats aggregator (online and offline)
    */
  def statsInputSchema(valueSchema: api.StructType): api.StructType = {
    val metrics = buildMetrics(valueSchema.unpack)
    val schemaMap = valueSchema.unpack.map { v =>
      v._1 -> v._2
    }.toMap
    api.StructType.from(
      "IrSchema",
      metrics.map { m =>
        m.expression match {
          case InputTransform.IsNull =>
            (s"${m.name}${m.suffix}", api.LongType)
          case InputTransform.One =>
            (s"${m.name}${m.suffix}", api.LongType)
          case InputTransform.Raw =>
            (m.name, schemaMap(m.name))
        }
      }.toArray
    )
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
        argMap = ScalaVersionSpecificCollectionsConverter.convertScalaMapToJava(
          Map("percentiles" -> s"[${finalizedPercentilesMerged.mkString(", ")}]"))
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
}
