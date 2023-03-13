package ai.chronon.aggregator.row

import ai.chronon.api
import ai.chronon.api.Extensions._
import com.google.gson.Gson

import scala.util.ScalaVersionSpecificCollectionsConverter
import java.util

/**
  * Stats aggregation happens in the spark module and relies on different data types to build the right aggregator.
  * In order to serve stats IRs need to be merged and finalized. So an aggregator is required.
  *
  * StatsGenerator takes care of computation of metadata for dataframes
  * This applies to drifts as well between two dataframes.
  *
  * Metrics define the order for the IR schema. This way there's consistency between the aggregator generated from
  * the joinConf valueSchema in fetcher and the SummaryJob uploaded value Schema.
  */
object StatsGenerator {

  val nullPrefix = "null__"
  val nullRatePrefix = "null_rate__"
  val totalColumn = "total"
  val finalizedPercentiles = (5 until 1000 by 5).map(_.toDouble / 1000)
  val ignoreColumns = Seq(api.Constants.TimeColumn, api.Constants.PartitionColumn)

  object InputTransform extends Enumeration {
    type InputTransform = Value
    val IsNull, Raw, One = Value
  }
  import InputTransform._
  case class MetricTransform(name: String, expression: InputTransform, operation: api.Operation, prefix: String = "", argMap: util.Map[String, String] = null)

  def statsIrSchema(valueSchema: api.StructType): api.StructType = {
    val metrics = buildMetrics(valueSchema.unpack)
    val schemaMap = valueSchema.unpack.map {
      v => v._1 -> v._2
    }.toMap
    api.StructType.from("IrSchema", metrics.map {
      m =>
        m.expression match {
          case InputTransform.IsNull =>
            (s"${m.prefix}${m.name}_sum", api.LongType)
          case InputTransform.One =>
            (s"${m.prefix}${m.name}_count", api.LongType)
          case InputTransform.Raw =>
            val aggPart = buildAggPart(m)
            val colAggregator = ColumnAggregator.construct(schemaMap(m.name), aggPart, ColumnIndices(1,1), None)
            (s"${m.name}_${aggPart.operation.name().toLowerCase()}", colAggregator.irType)
        }

    }.toArray)
  }

  def statsInputSchema(valueSchema: api.StructType): api.StructType = {
    val metrics = buildMetrics(valueSchema.unpack)
    val schemaMap = valueSchema.unpack.map {
      v => v._1 -> v._2
    }.toMap
    api.StructType.from("IrSchema", metrics.map {
      m =>
        m.expression match {
          case InputTransform.IsNull =>
            (s"${m.prefix}${m.name}", api.LongType)
          case InputTransform.One =>
            (s"${m.prefix}${m.name}", api.LongType)
          case InputTransform.Raw =>
            (m.name, schemaMap(m.name))
        }

    }.toArray)
  }

  def buildAggPart(m: MetricTransform): api.AggregationPart = {
    val aggPart = new api.AggregationPart()
    aggPart.setInputColumn(s"${m.prefix}${m.name}")
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
    Seq(MetricTransform(column, InputTransform.IsNull, operation = api.Operation.SUM, prefix = nullPrefix))

  /** Stats applied to numeric columns */
  def numericTransforms(column: String): Seq[MetricTransform] =
    anyTransforms(column) ++ Seq(
      MetricTransform(column,
        InputTransform.Raw,
        operation = api.Operation.APPROX_PERCENTILE,
        argMap = ScalaVersionSpecificCollectionsConverter.convertScalaMapToJava(Map("percentiles" -> s"[${finalizedPercentiles.mkString(", ")}]"))))

  /** For the schema of the data define metrics to be aggregated */
  def buildMetrics(fields: Seq[(String, api.DataType)]): Seq[MetricTransform] = {
    val metrics = fields.flatMap {
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
    }.sortBy(_.name)
    metrics :+ MetricTransform(totalColumn, InputTransform.One, api.Operation.COUNT)
  }
}
