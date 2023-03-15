package ai.chronon.aggregator.row

import ai.chronon.api
import ai.chronon.api.Extensions._
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

  val nullPrefix = "null__"
  val nullRatePrefix = "null_rate__"
  val totalColumn = "total"
  val finalizedPercentiles = (5 until 1000 by 5).map(_.toDouble / 1000)
  val ignoreColumns = Seq(api.Constants.TimeColumn, api.Constants.PartitionColumn)

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
  case class MetricTransform(name: String, expression: InputTransform, operation: api.Operation, prefix: String = "", argMap: util.Map[String, String] = null)

  /**
    * A valueSchema (for join) and Metric list define uniquely the IRSchema to be used for the statistics.
    * In order to support custom storage for statistic percentiles this method would need to be modified.
    * IR Schemas are used to decode streaming partial aggregations as well as KvStore partial stats.
    */
  def statsIrSchema(valueSchema: api.StructType): api.StructType = {
    val metrics: Seq[MetricTransform] = buildMetrics(valueSchema.unpack)
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

  /**
    * Input schema is the data required to update partial aggregations / stats.
    *
    * Given a valueSchema and a metric transform list, defines the schema expected by the Stats aggregator (online and offline)
    */
  def statsInputSchema(valueSchema: api.StructType): api.StructType = {
    val metrics = buildMetrics(valueSchema.unpack)
    val schemaMap = valueSchema.unpack.map {
      v => v._1 -> v._2
    }.toMap
    api.StructType.from("InputSchema", metrics.map {
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
