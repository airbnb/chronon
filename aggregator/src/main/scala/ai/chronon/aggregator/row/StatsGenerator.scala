package ai.chronon.aggregator.row

import ai.chronon.api
import ai.chronon.api.Extensions._
object StatsGenerator {

  def buildAggPart(field: api.StructField): api.AggregationPart = {
    val aggPart = new api.AggregationPart()
    val operation = field.name.split("_").last match {
      case "sum" => api.Operation.SUM
      case "percentiles" => api.Operation.APPROX_PERCENTILE
      case "count" => api.Operation.COUNT
    }
    aggPart.setInputColumn(field.name.stripSuffix(s"_${operation.stringified}"))
    aggPart.setOperation(operation)
    aggPart.setWindow(WindowUtils.Unbounded)
    aggPart
  }

    /** Build RowAggregator to use for computing stats on a dataframe based on metrics */
    def buildAggregator(schema: api.DataType): RowAggregator = {
      val aggParts = schema.asInstanceOf[api.StructType].map(buildAggPart(_))
      val chrononSchema = schema.asInstanceOf[api.StructType].fields.map {
        field => (field.name, field.fieldType)
      }
      new RowAggregator(chrononSchema, aggParts)
    }
}
