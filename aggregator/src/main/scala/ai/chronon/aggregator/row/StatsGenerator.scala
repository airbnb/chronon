package ai.chronon.aggregator.row

import ai.chronon.api
import ai.chronon.api.Extensions._
import scala.util.ScalaVersionSpecificCollectionsConverter

/**
  * Stats aggregation happens in the spark module and relies on different data types to build the right aggregator.
  * In order to serve stats IRs need to be merged and finalized. So an aggregator is required.
  *
  * The intention in this object is to rebuild the aggregators in order to fetch IRs and finalizing them for the
  * purpose of serving metadata.
  */
object StatsGenerator {

  def buildAggPart(field: api.StructField): api.AggregationPart = {
    val aggPart = new api.AggregationPart()
    field.name.split("_").last match {
      case "sum" =>
        aggPart.setOperation(api.Operation.SUM)
      case "percentile" =>
        aggPart.setOperation(api.Operation.APPROX_PERCENTILE)
        aggPart.setArgMap(ScalaVersionSpecificCollectionsConverter.convertScalaMapToJava(Map("percentiles" -> s"[${(5 until 1000 by 5).map(_.toDouble / 1000).mkString(",")}]")))
      case "count" =>
        aggPart.setOperation(api.Operation.COUNT)
    }
    aggPart.setInputColumn(field.name)
    aggPart.setWindow(WindowUtils.Unbounded)
    aggPart
  }

    /** Build RowAggregator to use for computing stats on a dataframe based on metrics */
    def buildAggregator(schema: api.StructType): RowAggregator = {
      val aggParts = schema.map(buildAggPart(_))
      val chrononSchema = schema.fields.map { field => (field.name, field.fieldType) }
      new RowAggregator(chrononSchema, aggParts)
    }
}
