package ai.chronon.spark.stats

import java.util
import ai.chronon.api._
import ai.chronon.spark.PartitionRange
import org.apache.spark.sql.{Column, DataFrame}

object StatsGenerator {
  case class MetricTransform(name: String,
                             operation: Operation,
                             argMap: util.Map[String, String] = null,
                             additionalExprs: Seq[(String, String)] = null)

  val numericAggregations = Seq(MetricTransform())

  class StatsAggregator {
    def init = ???
    def update = ???
    def merge = ???
  }
  /*
   * Navigate the dataframe and compute the statistics.
   */
  def summary(df: DataFrame, keys: Seq[String], partitionRange: PartitionRange): DataFrame = {
    val aggregator = new StatsAggregator
    df
      .select(df.columns.filter(!keys.contains(_)).map(colName => new Column(colName)): _*) // Remove the keys.
      .rdd
      .treeAggregate(aggregator.init)(
        seqOp = aggregator.update, combOp = aggregator.merge
      )
  }
}
