package ai.chronon.spark

import ai.chronon.api
import ai.chronon.api.Constants
import ai.chronon.api.Extensions._

object SourceUtils {
  def makeTableToPartitionOverride(sourceConf: api.Source): Map[String, String] = Map(
    sourceConf.table -> {
      if (sourceConf.query != null && sourceConf.query.selects != null) {
        sourceConf.query.selects.getOrDefault(Constants.PartitionColumn, Constants.PartitionColumn)
      } else {
        Constants.PartitionColumn
      }
    }
  )
}
