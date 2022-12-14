package ai.chronon.spark

import ai.chronon.api.DataModel.{Entities, Events}
import ai.chronon.api.Extensions._
import ai.chronon.api.{Accuracy, Constants, JoinPart, Source}
import ai.chronon.spark.Extensions._
import org.apache.spark.sql.DataFrame

object JoinUtils {

  /***
    * Util methods for join computation
    */

  def leftDf(joinConf: ai.chronon.api.Join, range: PartitionRange, tableUtils: TableUtils): Option[DataFrame] = {
    val timeProjection = if (joinConf.left.dataModel == Events) {
      Seq(Constants.TimeColumn -> Option(joinConf.left.query).map(_.timeColumn).orNull)
    } else {
      Seq()
    }
    val scanQuery = range.genScanQuery(joinConf.left.query,
      joinConf.left.table,
      fillIfAbsent = Map(Constants.PartitionColumn -> null) ++ timeProjection)

    val df = tableUtils.sql(scanQuery)
    val skewFilter = joinConf.skewFilter()
    val result = skewFilter
      .map(sf => {
        println(s"left skew filter: $sf")
        df.filter(sf)
      })
      .getOrElse(df)
    if (result.isEmpty) {
      println(s"Left side query below produced 0 rows in range $range. Query:\n$scanQuery")
      return None
    }
    Some(result)
  }
}
