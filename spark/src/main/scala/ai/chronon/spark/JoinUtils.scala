package ai.chronon.spark

import ai.chronon.api.DataModel.{Entities, Events}
import ai.chronon.api.Extensions._
import ai.chronon.api.{Accuracy, Constants, JoinPart, QueryUtils, Source}
import ai.chronon.spark.Extensions._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.lit

import scala.collection.JavaConverters._

object JoinUtils {

  /***
    * Util methods for join computation
    */

  def leftDf(joinConf: ai.chronon.api.Join, range: PartitionRange, tableUtils: BaseTableUtils): Option[DataFrame] = {
    val timeProjection = if (joinConf.left.dataModel == Events) {
      Seq(Constants.TimeColumn -> Option(joinConf.left.query).map(_.timeColumn).orNull)
    } else {
      Seq()
    }
    val isLeftUnpartitionedTable = tableUtils.isUnpartitionedTable(joinConf.left)
    val scanQuery = if (isLeftUnpartitionedTable) {
      if (joinConf.left.isSetEvents) {
        range.genScanQueryBasedOnTime(joinConf.left.query,
          joinConf.left.table,
          fillIfAbsent = timeProjection.toMap)
      } else {
        QueryUtils.build(selects = Option(joinConf.left.query).map { query => Option(query.selects).map(_.asScala.toMap).orNull }.orNull,
          from = joinConf.left.table,
          wheres = Option(joinConf.left.query).flatMap(q => Option(q.wheres).map(_.asScala)).getOrElse(Seq.empty[String]),
          fillIfAbsent = timeProjection.toMap)
      }
    } else {
      range.genScanQuery(joinConf.left.query,
        joinConf.left.table,
        fillIfAbsent = Map(Constants.PartitionColumn -> null) ++ timeProjection)
    }
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
    if (isLeftUnpartitionedTable && joinConf.left.isSetEntities) {
      // For unpartitioned left entities, one day's snapshot of data is used and
      // so the partition column is set to the end of the range (and for unpartitioned left entities, only
      // a single output partition is computed per job run and so start partition = end partition).
      Some(result.withColumn(Constants.PartitionColumn, lit(range.end)))
    } else if (result.schema.names.contains(Constants.TimeColumn) && isLeftUnpartitionedTable) {
      Some(result.withTimeBasedColumn(Constants.PartitionColumn))
    } else {
      Some(result)
    }
  }
}
