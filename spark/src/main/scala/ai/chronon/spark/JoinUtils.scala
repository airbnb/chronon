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

  def joinWithLeft(leftDf: DataFrame,
                   rightDf: DataFrame,
                   joinPart: JoinPart): DataFrame = {
    val partLeftKeys = joinPart.rightToLeft.values.toArray
    // apply key-renaming to key columns
    val keyRenamedRight = joinPart.rightToLeft.foldLeft(rightDf) {
      case (rightDf, (rightKey, leftKey)) => rightDf.withColumnRenamed(rightKey, leftKey)
    }

    val nonValueColumns = joinPart.rightToLeft.keys.toArray ++ Array(Constants.TimeColumn,
      Constants.PartitionColumn,
      Constants.TimePartitionColumn,
      Constants.LabelPartitionColumn)
    val valueColumns = rightDf.schema.names.filterNot(nonValueColumns.contains)
    val prefixedRight = keyRenamedRight.prefixColumnNames(joinPart.fullPrefix, valueColumns)

    val partName = joinPart.groupBy.metaData.name

    println(s"""Join keys for $partName: ${partLeftKeys.mkString(", ")}
               |Left Schema:
               |${leftDf.schema.pretty}
               |
               |Right Schema:
               |${prefixedRight.schema.pretty}
               |
               |""".stripMargin)

    leftDf.validateJoinKeys(prefixedRight, partLeftKeys)
    leftDf.join(prefixedRight, partLeftKeys, "left_outer")
  }

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
