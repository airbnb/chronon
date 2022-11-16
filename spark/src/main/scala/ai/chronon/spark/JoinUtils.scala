package ai.chronon.spark

import ai.chronon.api.DataModel.Entities
import ai.chronon.api.Extensions._
import ai.chronon.api.{Accuracy, Constants, JoinPart, Source}
import ai.chronon.spark.Extensions._
import org.apache.spark.sql.DataFrame

object JoinUtils {
  /***
   * Util method to join left dataframe with right.
   *
   * @param leftDf
   * @param rightDf
   * @param joinPart
   * @param left
   * @param dateOffset Default increment one day to align with left side ts_ds
      because one day was decremented from the partition range for snapshot accuracy
      No increment for label join
   * @return
   */

  def joinWithLeft(leftDf: DataFrame, rightDf: DataFrame, joinPart: JoinPart,
                   left: Source,
                   dateOffset: Int = 1): DataFrame = {
    val partLeftKeys = joinPart.rightToLeft.values.toArray

    // besides the ones specified in the group-by
    val additionalKeys: Seq[String] = {
      if (left.dataModel == Entities) {
        Seq(Constants.PartitionColumn)
      } else if (joinPart.groupBy.inferredAccuracy == Accuracy.TEMPORAL) {
        Seq(Constants.TimeColumn, Constants.PartitionColumn)
      } else { // left-events + snapshot => join-key = ds_of_left_ts
        Seq(Constants.TimePartitionColumn)
      }
    }

    // apply key-renaming to key columns
    val keyRenamedRight = joinPart.rightToLeft.foldLeft(rightDf) {
      case (rightDf, (rightKey, leftKey)) => rightDf.withColumnRenamed(rightKey, leftKey)
    }

    val nonValueColumns = joinPart.rightToLeft.keys.toArray ++ Array(Constants.TimeColumn,
      Constants.PartitionColumn,
      Constants.TimePartitionColumn)
    val valueColumns = rightDf.schema.names.filterNot(nonValueColumns.contains)
    val prefixedRight = keyRenamedRight.prefixColumnNames(joinPart.fullPrefix, valueColumns)

    // compute join keys, besides the groupBy keys -  like ds, ts etc.,
    val keys = partLeftKeys ++ additionalKeys

    val partName = joinPart.groupBy.metaData.name

    println(s"""Join keys for $partName: ${keys.mkString(", ")}
               |Left Schema:
               |${leftDf.schema.pretty}
               |
               |Right Schema:
               |${prefixedRight.schema.pretty}
               |
               |""".stripMargin)

    import org.apache.spark.sql.functions.{col, date_add, date_format}
    val joinableRight = if (additionalKeys.contains(Constants.TimePartitionColumn)) {
      prefixedRight
        .withColumn(Constants.TimePartitionColumn,
          date_format(date_add(col(Constants.PartitionColumn), dateOffset), Constants.Partition.format))
        .drop(Constants.PartitionColumn)
    } else {
      prefixedRight
    }

    leftDf.validateJoinKeys(joinableRight, keys)
    leftDf.join(joinableRight, keys, "left")
  }
}
