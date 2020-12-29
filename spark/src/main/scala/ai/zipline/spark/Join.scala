package ai.zipline.spark
import ai.zipline.api.Config.Accuracy._
import ai.zipline.api.Config.DataModel._
import ai.zipline.api.Config.{Constants, JoinPart, Join => JoinConf}
import ai.zipline.spark.Extensions._
import org.apache.spark.sql.DataFrame

class Join(joinConf: JoinConf, endPartition: String, namespace: String, tableUtils: TableUtils) {

  private val outputTable = s"$namespace.${joinConf.metadata.cleanName}"

  private lazy val leftUnfilledRange: PartitionRange = tableUtils.fillableRange(
    outputTable,
    PartitionRange(joinConf.startPartition, endPartition),
    Option(joinConf.table).toSeq)

  private val leftDf: DataFrame =
    if (joinConf.query != null) {
      Staging(s"${outputTable}_left", tableUtils, leftUnfilledRange)
        .query(joinConf.query, Option(joinConf.table).toSeq)
    } else {
      tableUtils.sql(leftUnfilledRange.scanQuery(joinConf.table))
    }

  private lazy val leftTimeRange = leftDf.timeRange

  def joinWithLeft(leftDf: DataFrame, rightDf: DataFrame, additionalKey: String, joinPart: JoinPart): DataFrame = {
    val replacedKeys = joinPart.groupBy.keys.toArray

    // apply key-renaming to key columns
    val keyRenamedRight = Option(joinPart.keyRenaming)
      .getOrElse(Map.empty)
      .foldLeft(rightDf) {
        case (right, (leftKey, rightKey)) => {
          replacedKeys.update(replacedKeys.indexOf(rightKey), leftKey)
          right.withColumnRenamed(rightKey, leftKey)
        }
      }

    // append prefixes to non key columns
    val renamedRight = Option(joinPart.prefix)
      .map { prefix =>
        val nonValueColumns =
          replacedKeys ++ Array(Constants.TimeColumn, Constants.PartitionColumn, Constants.TimePartitionColumn)
        keyRenamedRight.schema.names.foldLeft(keyRenamedRight) { (renamed, column) =>
          if (!nonValueColumns.contains(column)) {
            renamed.withColumnRenamed(column, s"${prefix}_$column")
          } else {
            renamed
          }
        }
      }
      .getOrElse(keyRenamedRight)

    val keys = replacedKeys :+ additionalKey
    val joinableLeft = if (additionalKey == Constants.TimePartitionColumn) {
      leftDf.withTimestampBasedPartition(Constants.TimePartitionColumn)
    } else {
      leftDf
    }

    println("Internal Join keys: " + keys.mkString(", "))
    println("Left Schema:")
    println(joinableLeft.schema.pretty)
    println("Right Schema:")
    println(renamedRight.schema.pretty)
    println()
//    joinableLeft.show()
//    println("Right sample data:")
//    joinableRight.show()
//    println("Right count: " + joinableRight.count())
//    println("Left count: " + joinableLeft.count())
    val result = joinableLeft.nullSafeJoin(renamedRight, keys, "left")
//    println(s"Left join result count: ${result.count()}")
    result.drop(Constants.TimePartitionColumn)
  }

  def computeJoinPart(joinPart: JoinPart): (DataFrame, String) = {
    // no-agg case - additional key, besides the user specified key, is simply the ds
    if (joinPart.groupBy.aggregations == null) {
      return GroupBy.from(joinPart.groupBy, leftUnfilledRange, tableUtils).preAggregated -> Constants.PartitionColumn
    }

    println(s"""
         |JoinPart Info:
         |  part name : ${joinPart.groupBy.metadata.name}, 
         |  left type : ${joinConf.dataModel}, 
         |  right type: ${joinPart.groupBy.dataModel}, 
         |  accuracy  : ${joinPart.accuracy}
         |""".stripMargin)

    (joinConf.dataModel, joinPart.groupBy.dataModel, joinPart.accuracy) match {
      case (Entities, Events, _) => {
        GroupBy
          .from(joinPart.groupBy, leftUnfilledRange, tableUtils)
          .snapshotEvents(leftUnfilledRange) -> Constants.PartitionColumn
      }
      case (Entities, Entities, _) => {
        GroupBy.from(joinPart.groupBy, leftUnfilledRange, tableUtils).snapshotEntities -> Constants.PartitionColumn
      }
      case (Events, Events, null | Temporal) => {
        lazy val renamedLeft = Option(joinPart.keyRenaming)
          .getOrElse(Map.empty)
          .foldLeft(leftDf) {
            case (left, (leftKey, rightKey)) => left.withColumnRenamed(leftKey, rightKey)
          }
        GroupBy
          .from(joinPart.groupBy, leftTimeRange.toPartitionRange, tableUtils)
          .temporalEvents(renamedLeft, Some(leftTimeRange)) -> Constants.TimeColumn
      }
      case (Events, Events, Snapshot) => {
        val leftTimePartitionRange = leftTimeRange.toPartitionRange
        GroupBy
          .from(joinPart.groupBy, leftTimePartitionRange, tableUtils)
          .snapshotEvents(leftTimePartitionRange)
          .withColumnRenamed(Constants.PartitionColumn, Constants.TimePartitionColumn) -> Constants.TimePartitionColumn
      }
      case (Events, Entities, null | Snapshot) => {
        val PartitionRange(start, end) = leftTimeRange.toPartitionRange
        val rightRange = PartitionRange(Constants.Partition.before(start), Constants.Partition.before(end))
        GroupBy
          .from(joinPart.groupBy, rightRange, tableUtils)
          .snapshotEntities
          .withColumnRenamed(Constants.PartitionColumn, Constants.TimePartitionColumn) -> Constants.TimePartitionColumn
      }
      case (Events, Entities, Temporal) =>
        throw new UnsupportedOperationException("Mutations are not yet supported")
    }
  }

  val computeJoin: DataFrame = {
    println(s"left df count: ${leftDf.count()}")
    joinConf.joinParts.foldLeft(leftDf) {
      case (left, joinPart) =>
        val (rightDf, additionalKey) = computeJoinPart(joinPart)
        // TODO: implement versioning
        // TODO: Cache join parts
        joinWithLeft(left, rightDf, additionalKey, joinPart)
    }
  }
}
