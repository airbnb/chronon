package ai.zipline.spark
import ai.zipline.api.Config.Accuracy._
import ai.zipline.api.Config.DataModel._
import ai.zipline.api.Config.{Constants, JoinPart, Join => JoinConf}
import ai.zipline.spark.Extensions._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.from_unixtime

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

  def joinWithLeft(leftDf: DataFrame, rightDf: DataFrame, joinPart: JoinPart): DataFrame = {
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

    val (joinableLeft, joinableRight, additionalKey) =
      if (joinConf.dataModel == Events && !leftDf.schema.names.contains(Constants.TimePartitionColumn)) {
        val left = leftDf.withTimestampBasedPartition(Constants.TimePartitionColumn)
        val right = renamedRight.withColumnRenamed(Constants.PartitionColumn, Constants.TimePartitionColumn)
        (left, right, Constants.TimePartitionColumn)
      } else {
        // for all other cases, the additional key is simply based on accuracy
        (leftDf,
         renamedRight,
         (joinConf.dataModel, joinPart.groupBy.accuracy) match {
           case (Events, Temporal) => Constants.TimeColumn
           case _                  => Constants.PartitionColumn
         })
      }

    val keys = replacedKeys :+ additionalKey
    println("Internal Join keys: " + keys.mkString(", "))
    println("Left sample Schema:")
    println(joinableLeft.schema.pretty)
    println("Right sample Schema:")
    println(joinableRight.schema.pretty)
    println("Left sample data:")
    joinableLeft.show()
    println("Right sample data:")
    joinableRight.show()

    joinableLeft.join(joinableRight, keys, "left_outer")
  }

  def computeJoinPart(joinPart: JoinPart): DataFrame = {
    // no-agg case
    if (joinPart.groupBy.aggregations == null) {
      return GroupBy.from(joinPart.groupBy, leftUnfilledRange, tableUtils).preAggregated
    }

    println(s"""
         |JoinPart Info:
         |  part name : ${joinPart.groupBy.metadata.name}, 
         |  left type : ${joinConf.dataModel}, 
         |  right type: ${joinPart.groupBy.dataModel}, 
         |  accuracy  : ${joinPart.groupBy.accuracy}
         |""".stripMargin)

    (joinConf.dataModel, joinPart.groupBy.dataModel, joinPart.groupBy.accuracy) match {
      case (Entities, Events, _) => {
        GroupBy
          .from(joinPart.groupBy, leftUnfilledRange, tableUtils)
          .snapshotEvents(leftUnfilledRange)
      }
      case (Entities, Entities, _) => {
        GroupBy.from(joinPart.groupBy, leftUnfilledRange, tableUtils).snapshotEntities
      }
      case (Events, Events, Temporal) => {
        lazy val renamedLeft = Option(joinPart.keyRenaming)
          .getOrElse(Map.empty)
          .foldLeft(leftDf) {
            case (left, (leftKey, rightKey)) => left.withColumnRenamed(leftKey, rightKey)
          }
        GroupBy
          .from(joinPart.groupBy, leftDf.timeRange.toPartitionRange, tableUtils)
          .temporalEvents(renamedLeft)
      }
      case (Events, Events, Snapshot) => {
        val leftTimePartitionRange = leftDf.timeRange.toPartitionRange
        GroupBy
          .from(joinPart.groupBy, leftTimePartitionRange, tableUtils)
          .snapshotEvents(leftTimePartitionRange)
      }
      case (Events, Entities, Snapshot) => {
        val PartitionRange(start, end) = leftDf.timeRange.toPartitionRange
        val rightRange = PartitionRange(Constants.Partition.before(start), Constants.Partition.before(end))
        GroupBy.from(joinPart.groupBy, rightRange, tableUtils).snapshotEntities
      }
      case (Events, Entities, Temporal) =>
        throw new UnsupportedOperationException("Mutations are not yet supported")
    }
  }

  val computeJoin: DataFrame = {
    joinConf.joinParts.foldLeft(leftDf) {
      case (left, joinPart) =>
        // TODO: implement versioning and join part caching here
        val right = computeJoinPart(joinPart)
        println("==== JoinPart result - right side - unrenamed ====")
        right.show()

        joinWithLeft(left, right, joinPart)
    }
  }
}
