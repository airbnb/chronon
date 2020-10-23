package ai.zipline.spark
import ai.zipline.api.Config.Accuracy._
import ai.zipline.api.Config.DataModel._
import ai.zipline.api.Config.{Constants, JoinPart, Join => JoinConf}
import ai.zipline.spark.Extensions._
import org.apache.spark.sql.DataFrame

//abstract class SparkConfRunner[Conf] {
//  protected val session = SessionUtil.createSparkSession
//  protected val tableUtils = TableUtils(session)
//  protected def run(conf: Conf, endPartition: String, outputTable: String): Unit
//}

class Join(joinConf: JoinConf, endPartition: String, outputTable: String, tableUtils: TableUtils) {

  private lazy val leftPartitionRange: PartitionRange =
    tableUtils.fillableRange(Option(joinConf.table).toSeq, outputTable, joinConf.startPartition, endPartition)

  private lazy val leftQuery: String = {
    val template = Option(joinConf.query).getOrElse(s"""
         |SELECT *
         |FROM ${joinConf.table}
         |WHERE
         |  ${Constants.PartitionColumn} >= ${Constants.StartPartitionMacro}
         |  ${Constants.PartitionColumn} <= ${Constants.EndPartitionMacro}
         |""".stripMargin)
    val substitutions = Seq(Constants.StartPartitionMacro -> Option(leftPartitionRange.start),
                            Constants.EndPartitionMacro -> Option(leftPartitionRange.end))
    substitutions.foldLeft(template) {
      case (q, (target, Some(replacement))) => q.replaceAllLiterally(target, replacement)
      case (q, (_, None))                   => q
    }
  }

  private val session = tableUtils.sparkSession
  private val leftDf = session.sql(leftQuery)
  private val leftRange: DataRange = if (joinConf.dataModel == Entities) {
    leftPartitionRange
  } else {
    leftDf.createOrReplaceTempView("left_df")
    val minMaxRows =
      session.sql(s"SELECT min(${joinConf.timeExpression}), max(${joinConf.timeExpression}) from left_df").collect()
    assert(minMaxRows.length > 0, s"leftDf is empty! (Inferred left partitionRange $leftPartitionRange)")
    val minMaxRow = minMaxRows(0)
    TimeRange(minMaxRow.getLong(0), minMaxRow.getLong(1))
  }

  def computeJoinPart(joinPart: JoinPart): (DataFrame, String) = {
    val groupByConf = joinPart.groupBy
    val rightScan = QueryGen(groupByConf, leftRange).query
    val rightData = session.sql(rightScan)

    // indicates that the data source is already pre-aggregated
    // directly join with the left side
    if (groupByConf.aggregations == null) return rightData -> Constants.PartitionColumn

    lazy val renamedLeft = Option(joinPart.keyRenaming).getOrElse(Map.empty).foldLeft(leftDf) {
      case (df, (leftKey, rightKey)) => df.withColumnRenamed(leftKey, rightKey)
    }

    lazy val groupBy = new GroupBy(groupByConf.aggregations, groupByConf.keys, rightData)

    (joinConf.dataModel, groupByConf.sources.map(_.dataModel).head, Option(joinPart.accuracy)) match {
      // Right entities
      case (Events, Entities, Some(Temporal)) =>
        throw new UnsupportedOperationException(s"Mutations are not yet supported")
      case (_, Entities, Some(Snapshot) | None) =>
        groupBy.snapshotEntities -> Constants.PartitionColumn

      case (Entities, _, Some(Temporal)) =>
        throw new IllegalArgumentException(s"JoinPart can't have temporal accuracy when the left side is entities")

      // right events
      case (Events, Events, Some(Temporal) | None) =>
        groupBy.temporalEvents(renamedLeft) -> Constants.TimeColumn
      case (Entities, Events, Some(Snapshot) | None) =>
        groupBy.snapshotEvents(leftPartitionRange) -> Constants.PartitionColumn

    }
  }

  val result: DataFrame = {
    joinConf.joinParts.foldLeft(leftDf) {
      case (left, joinPart) =>
        val (right, additionalKey) = computeJoinPart(joinPart)
        val renamedRight = Option(joinPart.keyRenaming)
          .getOrElse(Map.empty)
          .foldLeft(right) {
            case (rightDf, (leftKey, rightKey)) => rightDf.withColumnRenamed(rightKey, leftKey)
          }
        left.join(renamedRight, joinPart.groupBy.keys :+ additionalKey, "left_outer")
    }
  }
}
