package ai.zipline.spark

import ai.zipline.api.DataModel.{Entities, Events}
import ai.zipline.api.Extensions._
import ai.zipline.api.{Accuracy, Constants, JoinPart, ThriftJsonDecoder, Join => JoinConf}
import ai.zipline.spark.Extensions._
import org.apache.spark.sql.DataFrame

import scala.collection.JavaConverters._

class Join(joinConf: JoinConf, endPartition: String, namespace: String, tableUtils: TableUtils) {

  private val outputTable = s"$namespace.${joinConf.metaData.cleanName}"

  private lazy val leftUnfilledRange: PartitionRange = tableUtils.unfilledRange(
    outputTable,
    PartitionRange(joinConf.left.query.startPartition, endPartition),
    Option(joinConf.left.table).toSeq)

  private val leftDf: DataFrame = {
    val df = tableUtils.sql(leftUnfilledRange.genScanQuery(joinConf.left.query, joinConf.left.table))
    val skewFilter = joinConf.skewFilter()
    skewFilter
      .map(sf => {
        println(s"mega left skew filter: $sf")
        println(s"pre-skew filter count: ${df.count()}")
        val filtered = df.filter(sf)
        filtered
      })
      .getOrElse(df)
  }

  private def joinWithLeft(leftDf: DataFrame, rightDf: DataFrame, joinPart: JoinPart): DataFrame = {
    val replacedKeys = joinPart.groupBy.keyColumns.asScala.toArray

    val additionalKeys: Seq[String] = {
      if (joinConf.left.dataModel == Entities) {
        Seq(Constants.PartitionColumn)
      } else if (inferredAccuracy(joinPart) == Accuracy.TEMPORAL) {
        Seq(Constants.TimeColumn, Constants.PartitionColumn)
      } else {
        Seq(Constants.TimePartitionColumn)
      }
    }

    // apply key-renaming to key columns
    val keyRenamedRight = Option(joinPart.keyMapping)
      .map(_.asScala)
      .getOrElse(Map.empty)
      .foldLeft(rightDf) {
        case (right, (leftKey, rightKey)) =>
          replacedKeys.update(replacedKeys.indexOf(rightKey), leftKey)
          right.withColumnRenamed(rightKey, leftKey)
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

    val keys = replacedKeys ++ additionalKeys
    val (joinableLeft, joinableRight) = if (additionalKeys.contains(Constants.TimePartitionColumn)) {
      leftDf.withTimestampBasedPartition(Constants.TimePartitionColumn) ->
        renamedRight.withColumnRenamed(Constants.PartitionColumn, Constants.TimePartitionColumn)
    } else {
      (leftDf, renamedRight)
    }

    val partName = joinPart.groupBy.metaData.name
    println(s"Join keys for $partName: " + keys.mkString(", "))
    println("Left Schema:")
    println(joinableLeft.schema.pretty)
    println(s"Right Schema for $partName: - this is the output of GroupBy")
    println(joinableRight.schema.pretty)
    joinableLeft.validateJoinKeys(joinableRight, keys)
    val result = joinableLeft.nullSafeJoin(joinableRight, keys, "left")
    result.drop(Constants.TimePartitionColumn)
  }

  private def computeJoinPart(joinPart: JoinPart, unfilledRange: PartitionRange): DataFrame = {
    val rightSkewFilter = joinConf.partSkewFilter(joinPart)
    val leftPartitionPruneFilter = unfilledRange.whereClauses.mkString(" AND ")
    println(s"Pruning using $leftPartitionPruneFilter")
    val leftPartitionPruned = leftDf.filter(leftPartitionPruneFilter)
    val leftPrunedCount = leftPartitionPruned.count()
    println(s"Pruned count $leftPrunedCount")
    val rightBloomMap = joinPart.rightToLeft
      .mapValues(
        leftPartitionPruned.generateBloomFilter(_, Math.max(leftPrunedCount / 100, 100))
      )
    val bloomSizes = rightBloomMap.map { case (col, bloom) => s"$col -> ${bloom.bitSize()}" }.pretty

    println(s"""
         |JoinPart Info:
         |  part name : ${joinPart.groupBy.metaData.name}, 
         |  left type : ${joinConf.left.dataModel}, 
         |  right type: ${joinPart.groupBy.dataModel}, 
         |  accuracy  : ${joinPart.groupBy.accuracy},
         |  left unfilled range: $leftUnfilledRange
         |  right/part unfilled range: $unfilledRange
         |  bloom sizes: $bloomSizes
         |""".stripMargin)

    // all lazy vals - so evaluated only when needed by each case.
    lazy val partitionRangeGroupBy =
      GroupBy.from(joinPart.groupBy, unfilledRange, tableUtils, rightBloomMap, rightSkewFilter)

    lazy val unfilledTimeRange = {
      val timeRange = leftPartitionPruned.timeRange
      println(s"right unfilled time range: $timeRange")
      timeRange
    }
    lazy val leftTimePartitionRange = unfilledTimeRange.toPartitionRange
    lazy val timeRangeGroupBy =
      GroupBy.from(joinPart.groupBy, leftTimePartitionRange, tableUtils, rightBloomMap, rightSkewFilter)

    val leftSkewFilter = joinConf.skewFilter(Some(joinPart.rightToLeft.values.toSeq))
    // this is the second time we apply skew filter - but this filters only on the keys
    // relevant for this join part.
    lazy val skewFilteredLeft = leftSkewFilter
      .map { sf =>
        val filtered = leftDf.filter(sf)
        println(s"""Skew filtering left-df for
           |GroupBy: ${joinPart.groupBy.metaData.name}
           |filterClause: $sf
           |filtered-count: ${filtered.count()}
           |""".stripMargin)
        filtered
      }
      .getOrElse(leftDf)

    lazy val renamedLeftDf = Option(joinPart.keyMapping)
      .map(_.asScala)
      .getOrElse(Map.empty)
      .foldLeft(skewFilteredLeft) {
        case (left, (leftKey, rightKey)) => left.withColumnRenamed(leftKey, rightKey)
      }

    (joinConf.left.dataModel, joinPart.groupBy.dataModel, inferredAccuracy(joinPart)) match {
      case (Entities, Events, _)               => partitionRangeGroupBy.snapshotEvents(unfilledRange)
      case (Entities, Entities, _)             => partitionRangeGroupBy.snapshotEntities
      case (Events, Events, Accuracy.SNAPSHOT) => timeRangeGroupBy.snapshotEvents(leftTimePartitionRange)
      case (Events, Events, Accuracy.TEMPORAL) =>
        timeRangeGroupBy.temporalEvents(renamedLeftDf, Some(unfilledTimeRange))

      case (Events, Entities, Accuracy.SNAPSHOT) =>
        val PartitionRange(start, end) = leftTimePartitionRange
        val rightRange = PartitionRange(Constants.Partition.before(start), Constants.Partition.before(end))
        GroupBy.from(joinPart.groupBy, rightRange, tableUtils, rightBloomMap).snapshotEntities

      case (Events, Entities, Accuracy.TEMPORAL) =>
        throw new UnsupportedOperationException("Mutations are not yet supported")
    }
  }

  private def inferredAccuracy(joinPart: JoinPart): Accuracy =
    if (joinConf.left.dataModel == Events && joinPart.groupBy.accuracy == null) {
      if (joinPart.groupBy.dataModel == Events) {
        Accuracy.TEMPORAL
      } else {
        Accuracy.SNAPSHOT
      }
    } else {
      // doesn't matter for entities
      joinPart.groupBy.accuracy
    }

  def computeRange(leftRange: PartitionRange): DataFrame = {
    joinConf.joinParts.asScala.foldLeft(leftDf) {
      case (left, joinPart) =>
        val rightDf: DataFrame = if (joinPart.groupBy.aggregations != null) {
          // compute only the missing piece
          val joinPartTableName = s"${outputTable}_${joinPart.groupBy.metaData.cleanName}"
          val rightUnfilledRange = tableUtils.unfilledRange(joinPartTableName, leftRange)

          if (rightUnfilledRange.valid) {
            val rightDf = computeJoinPart(joinPart, rightUnfilledRange)
            // cache the join-part output into table partitions
            rightDf.save(joinPartTableName, tableProps)
          }

          tableUtils.sql(leftRange.genScanQuery(query = null, joinPartTableName))
        } else {
          // no need to generate join part cache if there are no aggregations
          computeJoinPart(joinPart, leftRange)
        }

        joinWithLeft(left, rightDf, joinPart)
    }
  }

  val tableProps = Option(joinConf.metaData.tableProperties)
    .map(_.asScala.toMap)
    .orNull

  def computeJoin(stepDays: Option[Int] = None): DataFrame = {
    val stepRanges = stepDays.map(leftUnfilledRange.steps).getOrElse(Seq(leftUnfilledRange))
    println(s"Join ranges to compute: ${stepRanges.map { _.toString }.pretty}")
    stepRanges.zipWithIndex.foreach {
      case (range, index) =>
        val progress = s"| [${index + 1}/${stepRanges.size}]"
        println(s"Computing join for range: $range  $progress")
        computeRange(range).save(outputTable, tableProps)
        println(s"Wrote to table $outputTable, into partitions: $range $progress")
    }
    println(s"Wrote to table $outputTable, into partitions: $leftUnfilledRange")
    tableUtils.sql(leftUnfilledRange.genScanQuery(null, outputTable))
  }
}

import org.rogach.scallop._
class ParsedArgs(args: Seq[String]) extends ScallopConf(args) {
  val confPath = opt[String](required = true)
  val endDate = opt[String](required = true)
  val namespace = opt[String](required = true)
  val stepDays = opt[Int](required = false)
  verify()
}

object Join {

  // TODO: make joins a subcommand of a larger driver that does multiple other things
  def main(args: Array[String]): Unit = {
    // args = conf path, end date, output namespace
    val parsedArgs = new ParsedArgs(args)
    println(s"Parsed Args: $parsedArgs")
    val joinConf =
      ThriftJsonDecoder.fromJsonFile[JoinConf](parsedArgs.confPath(), check = true, clazz = classOf[JoinConf])

    val join = new Join(
      joinConf,
      parsedArgs.endDate(),
      parsedArgs.namespace(),
      TableUtils(SparkSessionBuilder.build(s"join_${joinConf.metaData.name}", local = false))
    )
    join.computeJoin(parsedArgs.stepDays.toOption)
  }
}
