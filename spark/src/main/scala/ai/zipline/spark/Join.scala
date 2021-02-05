package ai.zipline.spark

import ai.zipline.api.DataModel.{Entities, Events}
import ai.zipline.api.Extensions._
import ai.zipline.api.{Accuracy, Constants, JoinPart, ThriftJsonDecoder, Join => JoinConf}
import ai.zipline.spark.Extensions._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col

import scala.collection.JavaConverters._
import org.apache.spark.util.sketch.BloomFilter
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
        println(s"left skew filter: $sf")
        println(s"pre-skew filter count: ${df.count()}")
        val filtered = df.filter(sf)
        filtered
      })
      .getOrElse(df)
  }

  private val leftCount: Long = {
    val count = leftDf.count()
    println(s"left side count $count")
    count
  }

  private val leftKeys = joinConf.keys

  private val leftDfBlooms: Map[String, BloomFilter] = {
    leftKeys.map { key =>
      key -> leftDf.filter(leftDf.col(key).isNotNull).stat.bloomFilter(key, leftCount / 100, 0.1)
    }.toMap
  }

  private def joinWithLeft(leftDf: DataFrame, rightDf: DataFrame, joinPart: JoinPart): DataFrame = {
    val replacedKeys = joinPart.groupBy.keyColumns.asScala.toArray

    val additionalKeys: Seq[String] = {
      if (joinConf.left.dataModel == Entities) {
        Seq(Constants.PartitionColumn)
      } else if (accuracy(joinPart) == Accuracy.TEMPORAL) {
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
      leftDf.withTimestampBasedPartition(Constants.TimePartitionColumn) -> renamedRight.withColumnRenamed(
        Constants.PartitionColumn,
        Constants.TimePartitionColumn)
    } else {
      (leftDf, renamedRight)
    }

    val partName = joinPart.groupBy.metaData.name
    println(s"Join keys for $partName: " + keys.mkString(", "))
    println("Left Schema:")
    println(joinableLeft.schema.pretty)
    println("Left sample: ")
    println(joinableLeft.selectExpr(keys: _*).show(10))
    println(s"Right Schema for $partName: - this is the output of GroupBy")
    println(joinableRight.schema.pretty)
    println("Right sample: ")
    println(joinableRight.selectExpr(keys: _*).show(10))
    println()
    joinableLeft.validateJoinKeys(joinableRight, keys)
    val result = joinableLeft.nullSafeJoin(joinableRight, keys, "left")
//    println(s"Left join result count: ${result.count()}")
    // drop intermediate join key (used for right snapshot events case)
    result.drop(Constants.TimePartitionColumn)
  }

  private def computeJoinPart(joinPart: JoinPart, unfilledRange: PartitionRange): DataFrame = {
    val rightSkewFilter = joinConf.partSkewFilter(joinPart)
    val rightBloomMap = joinPart.rightToLeft.mapValues(leftDfBlooms)

    lazy val partitionRangeGroupBy =
      GroupBy.from(joinPart.groupBy, unfilledRange, tableUtils, rightBloomMap, rightSkewFilter)
    // no-agg case - additional key, besides the user specified key, is simply the ds
    if (joinPart.groupBy.aggregations == null) {
      return partitionRangeGroupBy.preAggregated
    }

    lazy val unfilledTimeRange = {
      val whereClauses = unfilledRange.whereClauses.mkString(" AND ")
      println(s"filtering left df using query $whereClauses")
      val timeRange = leftDf.filter(unfilledRange.whereClauses.mkString(" AND ")).timeRange
      println(s"right unfilled time range: $timeRange")
      timeRange
    }

    println(s"""
         |JoinPart Info:
         |  part name : ${joinPart.groupBy.metaData.name}, 
         |  left type : ${joinConf.left.dataModel}, 
         |  right type: ${joinPart.groupBy.dataModel}, 
         |  accuracy  : ${joinPart.accuracy},
         |  left unfilled range: $leftUnfilledRange
         |  right unfilled range: $unfilledRange
         |""".stripMargin)

    lazy val leftTimePartitionRange = unfilledTimeRange.toPartitionRange
    lazy val timeRangeGroupBy =
      GroupBy.from(joinPart.groupBy, leftTimePartitionRange, tableUtils, rightBloomMap, rightSkewFilter)
    lazy val renamedLeftDf = Option(joinPart.keyMapping)
      .map(_.asScala)
      .getOrElse(Map.empty)
      .foldLeft(leftDf) {
        case (left, (leftKey, rightKey)) => left.withColumnRenamed(leftKey, rightKey)
      }
    (joinConf.left.dataModel, joinPart.groupBy.dataModel, accuracy(joinPart)) match {
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

  private def accuracy(joinPart: JoinPart): Accuracy =
    if (joinConf.left.dataModel == Events && joinPart.accuracy == null) {
      if (joinPart.groupBy.dataModel == Events) {
        Accuracy.TEMPORAL
      } else {
        Accuracy.SNAPSHOT
      }
    } else {
      // doesn't matter for entities
      joinPart.accuracy
    }

  def computeJoin: DataFrame = {
    joinConf.joinParts.asScala.foldLeft(leftDf) {
      case (left, joinPart) =>
        val rightDf = if (joinPart.groupBy.aggregations != null) {
          val joinPartTableName = s"${outputTable}_${joinPart.groupBy.metaData.cleanName}"

          val rightUnfilledRange = tableUtils.unfilledRange(joinPartTableName, leftUnfilledRange)

          if (rightUnfilledRange.valid) {
            val rightDf = computeJoinPart(joinPart, rightUnfilledRange)
            rightDf.save(joinPartTableName, tableProps)
          }
          tableUtils.sql(leftUnfilledRange.genScanQuery(query = null, joinPartTableName))
        } else {
          // no need to generate join part cache if there are no aggregations
          computeJoinPart(joinPart, leftUnfilledRange)
        }

        joinWithLeft(left, rightDf, joinPart)
    }
  }

  val tableProps = Option(joinConf.metaData.tableProperties)
    .map(_.asScala.toMap)
    .orNull

  def commitOutput: Unit = {
    computeJoin.save(outputTable, tableProps)
    println(s"Wrote to table $outputTable, into partitions: $leftUnfilledRange")
  }
}

import org.rogach.scallop._
class ParsedArgs(args: Seq[String]) extends ScallopConf(args) {
  val confPath = opt[String](required = true)
  val endDate = opt[String](required = true)
  val namespace = opt[String](required = true)
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
    join.commitOutput
  }
}
