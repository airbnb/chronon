package ai.zipline.spark

import ai.zipline.api.DataModel.{Entities, Events}
import ai.zipline.api.Extensions._
import ai.zipline.api.{Accuracy, Constants, JoinPart, ThriftJsonCodec, Join => JoinConf}
import ai.zipline.spark.Extensions._
import org.apache.spark.sql.DataFrame

import java.util.Base64
import scala.collection.JavaConverters._

class Join(joinConf: JoinConf, endPartition: String, tableUtils: TableUtils) {
  assert(Option(joinConf.metaData.outputNamespace).nonEmpty, s"output namespace could not be empty or null")
  private val outputTable = s"${joinConf.metaData.outputNamespace}.${joinConf.metaData.cleanName}"

  // Get table properties from config
  private val confTableProps = Option(joinConf.metaData.tableProperties)
    .map(_.asScala.toMap)
    .getOrElse(Map.empty[String, String])

  // Serialize the join object json to put on tableProperties (used to detect semantic changes from last run)
  private val confJson = ThriftJsonCodec.serializer.toString(joinConf)
  private val confJsonBase64 = Base64.getEncoder.encodeToString(confJson.getBytes("UTF-8"))

  // Combine tableProperties set on conf with encoded Join
  private val tableProps = confTableProps ++ Map(Constants.JoinMetadataKey -> confJsonBase64)

  private def joinWithLeft(leftDf: DataFrame, rightDf: DataFrame, joinPart: JoinPart): DataFrame = {
    val partLeftKeys = joinPart.rightToLeft.values.toArray

    // besides the ones specified in the group-by
    val additionalKeys: Seq[String] = {
      if (joinConf.left.dataModel == Entities) {
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
    // team name is assigned at the materialize step
    val team =
      if (joinConf.metaData.team.equals(joinPart.groupBy.metaData.team)) None else Some(joinPart.groupBy.metaData.team)
    val fullPrefix = (Option(joinPart.prefix) ++ team ++ Some(joinPart.groupBy.metaData.cleanName)).mkString("_")
    val prefixedRight = keyRenamedRight.prefixColumnNames(fullPrefix, valueColumns)

    // compute join keys, besides the groupBy keys -  like ds, ts etc.,
    val keys = partLeftKeys ++ additionalKeys

    val partName = joinPart.groupBy.metaData.name
    println(s"Join keys for $partName: " + keys.mkString(", "))
    println("Left Schema:")
    println(leftDf.schema.pretty)
    println(s"Right Schema for $partName: - this is the output of GroupBy")
    println(prefixedRight.schema.pretty)

    val joinableRight = if (additionalKeys.contains(Constants.TimePartitionColumn)) {
      prefixedRight.withColumnRenamed(Constants.PartitionColumn, Constants.TimePartitionColumn)
    } else {
      prefixedRight
    }

    leftDf.validateJoinKeys(joinableRight, keys)
    leftDf.join(joinableRight, keys, "left")
  }

  private def computeJoinPart(leftDf: DataFrame, joinPart: JoinPart, unfilledRange: PartitionRange): DataFrame = {
    val rightSkewFilter = joinConf.partSkewFilter(joinPart)
    val leftPrunedCount = leftDf.count()
    println(s"Pruned count $leftPrunedCount")
    // technically 1 billion is 512MB - but java overhead is crazy and we need to cutoff - to 100M
    val bloomSize = Math.min(100000000, Math.max(leftPrunedCount / 10, 100))
    println(s"Bloom size: $bloomSize")
    val rightBloomMap = joinPart.rightToLeft
      .mapValues(
        leftDf.generateBloomFilter(_, bloomSize)
      )
    val bloomSizes = rightBloomMap.map { case (col, bloom) => s"$col -> ${bloom.bitSize()}" }.pretty

    println(s"""
         |JoinPart Info:
         |  part name : ${joinPart.groupBy.metaData.name}, 
         |  left type : ${joinConf.left.dataModel}, 
         |  right type: ${joinPart.groupBy.dataModel}, 
         |  accuracy  : ${joinPart.groupBy.accuracy},
         |  part unfilled range: $unfilledRange,
         |  bloom sizes: $bloomSizes
         |""".stripMargin)

    // all lazy vals - so evaluated only when needed by each case.
    lazy val partitionRangeGroupBy =
      GroupBy.from(joinPart.groupBy, unfilledRange, tableUtils, Option(rightBloomMap), rightSkewFilter)

    lazy val unfilledTimeRange = {
      val timeRange = leftDf.timeRange
      println(s"right unfilled time range: $timeRange")
      timeRange
    }
    lazy val leftTimePartitionRange = unfilledTimeRange.toPartitionRange
    lazy val timeRangeGroupBy =
      GroupBy.from(joinPart.groupBy, leftTimePartitionRange, tableUtils, Option(rightBloomMap), rightSkewFilter)

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

    (joinConf.left.dataModel, joinPart.groupBy.dataModel, joinPart.groupBy.inferredAccuracy) match {
      case (Entities, Events, _)               => partitionRangeGroupBy.snapshotEvents(unfilledRange)
      case (Entities, Entities, _)             => partitionRangeGroupBy.snapshotEntities
      case (Events, Events, Accuracy.SNAPSHOT) => timeRangeGroupBy.snapshotEvents(leftTimePartitionRange)
      case (Events, Events, Accuracy.TEMPORAL) =>
        timeRangeGroupBy.temporalEvents(renamedLeftDf, Some(unfilledTimeRange))

      case (Events, Entities, Accuracy.SNAPSHOT) =>
        val PartitionRange(start, end) = leftTimePartitionRange
        val rightRange = PartitionRange(Constants.Partition.before(start), Constants.Partition.before(end))
        GroupBy.from(joinPart.groupBy, rightRange, tableUtils, Option(rightBloomMap)).snapshotEntities

      case (Events, Entities, Accuracy.TEMPORAL) =>
        throw new UnsupportedOperationException("Mutations are not yet supported")
    }
  }

  def getJoinPartTableName(joinPart: JoinPart): String = {
    val joinPartPrefix = Option(joinPart.prefix).map(prefix => s"_$prefix").getOrElse("")
    s"${outputTable}_$joinPartPrefix${joinPart.groupBy.metaData.cleanName}"
  }

  def computeRange(leftDf: DataFrame, leftRange: PartitionRange): DataFrame = {
    val leftTaggedDf = if (leftDf.schema.names.contains(Constants.TimeColumn)) {
      leftDf.withTimestampBasedPartition(Constants.TimePartitionColumn)
    } else {
      leftDf
    }

    val joined = joinConf.joinParts.asScala.foldLeft(leftTaggedDf) {
      case (partialDf, joinPart) =>
        val rightDf: DataFrame = if (joinPart.groupBy.aggregations != null) {
          // compute only the missing piece
          val joinPartTableName = getJoinPartTableName(joinPart)
          val rightUnfilledRange = tableUtils.unfilledRange(joinPartTableName, leftRange)
          println(s"Right unfilled range for $joinPartTableName is $rightUnfilledRange with leftRange of $leftRange")

          if (rightUnfilledRange.valid) {
            val rightDf = computeJoinPart(partialDf, joinPart, rightUnfilledRange)
            // cache the join-part output into table partitions
            rightDf.save(joinPartTableName, tableProps)
          }

          tableUtils.sql(leftRange.genScanQuery(query = null, joinPartTableName))
        } else {
          // no need to generate join part cache if there are no aggregations
          computeJoinPart(partialDf, joinPart, leftRange)
        }
        joinWithLeft(partialDf, rightDf, joinPart)
    }

    joined.explain()
    joined.drop(Constants.TimePartitionColumn)
  }

  def getLastRunJoinOpt: Option[JoinConf] = {
    tableUtils.getTableProperties(outputTable).map { lastRunMetadata =>
      // get the join object that was saved onto the table as part of the last run
      val encodedMetadata = lastRunMetadata.get(Constants.JoinMetadataKey).get
      val joinJsonBytes = Base64.getDecoder.decode(encodedMetadata)
      val joinJsonString = new String(joinJsonBytes)
      ThriftJsonCodec.fromJsonStr(joinJsonString, true, classOf[JoinConf])
    }
  }

  def joinPartsWereRemoved(lastRunJoin: Option[JoinConf]): Boolean = {
    // This check is to handle the edge case where a join part was removed without any other changes
    // to the join definition (and so the final table needs to be dropped for schema migration).
    lastRunJoin.exists { lastRunJoin =>
      lastRunJoin.joinParts.asScala.exists { joinPart =>
        !joinConf.joinParts.asScala.exists(_.copyForVersioningComparison == joinPart.copyForVersioningComparison)
      }
    }
  }

  def getJoinPartsToRecompute(lastRunJoin: Option[JoinConf]): Seq[JoinPart] = {
    lastRunJoin
      .map { lastRunJoin =>
        if (joinConf.copyForVersioningComparison != lastRunJoin.copyForVersioningComparison) {
          println("Changes detected on left side of join, recomputing all joinParts")
          joinConf.joinParts.asScala ++ lastRunJoin.joinParts.asScala
        } else {
          println("No changes detected on left side of join, comparing individual JoinParts for equality")
          joinConf.joinParts.asScala.filter { joinPart =>
            !lastRunJoin.joinParts.asScala.exists(_.copyForVersioningComparison == joinPart.copyForVersioningComparison)
          }
        }
      }
      .getOrElse {
        println("No Metadata found on existing table, attempting to proceed without recomputation.")
        Seq.empty
      }
  }

  def dropTablesToRecompute(): Unit = {
    // Detects semantic changes since last run in Join or GroupBy tables and drops the relevant tables so that they may be recomputed
    val lastRunjoin = getLastRunJoinOpt
    val joinPartsToRecompute = getJoinPartsToRecompute(lastRunjoin)
    joinPartsToRecompute.foreach { joinPart =>
      tableUtils.dropTableIfExists(getJoinPartTableName(joinPart))
    }
    if (joinPartsToRecompute.nonEmpty || joinPartsWereRemoved(lastRunjoin)) {
      // If anything changed, then we also need to recompute the join to the final table
      // This could be made more efficient with a "tetris" style backfill, only joining in the columns that had sematic chages
      // But this is left as a future improvement as the efficiency gain is only relevant for very-wide joins
      tableUtils.dropTableIfExists(outputTable)
    }
  }

  def computeJoin(stepDays: Option[Int] = None): DataFrame = {
    assert(Option(joinConf.metaData.team).nonEmpty,
           s"join.metaData.team needs to be set for join ${joinConf.metaData.name}")

    joinConf.joinParts.asScala.foreach { jp =>
      assert(Option(jp.groupBy.metaData.team).nonEmpty,
             s"groupBy.metaData.team needs to be set for joinPart ${jp.groupBy.metaData.name}")
    }

    // First run command to drop tables that have changed semantically since the last run
    dropTablesToRecompute

    joinConf.setups.foreach(tableUtils.sql)
    val leftUnfilledRange: PartitionRange = tableUtils.unfilledRange(
      outputTable,
      PartitionRange(joinConf.left.query.startPartition, endPartition),
      Option(joinConf.left.table).toSeq)

    println(s"left unfilled range: $leftUnfilledRange")
    val leftDfFull: DataFrame = {
      val timeProjection = if (joinConf.left.dataModel == Events) {
        Seq(Constants.TimeColumn -> Option(joinConf.left.query).map(_.timeColumn).orNull)
      } else {
        Seq()
      }
      val df = tableUtils.sql(
        leftUnfilledRange.genScanQuery(joinConf.left.query,
                                       joinConf.left.table,
                                       fillIfAbsent = Map(Constants.PartitionColumn -> null) ++ timeProjection))
      val skewFilter = joinConf.skewFilter()
      skewFilter
        .map(sf => {
          println(s"left skew filter: $sf")
          println(s"pre-skew-filter left count: ${df.count()}")
          val filtered = df.filter(sf)
          println(s"post-skew-filter left count: ${filtered.count()}")
          filtered
        })
        .getOrElse(df)
    }

    val stepRanges = stepDays.map(leftUnfilledRange.steps).getOrElse(Seq(leftUnfilledRange))
    println(s"Join ranges to compute: ${stepRanges.map { _.toString }.pretty}")
    stepRanges.zipWithIndex.foreach {
      case (range, index) =>
        val progress = s"| [${index + 1}/${stepRanges.size}]"
        println(s"Computing join for range: $range  $progress")
        computeRange(leftDfFull.prunePartition(range), range).save(outputTable, tableProps)
        println(s"Wrote to table $outputTable, into partitions: $range $progress")
    }
    println(s"Wrote to table $outputTable, into partitions: $leftUnfilledRange")
    tableUtils.sql(leftUnfilledRange.genScanQuery(null, outputTable))
  }
}

// the driver
object Join {
  import org.rogach.scallop._
  class ParsedArgs(args: Seq[String]) extends ScallopConf(args) {
    val confPath: ScallopOption[String] = opt[String](required = true)
    val endDate: ScallopOption[String] = opt[String](required = true)
    val stepDays: ScallopOption[Int] = opt[Int](required = false)
    verify()
  }

  // TODO: make joins a subcommand of a larger driver
  //  that does group-by backfills/bulk uploads etc
  def main(args: Array[String]): Unit = {
    // args = conf path, end date
    val parsedArgs = new ParsedArgs(args)
    println(s"Parsed Args: $parsedArgs")
    val joinConf =
      ThriftJsonCodec.fromJsonFile[JoinConf](parsedArgs.confPath(), check = true, clazz = classOf[JoinConf])

    val join = new Join(
      joinConf,
      parsedArgs.endDate(),
      TableUtils(SparkSessionBuilder.build(s"join_${joinConf.metaData.name}", local = false))
    )

    join.computeJoin(parsedArgs.stepDays.toOption)
  }
}
