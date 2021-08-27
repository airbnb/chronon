package ai.zipline.spark

import ai.zipline.api.DataModel.{Entities, Events}
import ai.zipline.api.Extensions._
import ai.zipline.api.{Accuracy, Constants, JoinPart, ThriftJsonCodec, Join => JoinConf}
import ai.zipline.spark.Extensions._
import org.apache.spark.sql.DataFrame

import java.util.Base64
import scala.collection.JavaConverters._

class Join(joinConf: JoinConf, endPartition: String, tableUtils: TableUtils, skipEqualCheck: Boolean = false) {
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

    println(s"""Join keys for $partName: ${keys.mkString(", ")}
         |Left Schema:
         |  ${leftDf.schema.pretty}
         |  
         |Right Schema:
         |  ${prefixedRight.schema.pretty}
         |  
         |""".stripMargin)

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
         |  accuracy  : ${joinPart.groupBy.inferredAccuracy},
         |  part unfilled range: $unfilledRange,
         |  bloom sizes: $bloomSizes
         |  groupBy: ${joinPart.groupBy.toString}
         |""".stripMargin)

    // all lazy vals - so evaluated only when needed by each case.
    lazy val partitionRangeGroupBy =
      GroupBy.from(joinPart.groupBy, unfilledRange, tableUtils, Option(rightBloomMap), rightSkewFilter)

    lazy val unfilledTimeRange = {
      val timeRange = leftDf.timeRange
      println(s"left unfilled time range: $timeRange")
      timeRange
    }

    def genGroupBy(partitionRange: PartitionRange) =
      GroupBy.from(joinPart.groupBy, partitionRange, tableUtils, Option(rightBloomMap), rightSkewFilter)

    val leftSkewFilter = joinConf.skewFilter(Some(joinPart.rightToLeft.values.toSeq))
    // this is the second time we apply skew filter - but this filters only on the keys
    // relevant for this join part.
    lazy val skewFilteredLeft = leftSkewFilter
      .map { sf =>
        val filtered = leftDf.filter(sf)
        println(s"""Skew filtering left-df for
           |GroupBy: ${joinPart.groupBy.metaData.name}
           |filterClause: $sf
           |""".stripMargin)
        filtered
      }
      .getOrElse(leftDf)

    lazy val renamedLeftDf = Option(joinPart.keyMapping)
      .map(_.asScala)
      .getOrElse(Map.empty)
      .foldLeft(skewFilteredLeft) {
        case (left, (leftKey, rightKey)) =>
          val result = if (left.schema.fieldNames.contains(rightKey)) left.drop(rightKey) else left
          result.withColumnRenamed(leftKey, rightKey)
      }

    lazy val shiftedPartitionRange = unfilledTimeRange.toPartitionRange.shift(-1)
    (joinConf.left.dataModel, joinPart.groupBy.dataModel, joinPart.groupBy.inferredAccuracy) match {
      case (Entities, Events, _)   => partitionRangeGroupBy.snapshotEvents(unfilledRange)
      case (Entities, Entities, _) => partitionRangeGroupBy.snapshotEntities
      case (Events, Events, Accuracy.SNAPSHOT) =>
        genGroupBy(shiftedPartitionRange).snapshotEvents(shiftedPartitionRange)
      case (Events, Events, Accuracy.TEMPORAL) =>
        genGroupBy(unfilledTimeRange.toPartitionRange).temporalEvents(renamedLeftDf, Some(unfilledTimeRange))

      case (Events, Entities, Accuracy.SNAPSHOT) => genGroupBy(shiftedPartitionRange).snapshotEntities

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

    // compute joinParts in parallel
    val rightDfs = joinConf.joinParts.asScala.par.map { joinPart =>
      if (joinPart.groupBy.aggregations == null) {
        // no need to generate join part cache if there are no aggregations
        computeJoinPart(leftTaggedDf, joinPart, leftRange)
      } else {

        // compute only the missing piece
        val joinPartTableName = getJoinPartTableName(joinPart)
        val rightUnfilledRange = tableUtils.unfilledRange(joinPartTableName, leftRange)
        println(s"Right unfilled range for $joinPartTableName is $rightUnfilledRange with leftRange of $leftRange")

        if (rightUnfilledRange.valid) {
          try {
            val rightDf = computeJoinPart(leftTaggedDf, joinPart, rightUnfilledRange)
            // cache the join-part output into table partitions
            rightDf.save(joinPartTableName, tableProps)
          } catch {
            case e: Exception =>
              println(s"Error while processing groupBy: ${joinPart.groupBy.getMetaData.getName}")
              throw e
          }
        }
        val scanRange =
          if (joinConf.left.dataModel == Events && joinPart.groupBy.inferredAccuracy == Accuracy.SNAPSHOT) {
            // the events of a ds will join with
            leftDf.timeRange.toPartitionRange.shift(-1)
          } else {
            leftRange
          }
        tableUtils.sql(scanRange.genScanQuery(query = null, joinPartTableName))
      }
    }

    val joined = rightDfs.zip(joinConf.joinParts.asScala).foldLeft(leftTaggedDf) {
      case (partialDf, (rightDf, joinPart)) => joinWithLeft(partialDf, rightDf, joinPart)
    }

    joined.explain()
    joined.drop(Constants.TimePartitionColumn)
  }

  def getLastRunJoinOpt: Option[JoinConf] = {
    tableUtils.getTableProperties(outputTable).map { lastRunMetadata =>
      // get the join object that was saved onto the table as part of the last run
      val encodedMetadata = lastRunMetadata(Constants.JoinMetadataKey)
      val joinJsonBytes = Base64.getDecoder.decode(encodedMetadata)
      val joinJsonString = new String(joinJsonBytes)
      ThriftJsonCodec.fromJsonStr(joinJsonString, check = !skipEqualCheck, classOf[JoinConf])
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

  def getJoinUnavailableRange: Option[PartitionRange] = {
    val leftPartitions = tableUtils
      .partitions(joinConf.left.table)
      .filter(ds => ds >= joinConf.left.query.startPartition && ds <= endPartition)
      .toSet
    val outputPartitions = tableUtils.partitions(outputTable).toSet
    (leftPartitions -- outputPartitions)
      .reduceLeftOption(Ordering[String].min)
      .map(PartitionRange(_, endPartition))
  }

  def computeJoin(stepDays: Option[Int] = None): DataFrame = {
    assert(Option(joinConf.metaData.team).nonEmpty,
           s"join.metaData.team needs to be set for join ${joinConf.metaData.name}")

    joinConf.joinParts.asScala.foreach { jp =>
      assert(Option(jp.groupBy.metaData.team).nonEmpty,
             s"groupBy.metaData.team needs to be set for joinPart ${jp.groupBy.metaData.name}")
    }

    // First run command to drop tables that have changed semantically since the last run
    dropTablesToRecompute()

    joinConf.setups.foreach(tableUtils.sql)

    val leftUnfilledRangeOpt = getJoinUnavailableRange
    if (leftUnfilledRangeOpt.isEmpty || !leftUnfilledRangeOpt.get.valid) {
      println(s"There is no data to compute based on the left unfilled range $leftUnfilledRangeOpt")
      System.exit(0)
    }
    val leftUnfilledRange = leftUnfilledRangeOpt.get
    println(s"Dropping left unfilled range $leftUnfilledRange from join output $outputTable")
    tableUtils.dropPartitionRange(outputTable, leftUnfilledRange.start, leftUnfilledRange.end)
    joinConf.joinParts.asScala.foreach { joinPart =>
      val partTable = getJoinPartTableName(joinPart)
      println(s"Dropping left unfilled range $leftUnfilledRange from join part table $partTable")
      tableUtils.dropPartitionRange(partTable, leftUnfilledRange.start, leftUnfilledRange.end)
    }
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
          df.filter(sf)
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
  // TODO: make joins a subcommand of a larger driver
  //  that does group-by backfills/bulk uploads etc
  def main(args: Array[String]): Unit = {
    // args = conf path, end date
    val parsedArgs = new Args(args)
    parsedArgs.verify()
    println(s"Parsed Args: $parsedArgs")
    val joinConf = parsedArgs.parseConf[JoinConf]
    val join = new Join(
      joinConf,
      parsedArgs.endDate(),
      TableUtils(SparkSessionBuilder.build(s"join_${joinConf.metaData.name}")),
      parsedArgs.skipEqualCheck()
    )

    join.computeJoin(parsedArgs.stepDays.toOption)
  }
}
