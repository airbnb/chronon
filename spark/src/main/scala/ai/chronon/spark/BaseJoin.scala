package ai.chronon.spark

import ai.chronon.api
import ai.chronon.api.DataModel.{Entities, Events}
import ai.chronon.api.Extensions._
import ai.chronon.api._
import ai.chronon.online.Metrics
import ai.chronon.spark.Extensions._
import ai.chronon.spark.JoinUtils.{leftDf, mergeDFs}
import com.google.gson.Gson
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

import java.time.Instant
import scala.collection.JavaConverters._
import scala.util.Try

abstract class BaseJoin(joinConf: api.Join, endPartition: String, tableUtils: TableUtils) {
  assert(Option(joinConf.metaData.outputNamespace).nonEmpty, s"output namespace could not be empty or null")
  val metrics = Metrics.Context(Metrics.Environment.JoinOffline, joinConf)
  private val outputTable = joinConf.metaData.outputTable

  // Get table properties from config
  protected val confTableProps = Option(joinConf.metaData.tableProperties)
    .map(_.asScala.toMap)
    .getOrElse(Map.empty[String, String])

  private val gson = new Gson()
  // Combine tableProperties set on conf with encoded Join
  protected val tableProps = confTableProps ++ Map(Constants.SemanticHashKey -> gson.toJson(joinConf.semanticHash.asJava))

  def joinWithLeft(leftDf: DataFrame, rightDf: DataFrame, joinPart: JoinPart): DataFrame = {
    val partLeftKeys = joinPart.rightToLeft.values.toArray

    // compute join keys, besides the groupBy keys -  like ds, ts etc.,
    val additionalKeys: Seq[String] = {
      if (joinConf.left.dataModel == Entities) {
        Seq(Constants.PartitionColumn)
      } else if (joinPart.groupBy.inferredAccuracy == Accuracy.TEMPORAL) {
        Seq(Constants.TimeColumn, Constants.PartitionColumn)
      } else { // left-events + snapshot => join-key = ds_of_left_ts
        Seq(Constants.TimePartitionColumn)
      }
    }
    val keys = partLeftKeys ++ additionalKeys

    // apply key-renaming to key columns
    val keyRenamedRightDf = joinPart.rightToLeft.foldLeft(rightDf) {
      case (rightDf, (rightKey, leftKey)) => rightDf.withColumnRenamed(rightKey, leftKey)
    }

    // apply prefix to value columns
    val nonValueColumns = joinPart.rightToLeft.keys.toArray ++ Array(Constants.TimeColumn,
                                                                     Constants.PartitionColumn,
                                                                     Constants.TimePartitionColumn)
    val valueColumns = rightDf.schema.names.filterNot(nonValueColumns.contains)
    val prefixedRightDf = keyRenamedRightDf.prefixColumnNames(joinPart.fullPrefix, valueColumns)

    // adjust join keys
    val joinableRightDf = if (additionalKeys.contains(Constants.TimePartitionColumn)) {
      // increment one day to align with left side ts_ds
      // because one day was decremented from the partition range for snapshot accuracy
      prefixedRightDf
        .withColumn(Constants.TimePartitionColumn,
                    date_format(date_add(col(Constants.PartitionColumn), 1), Constants.Partition.format))
        .drop(Constants.PartitionColumn)
    } else {
      prefixedRightDf
    }

    val joinedDf = mergeDFs(leftDf, joinableRightDf, keys)
    println(s"""
               |Join keys for ${joinPart.groupBy.metaData.name}: ${keys.mkString(", ")}
               |Left Schema:
               |${leftDf.schema.pretty}
               |Right Schema:
               |${prefixedRightDf.schema.pretty}
               |Final Schema:
               |${joinedDf.schema.pretty}
               |""".stripMargin)

    joinedDf
  }

  def computeRightTable(leftDf: DataFrame, joinPart: JoinPart, leftRange: PartitionRange): Option[DataFrame] = {
    val partMetrics = Metrics.Context(metrics, joinPart)
    if (joinPart.groupBy.aggregations == null) {
      computeJoinPart(leftDf, joinPart)
    } else {
      val partTable = joinConf.partOutputTable(joinPart)
      try {
        val unfilledRanges =
          tableUtils.unfilledRanges(partTable, leftRange, Some(Seq(joinConf.left.table))).getOrElse(Seq())
        val partitionCount = unfilledRanges.map(_.partitions.length).sum
        if (partitionCount > 0) {
          val start = System.currentTimeMillis()
          unfilledRanges
            .foreach(unfilledRange => {
              val filledDf = computeJoinPart(leftDf.prunePartition(unfilledRange), joinPart)
              // cache the join-part output into table partitions
              if (filledDf.isDefined) {
                println(s"Writing to join part table: $partTable for partition range $unfilledRange")
                filledDf.get.save(partTable, tableProps)
              }
            })
          val elapsedMins = (System.currentTimeMillis() - start) / 60000
          partMetrics.gauge(Metrics.Name.LatencyMinutes, elapsedMins)
          partMetrics.gauge(Metrics.Name.PartitionCount, partitionCount)
          println(s"Wrote ${partitionCount} partitions to join part table: $partTable in $elapsedMins minutes")
        }
      } catch {
        case e: Exception =>
          println(s"Error while processing groupBy: ${joinConf.metaData.name}/${joinPart.groupBy.getMetaData.getName}")
          throw e
      }
      val scanRange =
        if (joinConf.left.dataModel == Events && joinPart.groupBy.inferredAccuracy == Accuracy.SNAPSHOT) {
          // the events of a ds will join with
          leftRange.shift(-1)
        } else {
          leftRange
        }
      if (tableUtils.tableExists(partTable)) {
        Some(tableUtils.sql(scanRange.genScanQuery(query = null, partTable)))
      } else {
        None
      }
    }
  }

  def computeJoinPart(leftDf: DataFrame, joinPart: JoinPart): Option[DataFrame] = {

    val stats = leftDf
      .select(
        count(lit(1)),
        min(Constants.PartitionColumn),
        max(Constants.PartitionColumn)
      )
      .head()
    val rowCount = stats.getLong(0)

    val unfilledRange = PartitionRange(stats.getString(1), stats.getString(2))
    if (rowCount == 0) {
      // happens when all rows are already filled by bootstrap tables
      println(s"leftDf empty for joinPart ${joinPart.groupBy.metaData.name} and unfilled range ${unfilledRange}")
      return None
    }

    val leftBlooms = joinConf.leftKeyCols.par.map { key =>
      key -> leftDf.generateBloomFilter(key, rowCount, joinConf.left.table, unfilledRange)
    }.toMap

    val rightSkewFilter = joinConf.partSkewFilter(joinPart)
    val rightBloomMap = joinPart.rightToLeft.mapValues(leftBlooms(_))
    val bloomSizes = rightBloomMap.map { case (col, bloom) => s"$col -> ${bloom.bitSize()}" }.pretty
    println(s"""
         |JoinPart Info:
         |  part name : ${joinPart.groupBy.metaData.name},
         |  left type : ${joinConf.left.dataModel},
         |  right type: ${joinPart.groupBy.dataModel},
         |  accuracy  : ${joinPart.groupBy.inferredAccuracy},
         |  part unfilled range: $unfilledRange,
         |  left row count: $rowCount
         |  bloom sizes: $bloomSizes
         |  groupBy: ${joinPart.groupBy.toString}
         |""".stripMargin)

    def genGroupBy(partitionRange: PartitionRange) =
      GroupBy.from(joinPart.groupBy, partitionRange, tableUtils, Option(rightBloomMap), rightSkewFilter)

    // all lazy vals - so evaluated only when needed by each case.
    lazy val partitionRangeGroupBy = genGroupBy(unfilledRange)

    lazy val unfilledTimeRange = {
      val timeRange = leftDf.timeRange
      println(s"left unfilled time range: $timeRange")
      timeRange
    }

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
    val rightDf = (joinConf.left.dataModel, joinPart.groupBy.dataModel, joinPart.groupBy.inferredAccuracy) match {
      case (Entities, Events, _)   => partitionRangeGroupBy.snapshotEvents(unfilledRange)
      case (Entities, Entities, _) => partitionRangeGroupBy.snapshotEntities
      case (Events, Events, Accuracy.SNAPSHOT) =>
        genGroupBy(shiftedPartitionRange).snapshotEvents(shiftedPartitionRange)
      case (Events, Events, Accuracy.TEMPORAL) =>
        genGroupBy(unfilledTimeRange.toPartitionRange).temporalEvents(renamedLeftDf, Some(unfilledTimeRange))

      case (Events, Entities, Accuracy.SNAPSHOT) => genGroupBy(shiftedPartitionRange).snapshotEntities

      case (Events, Entities, Accuracy.TEMPORAL) => {
        // Snapshots and mutations are partitioned with ds holding data between <ds 00:00> and ds <23:53>.
        genGroupBy(shiftedPartitionRange).temporalEntities(renamedLeftDf)
      }
    }
    Some(rightDf)
  }

  def tablesToRecompute(): Seq[String] = {
    (for (
      props <- tableUtils.getTableProperties(outputTable);
      oldSemanticJson <- props.get(Constants.SemanticHashKey);
      oldSemanticHash = gson.fromJson(oldSemanticJson, classOf[java.util.HashMap[String, String]]).asScala.toMap
    ) yield {
      println(s"Comparing Hashes:\nNew: ${joinConf.semanticHash},\nOld: $oldSemanticHash");
      joinConf.tablesToDrop(oldSemanticHash)
    }).getOrElse(Seq.empty)
  }

  def computeRange(leftDf: DataFrame, leftRange: PartitionRange): DataFrame

  def computeJoin(stepDays: Option[Int] = None): DataFrame = {

    assert(Option(joinConf.metaData.team).nonEmpty,
           s"join.metaData.team needs to be set for join ${joinConf.metaData.name}")

    joinConf.joinParts.asScala.foreach { jp =>
      assert(Option(jp.groupBy.metaData.team).nonEmpty,
             s"groupBy.metaData.team needs to be set for joinPart ${jp.groupBy.metaData.name}")
    }

    // First run command to archive tables that have changed semantically since the last run
    val jobRunTimestamp = Instant.now()
    tablesToRecompute().foreach { tableName =>
      val archiveTry = Try(tableUtils.archiveTableIfExists(tableName, jobRunTimestamp))
      archiveTry.failed.foreach { e =>
        println(s"""Fail to archive table ${tableName}
                   |${e.getMessage}
                   |Proceed to dropping the table instead.
                   |""".stripMargin)
        tableUtils.dropTableIfExists(tableName)
      }
    }

    // run SQL environment setups such as UDFs and JARs
    joinConf.setups.foreach(tableUtils.sql)

    // detect holes and chunks to fill
    val leftStart = Option(joinConf.left.query.startPartition)
      .getOrElse(tableUtils.firstAvailablePartition(joinConf.left.table, joinConf.left.subPartitionFilters).get)
    val leftEnd = Option(joinConf.left.query.endPartition).getOrElse(endPartition)
    val rangeToFill = PartitionRange(leftStart, leftEnd)
    println(s"Join range to fill $rangeToFill")
    val unfilledRanges =
      tableUtils.unfilledRanges(outputTable, rangeToFill, Some(Seq(joinConf.left.table))).getOrElse(Seq.empty)

    stepDays.foreach(metrics.gauge("step_days", _))
    val stepRanges = unfilledRanges.flatMap { unfilledRange =>
      stepDays.map(unfilledRange.steps).getOrElse(Seq(unfilledRange))
    }

    def finalResult: DataFrame = tableUtils.sql(rangeToFill.genScanQuery(null, outputTable))
    if (stepRanges.isEmpty) {
      println(s"\nThere is no data to compute based on end partition of $leftEnd.\n\n Exiting..")
      return finalResult
    }

    println(s"Join ranges to compute: ${stepRanges.map { _.toString }.pretty}")
    stepRanges.zipWithIndex.foreach {
      case (range, index) =>
        val startMillis = System.currentTimeMillis()
        val progress = s"| [${index + 1}/${stepRanges.size}]"
        println(s"Computing join for range: $range  $progress")
        leftDf(joinConf, range, tableUtils).map { leftDfInRange =>
          // set autoExpand = true to ensure backward compatibility due to column ordering changes
          computeRange(leftDfInRange, range).save(outputTable, tableProps, autoExpand = true)
          val elapsedMins = (System.currentTimeMillis() - startMillis) / (60 * 1000)
          metrics.gauge(Metrics.Name.LatencyMinutes, elapsedMins)
          metrics.gauge(Metrics.Name.PartitionCount, range.partitions.length)
          println(s"Wrote to table $outputTable, into partitions: $range $progress in $elapsedMins mins")
        }
    }
    println(s"Wrote to table $outputTable, into partitions: $unfilledRanges")
    finalResult
  }
}
