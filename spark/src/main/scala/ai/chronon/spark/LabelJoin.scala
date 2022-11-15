package ai.chronon.spark

import ai.chronon.api
import ai.chronon.api.DataModel.{Entities, Events}
import ai.chronon.api.Extensions._
import ai.chronon.api.{Accuracy, Constants, JoinPart, Source, TimeUnit, Window}
import ai.chronon.spark.Extensions._
import ai.chronon.online.Metrics
import org.apache.spark.sql.DataFrame
import org.apache.spark.util.sketch.BloomFilter

import scala.collection.JavaConverters._
import scala.collection.parallel.ParMap

class LabelJoin(joinConf: api.Join,
                tableUtils: TableUtils,
                labelDS: String,
                labelPartitionName: String = Constants.LabelPartitionColumn) {

  assert(Option(joinConf.metaData.outputNamespace).nonEmpty, s"output namespace could not be empty or null")
  assert(joinConf.labelJoin.leftStartOffset >= joinConf.labelJoin.getLeftEndOffset,
    s"Start time offset ${joinConf.labelJoin.leftStartOffset} must be earlier than end offset " +
      s"${joinConf.labelJoin.leftEndOffset}")

  val metrics = Metrics.Context(Metrics.Environment.JoinOffline, joinConf)
  private val outputTable = joinConf.metaData.outputTable
  private val labelJoinConf = joinConf.labelJoin
  private val confTableProps = Option(joinConf.metaData.tableProperties)
    .map(_.asScala.toMap)
    .getOrElse(Map.empty[String, String])

  val leftStart = Constants.Partition.minus(labelDS, new Window(labelJoinConf.leftStartOffset, TimeUnit.DAYS))
  val leftEnd = Constants.Partition.minus(labelDS, new Window(labelJoinConf.leftEndOffset, TimeUnit.DAYS))

  def computeLabelJoin(stepDays: Option[Int] = None): DataFrame = {

    assert(Option(joinConf.left.dataModel).equals(Option(Events)),
           s"join.left.dataMode needs to be Events for label join ${joinConf.metaData.name}")

    assert(Option(joinConf.metaData.team).nonEmpty,
      s"join.metaData.team needs to be set for join ${joinConf.metaData.name}")

    labelJoinConf.labels.asScala.foreach { jp =>
      assert(Option(jp.groupBy.dataModel).equals(Option(Entities)),
             s"groupBy.dataModel must be Entities for label join ${jp.groupBy.metaData.name}")

      assert(Option(jp.groupBy.aggregations).isEmpty,
             s"groupBy.aggregations not yet supported for label join ${jp.groupBy.metaData.name}")

      assert(Option(jp.groupBy.metaData.team).nonEmpty,
        s"groupBy.metaData.team needs to be set for label join ${jp.groupBy.metaData.name}")
    }

    labelJoinConf.setups.foreach(tableUtils.sql)
    // update time window
    println(s"Join label window: ${joinConf.left.query.startPartition} - ${joinConf.left.query.endPartition}")
    val labelJoinLeft = joinConf.left.updateQueryPartitions(Option(leftStart), Option(leftEnd))
    println(s"Join label window: ${joinConf.left.query.startPartition} - ${joinConf.left.query.endPartition}")

    compute(labelJoinLeft, stepDays, Option(labelDS), Option(labelPartitionName))
  }

  def compute(left: Source,
              stepDays: Option[Int] = None,
              labelDS: Option[String] = None,
              labelPartition: Option[String] = None): DataFrame = {
    val rangeToFill = PartitionRange(leftStart, leftEnd)
    val today =  Constants.Partition.at(System.currentTimeMillis())
    println(s"Join range to fill $rangeToFill")
    def finalResult = tableUtils.sql(rangeToFill.genScanQuery(null, outputTable))
    val earliestHoleOpt =
      tableUtils.dropPartitionsAfterHole(left.table, outputTable, rangeToFill,
        Map(labelPartition.getOrElse(Constants.LabelPartitionColumn) -> labelDS.getOrElse(today)))
    if (earliestHoleOpt.forall(_ > rangeToFill.end)) {
      println(s"\nThere is no data to compute based on end partition of $leftEnd.\n\n Exiting..")
      return finalResult
    }
    val leftUnfilledRange = PartitionRange(earliestHoleOpt.getOrElse(rangeToFill.start), leftEnd)
    if (leftUnfilledRange.start != null && leftUnfilledRange.end != null) {
      metrics.gauge(Metrics.Name.PartitionCount, leftUnfilledRange.partitions.length)
    }
    labelJoinConf.labels.asScala.foreach { joinPart =>
      val partTable = joinConf.partOutputTable(joinPart)
      println(s"Dropping left unfilled range $leftUnfilledRange from join part table $partTable")
      tableUtils.dropPartitionsAfterHole(left.table, partTable, rangeToFill,
        Map(labelPartition.getOrElse(Constants.LabelPartitionColumn) -> labelDS.getOrElse(today)))
    }

    stepDays.foreach(metrics.gauge("step_days", _))
    val stepRanges = stepDays.map(leftUnfilledRange.steps).getOrElse(Seq(leftUnfilledRange))
    println(s"Join ranges to compute: ${stepRanges.map { _.toString }.pretty}")
    stepRanges.zipWithIndex.foreach {
      case (range, index) =>
        val startMillis = System.currentTimeMillis()
        val progress = s"| [${index + 1}/${stepRanges.size}]"
        println(s"Computing join for range: $range  $progress")
        leftDf(range).map { leftDfInRange =>
          val labelPartitionName = labelPartition.getOrElse(Constants.LabelPartitionColumn)
          computeRange(leftDfInRange, range)
            .appendColumn(labelPartitionName, labelDS.getOrElse(today))
            .save(outputTable, confTableProps, Seq(labelPartitionName, Constants.PartitionColumn), true)

          val elapsedMins = (System.currentTimeMillis() - startMillis) / (60 * 1000)
          metrics.gauge(Metrics.Name.LatencyMinutes, elapsedMins)
          metrics.gauge(Metrics.Name.PartitionCount, range.partitions.length)
          println(s"Wrote to table $outputTable, into partitions: $range $progress in $elapsedMins mins")
        }
    }
    println(s"Wrote to table $outputTable, into partitions: $leftUnfilledRange")
    finalResult
  }

  def leftDf(range: PartitionRange): Option[DataFrame] = {
    val timeProjection = if (joinConf.left.dataModel == Events) {
      Seq(Constants.TimeColumn -> Option(joinConf.left.query).map(_.timeColumn).orNull)
    } else {
      Seq()
    }
    val scanQuery = range.genScanQuery(joinConf.left.query,
      joinConf.left.table,
      fillIfAbsent =
        Map(Constants.PartitionColumn -> null) ++ timeProjection)

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

  def computeRange(leftDf: DataFrame, leftRange: PartitionRange): DataFrame = {
    val leftTaggedDf = if (leftDf.schema.names.contains(Constants.TimeColumn)) {
      leftDf.withTimeBasedColumn(Constants.TimePartitionColumn)
    } else {
      leftDf
    }
    val leftDfCount = leftTaggedDf.count()
    val leftBlooms = labelJoinConf.leftKeyCols.par.map { key =>
      key -> leftDf.generateBloomFilter(key, leftDfCount, joinConf.left.table, leftRange)
    }.toMap

    // compute joinParts in parallel
    val rightDfs = labelJoinConf.labels.asScala.par.map { joinPart =>
      if (joinPart.groupBy.aggregations == null) {
        // no need to generate join part cache if there are no aggregations
        computeJoinPart(leftTaggedDf, joinPart, leftRange, leftBlooms)
      } else {
        // not yet supported
        throw new IllegalArgumentException("Label Join aggregations not supported.")
      }
    }

    val joined = rightDfs.zip(labelJoinConf.labels.asScala).foldLeft(leftTaggedDf) {
      case (partialDf, (rightDf, joinPart)) => joinWithLeft(partialDf, rightDf, joinPart)
    }

    joined.explain()
    joined.drop(Constants.TimePartitionColumn)
  }

  private def computeJoinPart(leftDf: DataFrame,
                              joinPart: JoinPart,
                              unfilledRange: PartitionRange,
                              leftBlooms: ParMap[String, BloomFilter]): DataFrame = {
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
               |  bloom sizes: $bloomSizes
               |  groupBy: ${joinPart.groupBy.toString}
               |""".stripMargin)

    lazy val unfilledTimeRange = {
      val timeRange = leftDf.timeRange
      println(s"left unfilled time range: $timeRange")
      timeRange
    }

    def genGroupBy(partitionRange: PartitionRange) =
      GroupBy.from(joinPart.groupBy, partitionRange, tableUtils, Option(rightBloomMap), rightSkewFilter)

    (joinConf.left.dataModel, joinPart.groupBy.dataModel, joinPart.groupBy.inferredAccuracy) match {
      case (Events, Entities, Accuracy.SNAPSHOT) =>
        genGroupBy(unfilledTimeRange.toPartitionRange).snapshotEntities
      case (_, _, _) => {
        println("Label join only supports Events : Entities")
        leftDf
      }
    }
  }

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
      // increment one day to align with left side ts_ds
      // because one day was decremented from the partition range for snapshot accuracy
      prefixedRight
        .withColumn(Constants.TimePartitionColumn,
          date_format(date_add(col(Constants.PartitionColumn), 0), Constants.Partition.format))
        .drop(Constants.PartitionColumn)
    } else {
      prefixedRight
    }

    leftDf.validateJoinKeys(joinableRight, keys)
    leftDf.join(joinableRight, keys, "left")
  }
}
