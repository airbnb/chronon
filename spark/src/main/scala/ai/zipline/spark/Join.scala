package ai.zipline.spark

import ai.zipline.api.DataModel.{Entities, Events}
import ai.zipline.api.Extensions._
import ai.zipline.api.{Accuracy, Constants, JoinPart, ThriftJsonCodec, Join => JoinConf}
import ai.zipline.online.Metrics
import ai.zipline.spark.Extensions._
import com.google.gson.Gson
import org.apache.spark.sql.DataFrame

import scala.collection.JavaConverters._

class Join(joinConf: JoinConf, endPartition: String, tableUtils: TableUtils) {
  assert(Option(joinConf.metaData.outputNamespace).nonEmpty, s"output namespace could not be empty or null")
  val metrics = Metrics.Context(Metrics.Environment.JoinOffline, joinConf)
  private val outputTable = joinConf.metaData.outputTable

  // Get table properties from config
  private val confTableProps = Option(joinConf.metaData.tableProperties)
    .map(_.asScala.toMap)
    .getOrElse(Map.empty[String, String])

  private val gson = new Gson()
  // Combine tableProperties set on conf with encoded Join
  private val tableProps = confTableProps ++ Map(Constants.SemanticHashKey -> gson.toJson(joinConf.semanticHash.asJava))

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
                    date_format(date_add(col(Constants.PartitionColumn), 1), Constants.Partition.format))
        .drop(Constants.PartitionColumn)
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
    val rightBloomMap = joinPart.rightToLeft.mapValues(leftDf.generateBloomFilter(_, bloomSize))
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

      case (Events, Entities, Accuracy.TEMPORAL) => {
        // Snapshots and mutations are partitioned with ds holding data between <ds 00:00> and ds <23:53>.
        genGroupBy(unfilledTimeRange.toPartitionRange.shift(-1)).entitiesMutations(renamedLeftDf)
      }
    }
  }

  def computeRange(leftDf: DataFrame, leftRange: PartitionRange): DataFrame = {
    val leftTaggedDf = if (leftDf.schema.names.contains(Constants.TimeColumn)) {
      leftDf.withTimeBasedColumn(Constants.TimePartitionColumn)
    } else {
      leftDf
    }

    // compute joinParts in parallel
    val rightDfs = joinConf.joinParts.asScala.par.map { joinPart =>
      val partMetrics = Metrics.Context(metrics, joinPart)
      if (joinPart.groupBy.aggregations == null) {
        // no need to generate join part cache if there are no aggregations
        computeJoinPart(leftTaggedDf, joinPart, leftRange)
      } else {
        // compute only the missing piece
        val joinPartTableName = joinConf.partOutputTable(joinPart)
        val rightUnfilledRange = tableUtils.unfilledRange(joinPartTableName, leftRange, Some(joinConf.left.table))
        println(s"Right unfilled range for $joinPartTableName is $rightUnfilledRange with leftRange of $leftRange")

        if (rightUnfilledRange.isDefined) {
          try {
            val start = System.currentTimeMillis()
            println(s"Writing to join part table: $joinPartTableName")
            val rightDf = computeJoinPart(leftTaggedDf, joinPart, rightUnfilledRange.get)
            // cache the join-part output into table partitions
            rightDf.save(joinPartTableName, tableProps)
            val elapsed = System.currentTimeMillis() - start
            partMetrics.gauge("latency.minutes", elapsed)
            partMetrics.gauge("partition.count", rightUnfilledRange.get.partitions.length)
            println(s"Wrote to join part table: $joinPartTableName in $elapsed ms")
          } catch {
            case e: Exception =>
              println(
                s"Error while processing groupBy: ${joinConf.metaData.name}/${joinPart.groupBy.getMetaData.getName}")
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

  def tablesToRecompute(): Option[Seq[String]] = {
    for (
      props <- tableUtils.getTableProperties(outputTable);
      oldSemanticJson <- props.get(Constants.SemanticHashKey);
      oldSemanticHash = gson.fromJson(oldSemanticJson, classOf[java.util.HashMap[String, String]]).asScala.toMap
    ) yield {
      println(s"Comparing Hashes:\nNew: ${joinConf.semanticHash},\nOld: $oldSemanticHash");
      joinConf.tablesToDrop(oldSemanticHash)
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
    tablesToRecompute().foreach(_.foreach(tableUtils.dropTableIfExists))

    joinConf.setups.foreach(tableUtils.sql)
    val leftStart = Option(joinConf.left.query.startPartition)
      .getOrElse(tableUtils.firstAvailablePartition(joinConf.left.table).get)
    val rangeToFill = PartitionRange(leftStart, endPartition)
    println(s"Join range to fill $rangeToFill")
    def finalResult = tableUtils.sql(rangeToFill.genScanQuery(null, outputTable))
    val earliestHoleOpt = tableUtils.dropPartitionsAfterHole(joinConf.left.table, outputTable, rangeToFill)
    if (earliestHoleOpt.forall(_ > rangeToFill.end)) {
      println(s"\nThere is no data to compute based on end partition of $endPartition.\n\n Exiting..")
      return finalResult
    }
    val leftUnfilledRange = PartitionRange(earliestHoleOpt.getOrElse(rangeToFill.start), endPartition)
    if (leftUnfilledRange.start != null && leftUnfilledRange.end != null) {
      metrics.gauge("partition.count", leftUnfilledRange.partitions.length)
    }
    joinConf.joinParts.asScala.foreach { joinPart =>
      val partTable = joinConf.partOutputTable(joinPart)
      println(s"Dropping left unfilled range $leftUnfilledRange from join part table $partTable")
      tableUtils.dropPartitionsAfterHole(joinConf.left.table, partTable, rangeToFill)
    }

    val timeProjection = if (joinConf.left.dataModel == Events) {
      Seq(Constants.TimeColumn -> Option(joinConf.left.query).map(_.timeColumn).orNull)
    } else {
      Seq()
    }
    stepDays.foreach(metrics.gauge("step_days", _))
    val stepRanges = stepDays.map(leftUnfilledRange.steps).getOrElse(Seq(leftUnfilledRange))
    println(s"Join ranges to compute: ${stepRanges.map { _.toString }.pretty}")
    stepRanges.zipWithIndex.foreach {
      case (range, index) =>
        val startMillis = System.currentTimeMillis()
        val progress = s"| [${index + 1}/${stepRanges.size}]"
        println(s"Computing join for range: $range  $progress")

        val scanQuery = range.genScanQuery(joinConf.left.query,
                                           joinConf.left.table,
                                           fillIfAbsent =
                                             Map(Constants.PartitionColumn -> null) ++ timeProjection)

        val leftDfInRange: DataFrame = {
          val df = tableUtils.sql(scanQuery)
          val skewFilter = joinConf.skewFilter()
          val filteredDf = skewFilter
            .map(sf => {
              println(s"left skew filter: $sf")
              df.filter(sf)
            })
            .getOrElse(df)
          filteredDf
        }
        if (!leftDfInRange.isEmpty) {
          computeRange(leftDfInRange, range).save(outputTable, tableProps)
          metrics.gauge("latency.minutes", (System.currentTimeMillis() - startMillis) / (60 * 1000))
          metrics.gauge("partition.count", range.partitions.length)
          println(s"Wrote to table $outputTable, into partitions: $range $progress")
        } else {
          println(s"Left side query below produced 0 rows in range $range, moving onto next range. \n $scanQuery")
        }

        val start = System.currentTimeMillis()
        computeRange(leftDfInRange.prunePartition(range), range).save(outputTable, tableProps)
        val elapsed = System.currentTimeMillis() - start
        println(s"Wrote $range to table $outputTable, into partitions: $range $progress, in $elapsed ms")
    }
    println(s"Wrote to table $outputTable, into partitions: $leftUnfilledRange")
    finalResult
  }
}
