package ai.chronon.spark

import ai.chronon.api
import ai.chronon.api.DataModel.{Entities, Events}
import ai.chronon.api.Extensions._
import ai.chronon.api.{Accuracy, Constants, JoinPart, StructField}
import ai.chronon.online.{JoinCodec, Metrics}
import ai.chronon.spark.Extensions._
import com.google.gson.Gson
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

import java.time.Instant
import scala.collection.JavaConverters._

class Join(joinConf: api.Join, endPartition: String, tableUtils: TableUtils) {
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
    val prefixedRight = keyRenamedRight.prefixColumnNames(joinPart.fullPrefix, valueColumns)

    // compute join keys, besides the groupBy keys -  like ds, ts etc.,
    val keys = partLeftKeys ++ additionalKeys

    val partName = joinPart.groupBy.metaData.name

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

    val finalDf = mergeDFs(leftDf, joinableRight, keys)
    println(s"""Join keys for $partName: ${keys.mkString(", ")}
         |Left Schema:
         |${leftDf.schema.pretty}
         |Right Schema:
         |${joinableRight.schema.pretty}
         |Final Schema:
         |${finalDf.schema.pretty}
         |""".stripMargin)

    finalDf
  }

  private def mergeDFs(leftDf: DataFrame, rightDf: DataFrame, keys: Seq[String]): DataFrame = {
    leftDf.validateJoinKeys(rightDf, keys)
    val joinedDf = leftDf.join(rightDf, keys, "left")

    // find columns that exist both on left and right that are not keys and merge them.
    // this could happen in waterfall: left => log => backfill
    val sharedColumns = rightDf.columns.intersect(leftDf.columns)
    val selects = keys.map(col) ++
      leftDf.columns.flatMap { colName =>
        if (keys.contains(colName)) {
          None
        } else if (sharedColumns.contains(colName)) {
          Some(coalesce(leftDf(colName), rightDf(colName)).as(colName))
        } else {
          Some(leftDf(colName))
        }
      } ++
      rightDf.columns.flatMap { colName =>
        if (keys.contains(colName)) {
          None
        } else if (sharedColumns.contains(colName)) {
          None
        } else {
          Some(rightDf(colName))
        }
      }
    val finalDf = joinedDf.select(selects: _*)
    finalDf
  }

  private def computeJoinPart(leftDf: DataFrame, joinPart: JoinPart): Option[DataFrame] = {

    val stats = leftDf
      .select(
        count(lit(1)),
        min(Constants.PartitionColumn),
        max(Constants.PartitionColumn)
      )
      .head()
    val rowCount = stats.getLong(0)

    // construct unfilledRange based on the remaining rows where logs are not available
    val unfilledRange = PartitionRange(stats.getString(1), stats.getString(2))
    if (rowCount == 0) {
      // happens when all rows are already filled by logs
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

  private def getUnfilledRows(joinPart: JoinPart,
                              leftLogJoinDf: DataFrame,
                              valueSchemas: Map[String, Set[StructField]],
                              joinPartFields: Array[StructField]): DataFrame = {

    val validHashes = valueSchemas
      .filter(entry => joinPartFields.forall(entry._2.contains))
      .keys
      .toSeq

    println(
      s"Splitting left into filled vs unfilled by validHashes: ${validHashes.pretty} for joinPart: ${joinPart.groupBy.metaData.name}")

    if (joinPartFields.map(_.name).exists(n => !leftLogJoinDf.columns.contains(n))) {
      return leftLogJoinDf
    }
    if (!leftLogJoinDf.columns.contains(Constants.SchemaHash)) {
      return leftLogJoinDf
    }
    leftLogJoinDf.where(not(col(Constants.SchemaHash).isin(validHashes: _*)))
  }

  /*
   * if rowIdColumns is set, use it as the join key,
   * otherwise, use keys + ts, which has to be conditionally based on schema_hash.
   *
   * we need to handle the scenario where a new key is added to driver so it only exists on the left but not in the log
   */
  private def joinLeftWithLogTable(leftDf: DataFrame,
                                   leftRange: PartitionRange,
                                   keySchemas: Map[String, Set[StructField]]): DataFrame = {
    if (tableUtils.tableExists(joinConf.metaData.loggedTable)) {
      // We require that logs are in the same date range as the left rows
      val unfilledRangeOpt = tableUtils.unfilledRange(joinConf.metaData.leftLogJoinTable, leftRange)
      unfilledRangeOpt.foreach { unfilledRange =>
        val leftDfInRange = leftDf.prunePartition(unfilledRange)

        // separately define logDf for each schemaHash to avoid the spark ambiguous join problem
        def logDf(schemaHash: Option[String] = None): DataFrame = {
          val scanQuery =
            tableUtils.sql(unfilledRange.genScanQuery(query = null, table = joinConf.metaData.loggedTable))
          if (schemaHash.isDefined) {
            scanQuery.where(col(Constants.SchemaHash) === schemaHash.get)
          } else {
            scanQuery
          }
        }

        val leftLogJoinedDf = if (joinConf.isSetRowIdColumns) {
          mergeDFs(leftDfInRange, logDf(), joinConf.rowIdColumns.asScala)
        } else {
          val joinableRights = keySchemas.toSeq.flatMap {
            case (hash, keys) =>
              val sharedKeys = keys.map(_.name).toSeq.intersect(leftDfInRange.columns)
              if (sharedKeys.isEmpty) {
                None
              } else {
                Some((logDf(Some(hash)), sharedKeys :+ "ts"))
              }
          }
          joinableRights.foldLeft(leftDfInRange) {
            case (partialLeftDf, (rightDf, keys)) => mergeDFs(partialLeftDf, rightDf, keys)
          }
        }
        tableUtils.insertPartitions(
          leftLogJoinedDf,
          joinConf.metaData.leftLogJoinTable,
          tableProps,
          autoExpand = true
        )
      }
      tableUtils.sql(leftRange.genScanQuery(query = null, table = joinConf.metaData.leftLogJoinTable))
    } else {
      leftDf
    }
  }

  private def computeRange(leftDf: DataFrame, leftRange: PartitionRange): DataFrame = {
    val leftTaggedDf = if (leftDf.schema.names.contains(Constants.TimeColumn)) {
      leftDf.withTimeBasedColumn(Constants.TimePartitionColumn)
    } else {
      leftDf
    }
    val loggingSchemas = LogFlattenerJob
      .readSchemaTableProperties(tableUtils, joinConf)
      .mapValues(value =>
        (
          JoinCodec.fromLoggingSchema(value, joinConf).keyFields.toSet,
          JoinCodec.fromLoggingSchema(value, joinConf).valueFields.toSet
        ))
    val leftLogJoinDf = joinLeftWithLogTable(leftTaggedDf, leftRange, loggingSchemas.mapValues(_._1))

    val parts = joinConf.joinParts.asScala.map { jp =>
      val partFields =
        GroupBy.from(jp.groupBy, leftRange, tableUtils).outputSchema.fields.map(jp.constructJoinPartSchema)
      (jp, partFields)
    }

    // compute joinParts in parallel
    val rights = parts.par.flatMap {
      case (joinPart, joinPartFields) =>
        val partMetrics = Metrics.Context(metrics, joinPart)

        // filter down to rows where features are NOT populated by logging
        val unfilledLeftDf =
          getUnfilledRows(joinPart, leftLogJoinDf, loggingSchemas.mapValues(_._2), joinPartFields)
            .select(leftTaggedDf.columns.head, leftTaggedDf.columns.tail: _*)

        if (joinPart.groupBy.aggregations == null) {
          // no need to generate join part cache if there are no aggregations
          computeJoinPart(unfilledLeftDf, joinPart).map(df => (df, joinPart))
        } else {
          // compute only the missing piece
          val joinPartTableName = joinConf.partOutputTable(joinPart)
          val rightUnfilledRange = tableUtils.unfilledRange(joinPartTableName, leftRange, Some(joinConf.left.table))
          println(s"Right unfilled range for $joinPartTableName is $rightUnfilledRange with leftRange of $leftRange")

          if (rightUnfilledRange.isDefined) {
            try {
              val start = System.currentTimeMillis()
              val filledDf = computeJoinPart(
                unfilledLeftDf.prunePartition(rightUnfilledRange.get),
                joinPart
              )
              // cache the join-part output into table partitions
              filledDf.foreach { df =>
                println(s"Writing to join part table: $joinPartTableName")
                df.save(joinPartTableName, tableProps)
                val elapsedMins = (System.currentTimeMillis() - start) / 60000
                partMetrics.gauge(Metrics.Name.LatencyMinutes, elapsedMins)
                partMetrics.gauge(Metrics.Name.PartitionCount, rightUnfilledRange.get.partitions.length)
                println(s"Wrote to join part table: $joinPartTableName in $elapsedMins minutes")
              }
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

          if (tableUtils.tableExists(joinPartTableName)) {
            Some(
              tableUtils.sql(scanRange.genScanQuery(query = null, joinPartTableName)),
              joinPart
            )
          } else {
            None
          }
        }
    }

    val joined = rights.foldLeft(leftLogJoinDf) {
      case (partialDf, (rightDf, joinPart)) => joinWithLeft(partialDf, rightDf, joinPart)
    }

    joined.explain()

    // select required columns to exclude old features pulled from logs that no longer exist in join definition
    val outputDataColumns = leftTaggedDf.columns ++ parts.flatMap(_._2.map(_.name))
    val outputColumns = if (joined.columns.contains(Constants.SchemaHash)) {
      Constants.SchemaHash +: outputDataColumns
    } else {
      outputDataColumns
    }
    joined.select(outputColumns.map(col): _*).drop(Constants.TimePartitionColumn)
  }

  def tablesToRecompute(): Option[Seq[String]] = {
    for (
      props <- tableUtils.getTableProperties(outputTable);
      oldSemanticJson <- props.get(Constants.SemanticHashKey);
      oldSemanticHash = gson.fromJson(oldSemanticJson, classOf[java.util.HashMap[String, String]]).asScala.toMap
    ) yield {
      println(s"Comparing Hashes:\nNew: ${joinConf.semanticHash.pretty},\nOld: ${oldSemanticHash.pretty}")
      joinConf.tablesToDrop(oldSemanticHash)
    }
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

  def computeJoin(stepDays: Option[Int] = None): DataFrame = {

    assert(Option(joinConf.metaData.team).nonEmpty,
           s"join.metaData.team needs to be set for join ${joinConf.metaData.name}")

    joinConf.joinParts.asScala.foreach { jp =>
      assert(Option(jp.groupBy.metaData.team).nonEmpty,
             s"groupBy.metaData.team needs to be set for joinPart ${jp.groupBy.metaData.name}")
    }

    // First run command to archive tables that have changed semantically since the last run
    val jobRunTimestamp = Instant.now()
    tablesToRecompute().foreach(_.foreach(tableName => tableUtils.archiveTableIfExists(tableName, jobRunTimestamp)))

    joinConf.setups.foreach(tableUtils.sql)
    val leftStart = Option(joinConf.left.query.startPartition)
      .getOrElse(tableUtils.firstAvailablePartition(joinConf.left.table).get)
    val leftEnd = Option(joinConf.left.query.endPartition).getOrElse(endPartition)
    val rangeToFill = PartitionRange(leftStart, leftEnd)
    println(s"Join range to fill $rangeToFill")
    def finalResult = tableUtils.sql(rangeToFill.genScanQuery(null, outputTable))
    val earliestHoleOpt = tableUtils.dropPartitionsAfterHole(joinConf.left.table, outputTable, rangeToFill)
    if (earliestHoleOpt.forall(_ > rangeToFill.end)) {
      println(s"\nThere is no data to compute based on end partition of $leftEnd.\n\n Exiting..")
      return finalResult
    }
    val leftUnfilledRange = PartitionRange(earliestHoleOpt.getOrElse(rangeToFill.start), leftEnd)
    if (leftUnfilledRange.start != null && leftUnfilledRange.end != null) {
      metrics.gauge(Metrics.Name.PartitionCount, leftUnfilledRange.partitions.length)
    }
    joinConf.joinParts.asScala.foreach { joinPart =>
      val partTable = joinConf.partOutputTable(joinPart)
      println(s"Dropping left unfilled range $leftUnfilledRange from join part table $partTable")
      tableUtils.dropPartitionsAfterHole(joinConf.left.table, partTable, rangeToFill)
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
          // set autoExpand = true to ensure backward compatibility due to column ordering changes
          computeRange(leftDfInRange, range).save(outputTable, tableProps, autoExpand = true)
          val elapsedMins = (System.currentTimeMillis() - startMillis) / (60 * 1000)
          metrics.gauge(Metrics.Name.LatencyMinutes, elapsedMins)
          metrics.gauge(Metrics.Name.PartitionCount, range.partitions.length)
          println(s"Wrote to table $outputTable, into partitions: $range $progress in $elapsedMins mins")
        }
    }
    println(s"Wrote to table $outputTable, into partitions: $leftUnfilledRange")
    finalResult
  }
}
