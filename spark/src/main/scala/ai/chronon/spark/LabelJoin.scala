package ai.chronon.spark

import ai.chronon.api
import ai.chronon.api.DataModel.{Entities, Events}
import ai.chronon.api.Extensions._
import ai.chronon.api.{Constants, JoinPart, TimeUnit, Window}
import ai.chronon.spark.Extensions._
import ai.chronon.online.Metrics
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.lit
import org.apache.spark.util.sketch.BloomFilter

import scala.collection.JavaConverters._
import scala.collection.Seq
import scala.collection.parallel.ParMap
import scala.util.ScalaJavaConversions.IterableOps

class LabelJoin(joinConf: api.Join, tableUtils: TableUtils, labelDS: String) {

  assert(Option(joinConf.metaData.outputNamespace).nonEmpty, s"output namespace could not be empty or null")
  assert(
    joinConf.labelPart.leftStartOffset >= joinConf.labelPart.getLeftEndOffset,
    s"Start time offset ${joinConf.labelPart.leftStartOffset} must be earlier than end offset " +
      s"${joinConf.labelPart.leftEndOffset}"
  )

  val metrics = Metrics.Context(Metrics.Environment.LabelJoin, joinConf)
  private val outputLabelTable = joinConf.metaData.outputLabelTable
  private val labelJoinConf = joinConf.labelPart
  private val confTableProps = Option(joinConf.metaData.tableProperties)
    .map(_.asScala.toMap)
    .getOrElse(Map.empty[String, String])

  val leftStart = Constants.Partition.minus(labelDS, new Window(labelJoinConf.leftStartOffset, TimeUnit.DAYS))
  val leftEnd = Constants.Partition.minus(labelDS, new Window(labelJoinConf.leftEndOffset, TimeUnit.DAYS))

  def computeLabelJoin(stepDays: Option[Int] = None, skipFinalJoin: Boolean = false): DataFrame = {
    // validations
    assert(Option(joinConf.left.dataModel).equals(Option(Events)),
           s"join.left.dataMode needs to be Events for label join ${joinConf.metaData.name}")

    assert(Option(joinConf.metaData.team).nonEmpty,
           s"join.metaData.team needs to be set for join ${joinConf.metaData.name}")

    var leftRange = PartitionRange(leftStart, leftEnd)
    labelJoinConf.labels.asScala.foreach { jp =>
      if (Option(jp.groupBy.aggregations).isDefined) {
        assert(Option(jp.groupBy.dataModel).equals(Option(Events)),
               s"groupBy.dataModel must be Events for label join with aggregations ${jp.groupBy.metaData.name}")

        assert(Option(jp.groupBy.aggregations).get.size() == 1,
               s"Multiple aggregations not yet supported for label join ${jp.groupBy.metaData.name}")

        assert(
          Option(jp.groupBy.aggregations.get(0).windows).get.size() == 1,
          s"Multiple aggregation windows not yet supported for label join ${jp.groupBy.metaData.name}"
        )

        val aggWindow = jp.groupBy.aggregations.get(0).windows.get(0)
        assert(aggWindow.timeUnit == TimeUnit.DAYS,
               s"${aggWindow.timeUnit} window time unit not supported for label aggregations.")
        // update leftStart & leftEnd to align with window size for aggregation
        // leftStartOffset/leftEndOffset = label_ds - window + 1
        val updatedStart = Constants.Partition.minus(labelDS, new Window(aggWindow.getLength - 1, TimeUnit.DAYS))
        val updatedEnd = Constants.Partition.minus(labelDS, new Window(aggWindow.getLength - 1, TimeUnit.DAYS))
        leftRange = PartitionRange(updatedStart, updatedEnd)
      } else {
        assert(
          Option(jp.groupBy.dataModel).equals(Option(Entities)),
          s"To perform a none-aggregation label join, the groupBy.dataModel must be entities: ${jp.groupBy.metaData.name}"
        )
      }

      assert(Option(jp.groupBy.metaData.team).nonEmpty,
             s"groupBy.metaData.team needs to be set for label join ${jp.groupBy.metaData.name}")
    }

    labelJoinConf.setups.foreach(tableUtils.sql)
    val labelTable = compute(leftRange, stepDays, Option(labelDS))

    if (skipFinalJoin) {
      labelTable
    } else {
      // creating final join view with feature join output table
      println(s"Joining label table : ${outputLabelTable} with joined output table : ${joinConf.metaData.outputTable}")
      JoinUtils.createOrReplaceView(
        joinConf.metaData.outputFinalView,
        leftTable = joinConf.metaData.outputTable,
        rightTable = outputLabelTable,
        joinKeys = labelJoinConf.rowIdentifier(joinConf.rowIds),
        tableUtils = tableUtils,
        viewProperties = Map(Constants.LabelViewPropertyKeyLabelTable -> outputLabelTable,
                             Constants.LabelViewPropertyFeatureTable -> joinConf.metaData.outputTable)
      )
      println(s"Final labeled view created: ${joinConf.metaData.outputFinalView}")
      JoinUtils.createLatestLabelView(joinConf.metaData.outputLatestLabelView,
                                      baseView = joinConf.metaData.outputFinalView,
                                      tableUtils)
      println(s"Final view with latest label created: ${joinConf.metaData.outputLatestLabelView}")
      labelTable
    }
  }

  def compute(leftRange: PartitionRange, stepDays: Option[Int] = None, labelDS: Option[String] = None): DataFrame = {
    val today = Constants.Partition.at(System.currentTimeMillis())
    val sanitizedLabelDs = labelDS.getOrElse(today)
    println(s"Label join range to fill $leftRange")
    def finalResult = tableUtils.sql(leftRange.genScanQuery(null, outputLabelTable))

    val leftFeatureRange = leftRange
    stepDays.foreach(metrics.gauge("step_days", _))
    val stepRanges = stepDays.map(leftFeatureRange.steps).getOrElse(Seq(leftFeatureRange))
    println(s"Label Join left ranges to compute: ${stepRanges.map { _.toString }.pretty}")
    stepRanges.zipWithIndex.foreach {
      case (range, index) =>
        val startMillis = System.currentTimeMillis()
        val progress = s"| [${index + 1}/${stepRanges.size}]"
        println(s"Computing label join for range: $range  Label DS: ${labelDS.getOrElse(today)} $progress")
        JoinUtils.leftDf(joinConf, range, tableUtils).map { leftDfInRange =>
          computeRange(leftDfInRange, range, sanitizedLabelDs).save(outputLabelTable,
                                                                    confTableProps,
                                                                    Seq(Constants.LabelPartitionColumn,
                                                                        Constants.PartitionColumn),
                                                                    true)
          val elapsedMins = (System.currentTimeMillis() - startMillis) / (60 * 1000)
          metrics.gauge(Metrics.Name.LatencyMinutes, elapsedMins)
          metrics.gauge(Metrics.Name.PartitionCount, range.partitions.length)
          println(s"Wrote to table $outputLabelTable, into partitions: $range $progress in $elapsedMins mins")
        }
    }
    println(s"Wrote to table $outputLabelTable, into partitions: $leftFeatureRange")
    finalResult
  }

  def computeRange(leftDf: DataFrame, leftRange: PartitionRange, sanitizedLabelDs: String): DataFrame = {
    val leftDfCount = leftDf.count()
    val leftBlooms = labelJoinConf.leftKeyCols.toSeq.parallel.map { key =>
      key -> leftDf.generateBloomFilter(key, leftDfCount, joinConf.left.table, leftRange)
    }.toMap

    // compute joinParts in parallel
    val rightDfs = labelJoinConf.labels.asScala.parallel.map { labelJoinPart =>
      val labelJoinPartMetrics = Metrics.Context(metrics, labelJoinPart)
      if (labelJoinPart.groupBy.aggregations == null) {
        // no need to generate join part cache if there are no aggregations
        computeLabelPart(labelJoinPart, leftRange, leftBlooms)
      } else {
        val labelOutputRange = PartitionRange(sanitizedLabelDs, sanitizedLabelDs)
        val partTable = joinConf.partOutputTable(labelJoinPart)
        try {
          val leftRanges = tableUtils
            .unfilledRanges(partTable, labelOutputRange, skipFirstHole = false)
            .getOrElse(Seq())
          val partitionCount = leftRanges.map(_.partitions.length).sum
          if (partitionCount > 0) {
            val start = System.currentTimeMillis()
            leftRanges
              .foreach(leftRange => {
                val labeledDf = computeLabelPart(labelJoinPart, leftRange, leftBlooms)
                // Cache label part data into intermediate table
                println(s"Writing to join part table: $partTable for partition range $leftRange")
                labeledDf.save(tableName = partTable,
                               tableProperties = confTableProps,
                               partitionColumns = Seq(Constants.LabelPartitionColumn))
              })
            val elapsedMins = (System.currentTimeMillis() - start) / 60000
            labelJoinPartMetrics.gauge(Metrics.Name.LatencyMinutes, elapsedMins)
            labelJoinPartMetrics.gauge(Metrics.Name.PartitionCount, partitionCount)
            println(s"Wrote ${partitionCount} partitions to label part table: $partTable in $elapsedMins minutes")
          }
        } catch {
          case e: Exception =>
            println(
              s"Error while processing groupBy: " +
                s"${joinConf.metaData.name}/${labelJoinPart.groupBy.getMetaData.getName}")
            throw e
        }
        tableUtils.sql(
          labelOutputRange.genScanQuery(query = null, partTable, partitionColumn = Constants.LabelPartitionColumn))
      }
    }

    val rowIdentifier = labelJoinConf.rowIdentifier(joinConf.rowIds)
    println("Label Join filtering left df with only row identifier:", rowIdentifier.mkString(", "))
    val leftFiltered = JoinUtils.filterColumns(leftDf, rowIdentifier)

    val joined = rightDfs.zip(labelJoinConf.labels.asScala).foldLeft(leftFiltered) {
      case (partialDf, (rightDf, joinPart)) => joinWithLeft(partialDf, rightDf, joinPart)
    }

    // assign label ds value and drop duplicates
    val updatedJoin = joined
      .withColumn(Constants.LabelPartitionColumn, lit(sanitizedLabelDs))
      .dropDuplicates(rowIdentifier)
    updatedJoin.explain()
    updatedJoin.drop(Constants.TimePartitionColumn)
  }

  private def computeLabelPart(joinPart: JoinPart,
                               leftRange: PartitionRange,
                               leftBlooms: ParMap[String, BloomFilter]): DataFrame = {
    val rightSkewFilter = joinConf.partSkewFilter(joinPart)
    val rightBloomMap = joinPart.rightToLeft.mapValues(leftBlooms(_)).toMap
    val bloomSizes = rightBloomMap.map { case (col, bloom) => s"$col -> ${bloom.bitSize()}" }.pretty
    println(s"""
               |Label JoinPart Info:
               |  part name : ${joinPart.groupBy.metaData.name},
               |  left type : ${joinConf.left.dataModel},
               |  right type: ${joinPart.groupBy.dataModel},
               |  accuracy  : ${joinPart.groupBy.inferredAccuracy},
               |  part unfilled range: $leftRange,
               |  bloom sizes: $bloomSizes
               |  groupBy: ${joinPart.groupBy.toString}
               |""".stripMargin)

    val groupBy = GroupBy.from(joinPart.groupBy,
                               PartitionRange(labelDS, labelDS),
                               tableUtils,
                               Option(rightBloomMap),
                               rightSkewFilter)

    val df = (joinConf.left.dataModel, joinPart.groupBy.dataModel, joinPart.groupBy.inferredAccuracy) match {
      case (Events, Entities, _) =>
        groupBy.snapshotEntities
      case (Events, Events, _) =>
        groupBy.snapshotEvents(leftRange)
      case (_, _, _) =>
        throw new IllegalArgumentException(
          s"Data model type ${joinConf.left.dataModel}:${joinPart.groupBy.dataModel} " +
            s"not supported for label join. Valid type [Events : Entities] or [Events : Events]")
    }
    df.withColumnRenamed(Constants.PartitionColumn, Constants.LabelPartitionColumn)
  }

  def joinWithLeft(leftDf: DataFrame, rightDf: DataFrame, joinPart: JoinPart): DataFrame = {
    val partLeftKeys = joinPart.rightToLeft.values.toArray
    // drop dup label_ds column if exists in leftDf.
    val updatedLeftDf = if (leftDf.columns.contains(Constants.LabelPartitionColumn)) {
      leftDf.drop(Constants.LabelPartitionColumn)
    } else {
      leftDf
    }

    // apply key-renaming to key columns
    val keyRenamedRight = joinPart.rightToLeft.foldLeft(rightDf) {
      case (rightDf, (rightKey, leftKey)) => rightDf.withColumnRenamed(rightKey, leftKey)
    }

    val nonValueColumns = joinPart.rightToLeft.keys.toArray ++ Array(Constants.TimeColumn,
                                                                     Constants.PartitionColumn,
                                                                     Constants.TimePartitionColumn,
                                                                     Constants.LabelPartitionColumn)
    val valueColumns = rightDf.schema.names.filterNot(nonValueColumns.contains)
    val prefixedRight = keyRenamedRight.prefixColumnNames(joinPart.fullPrefix, valueColumns)

    val partName = joinPart.groupBy.metaData.name

    println(s"""Join keys for $partName: ${partLeftKeys.mkString(", ")}
               |Left Schema:
               |${updatedLeftDf.schema.pretty}
               |
               |Right Schema:
               |${prefixedRight.schema.pretty}
               |
               |""".stripMargin)

    updatedLeftDf.validateJoinKeys(prefixedRight, partLeftKeys)
    updatedLeftDf.join(prefixedRight, partLeftKeys, "left_outer")
  }
}
