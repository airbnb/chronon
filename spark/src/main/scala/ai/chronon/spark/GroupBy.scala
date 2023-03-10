package ai.chronon.spark

import ai.chronon.aggregator.base.TimeTuple
import ai.chronon.aggregator.row.RowAggregator
import ai.chronon.aggregator.windowing._
import ai.chronon.api
import ai.chronon.api.Constants
import ai.chronon.api.DataModel.{Entities, Events}
import ai.chronon.api.Extensions._
import ai.chronon.spark.Extensions._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.util.sketch.BloomFilter
import org.slf4j.{Logger, LoggerFactory}

import java.util
import scala.collection.JavaConverters._
import scala.collection.mutable

class GroupBy(val aggregations: Seq[api.Aggregation],
              val keyColumns: Seq[String],
              val inputDf: DataFrame,
              val mutationDf: DataFrame = null,
              skewFilter: Option[String] = None,
              finalize: Boolean = true)
    extends Serializable {

  import GroupBy.logger

  protected[spark] val tsIndex: Int = inputDf.schema.fieldNames.indexOf(Constants.TimeColumn)
  protected val selectedSchema: Array[(String, api.DataType)] = Conversions.toChrononSchema(inputDf.schema)

  val keySchema: StructType = StructType(keyColumns.map(inputDf.schema.apply).toArray)
  implicit val sparkSession: SparkSession = inputDf.sparkSession
  // distinct inputs to aggregations - post projection types that needs to match with
  // streaming's post projection types etc.,
  val preAggSchema: StructType = if (aggregations != null) {
    StructType(
      aggregations
        .flatMap(agg =>
          Option(agg.buckets)
            .map(_.asScala)
            .getOrElse(Seq.empty[String]) :+
            agg.inputColumn)
        .distinct
        .map(inputDf.schema.apply))
  } else {
    val values = inputDf.schema.map(_.name).filterNot((keyColumns ++ Constants.ReservedColumns).contains)
    val valuesIndices = values.map(inputDf.schema.fieldIndex).toArray
    StructType(valuesIndices.map(inputDf.schema))
  }

  lazy val outputSchema: api.StructType = {
    val columns = if (aggregations != null) {
      windowAggregator.outputSchema
    } else {
      Conversions.toChrononSchema(preAggSchema)
    }
    api.StructType("", columns.map(tup => api.StructField(tup._1, tup._2)))
  }

  lazy val aggregationParts = aggregations.flatMap(_.unpack)

  lazy val columnAggregators = (new RowAggregator(selectedSchema, aggregationParts)).columnAggregators

  //should be only used when aggregations != null
  lazy val aggPartWithSchema = aggregationParts.zip(columnAggregators.map(_.outputType))

  lazy val postAggSchema: StructType = {
    val valueChrononSchema = if (finalize) windowAggregator.outputSchema else windowAggregator.irSchema
    Conversions.fromChrononSchema(valueChrononSchema)
  }

  @transient
  protected[spark] lazy val windowAggregator: RowAggregator =
    new RowAggregator(selectedSchema, aggregations.flatMap(_.unpack))

  def snapshotEntitiesBase: RDD[(Array[Any], Array[Any])] = {
    val keyBuilder = FastHashing.generateKeyBuilder((keyColumns :+ Constants.PartitionColumn).toArray, inputDf.schema)
    val (preppedInputDf, irUpdateFunc) = if (aggregations.hasWindows) {
      val partitionTs = "ds_ts"
      val inputWithPartitionTs = inputDf.withPartitionBasedTimestamp(partitionTs)
      val partitionTsIndex = inputWithPartitionTs.schema.fieldIndex(partitionTs)
      val updateFunc = (ir: Array[Any], row: Row) => {
        // update when ts < tsOf(ds + 1)
        windowAggregator.updateWindowed(ir,
                                        Conversions.toChrononRow(row, tsIndex),
                                        row.getLong(partitionTsIndex) + Constants.Partition.spanMillis)
        ir
      }
      inputWithPartitionTs -> updateFunc
    } else {
      val updateFunc = (ir: Array[Any], row: Row) => {
        windowAggregator.update(ir, Conversions.toChrononRow(row, tsIndex))
        ir
      }
      inputDf -> updateFunc
    }

    logger.info("prepped input schema")
    logger.info(preppedInputDf.schema.pretty)

    preppedInputDf.rdd
      .keyBy(keyBuilder)
      .aggregateByKey(windowAggregator.init)(seqOp = irUpdateFunc, combOp = windowAggregator.merge)
      .map { case (keyWithHash, ir) => keyWithHash.data -> normalizeOrFinalize(ir) }
  }

  def snapshotEntities: DataFrame =
    if (aggregations == null || aggregations.isEmpty) {
      inputDf
    } else {
      toDf(snapshotEntitiesBase, Seq(Constants.PartitionColumn -> StringType))
    }

  def snapshotEventsBase(partitionRange: PartitionRange,
                         resolution: Resolution = DailyResolution): RDD[(Array[Any], Array[Any])] = {
    val endTimes: Array[Long] = partitionRange.toTimePoints
    // add 1 day to the end times to include data [ds 00:00:00.000, ds + 1 00:00:00.000)
    val shiftedEndTimes = endTimes.map(_ + Constants.Partition.spanMillis)
    val sawtoothAggregator = new SawtoothAggregator(aggregations, selectedSchema, resolution)
    val hops = hopsAggregate(endTimes.min, resolution)

    hops
      .flatMap {
        case (keys, hopsArrays) =>
          val irs = sawtoothAggregator.computeWindows(hopsArrays, shiftedEndTimes)
          irs.indices.map { i =>
            (keys.data :+ Constants.Partition.at(endTimes(i)), normalizeOrFinalize(irs(i)))
          }
      }
  }
  // Calculate snapshot accurate windows for ALL keys at pre-defined "endTimes"
  // At this time, we hardcode the resolution to Daily, but it is straight forward to support
  // hourly resolution.
  def snapshotEvents(partitionRange: PartitionRange): DataFrame =
    toDf(snapshotEventsBase(partitionRange), Seq((Constants.PartitionColumn, StringType)))

  /**
    * Support for entities with mutations.
    * Three way join between:
    *   Queries: grouped by key and dsOf[ts]
    *   Snapshot[InputDf]: Grouped by key and ds providing a FinalBatchIR to be extended.
    *   Mutations[MutationDf]: Grouped by key and dsOf[MutationTs] providing an array of updates/deletes to be done
    * With this process the components (end of day batchIr + day's mutations + day's queries -> output)
    */
  def temporalEntities(queriesUnfilteredDf: DataFrame, resolution: Resolution = FiveMinuteResolution): DataFrame = {

    // Add extra column to the queries and generate the key hash.
    val queriesDf = queriesUnfilteredDf.removeNulls(keyColumns)
    val timeBasedPartitionColumn = "ds_of_ts"
    val queriesWithTimeBasedPartition = queriesDf.withTimeBasedColumn(timeBasedPartitionColumn)

    val queriesKeyHashFx = FastHashing.generateKeyBuilder(keyColumns.toArray, queriesWithTimeBasedPartition.schema)
    val timeBasedPartitionIndex = queriesWithTimeBasedPartition.schema.fieldIndex(timeBasedPartitionColumn)
    val timeIndex = queriesWithTimeBasedPartition.schema.fieldIndex(Constants.TimeColumn)
    val partitionIndex = queriesWithTimeBasedPartition.schema.fieldIndex(Constants.PartitionColumn)

    // queries by key & ds_of_ts
    val queriesByKeys = queriesWithTimeBasedPartition.rdd
      .map { row =>
        val ts = row.getLong(timeIndex)
        val partition = row.getString(partitionIndex)
        (
          (queriesKeyHashFx(row), row.getString(timeBasedPartitionIndex)),
          TimeTuple.make(ts, partition)
        )
      }
      .groupByKey()
      .mapValues { _.toArray.uniqSort(TimeTuple) }

    // Snapshot data needs to be shifted. We need to extract the end state of the IR by EOD before mutations.
    // Since partition data for <ds> contains all history up to and including <ds>, we need to join with the previous ds.
    // This is the same as the code for snapshot entities. Define behavior for aggregateByKey.
    val shiftedColumnName = "end_of_day_ds"
    val shiftedColumnNameTs = "end_of_day_ts"
    val expandedInputDf = inputDf
      .withShiftedPartition(shiftedColumnName)
      .withPartitionBasedTimestamp(shiftedColumnNameTs, shiftedColumnName)
    val shiftedColumnIndex = expandedInputDf.schema.fieldIndex(shiftedColumnName)
    val shiftedColumnIndexTs = expandedInputDf.schema.fieldIndex(shiftedColumnNameTs)
    val snapshotKeyHashFx = FastHashing.generateKeyBuilder(keyColumns.toArray, expandedInputDf.schema)
    val sawtoothAggregator =
      new SawtoothMutationAggregator(aggregations, Conversions.toChrononSchema(expandedInputDf.schema), resolution)
    val updateFunc = (ir: BatchIr, row: Row) => {
      sawtoothAggregator.update(row.getLong(shiftedColumnIndexTs), ir, Conversions.toChrononRow(row, tsIndex))
      ir
    }

    // end of day IR
    val snapshotByKeys = expandedInputDf.rdd
      .keyBy(row => (snapshotKeyHashFx(row), row.getString(shiftedColumnIndex)))
      .aggregateByKey(sawtoothAggregator.init)(seqOp = updateFunc, combOp = sawtoothAggregator.merge)
      .mapValues(sawtoothAggregator.finalizeSnapshot)

    // Preprocess for mutations: Add a ds of mutation ts column, collect sorted mutations by keys and ds of mutation.
    val mutationsTsIndex = mutationDf.schema.fieldIndex(Constants.MutationTimeColumn)
    val mTsIndex = mutationDf.schema.fieldIndex(Constants.TimeColumn)
    val mutationsReversalIndex = mutationDf.schema.fieldIndex(Constants.ReversalColumn)
    val mutationsHashFx = FastHashing.generateKeyBuilder(keyColumns.toArray, mutationDf.schema)
    val mutationPartitionIndex = mutationDf.schema.fieldIndex(Constants.PartitionColumn)

    //mutations by ds, sorted
    val mutationsByKeys: RDD[((KeyWithHash, String), Array[api.Row])] = mutationDf.rdd
      .map { row =>
        (
          (mutationsHashFx(row), row.getString(mutationPartitionIndex)),
          row
        )
      }
      .groupByKey()
      .mapValues(_.map(Conversions.toChrononRow(_, mTsIndex, mutationsReversalIndex, mutationsTsIndex)).toBuffer
        .sortWith(_.mutationTs < _.mutationTs)
        .toArray)

    // Having the final IR of previous day + mutations (if any), build the array of finalized IR for each query.
    val queryValuesRDD = queriesByKeys
      .leftOuterJoin(snapshotByKeys)
      .leftOuterJoin(mutationsByKeys)
      .map {
        case ((keyWithHash: KeyWithHash, ds: String), ((timeQueries, eodIr), dayMutations)) =>
          val sortedQueries = timeQueries.map { TimeTuple.getTs }
          val finalizedEodIr = eodIr.orNull

          val irs = sawtoothAggregator.lambdaAggregateIrMany(Constants.Partition.epochMillis(ds),
                                                             finalizedEodIr,
                                                             dayMutations.orNull,
                                                             sortedQueries)
          ((keyWithHash, ds), (timeQueries, sortedQueries.indices.map(i => normalizeOrFinalize(irs(i)))))
      }

    val outputRdd = queryValuesRDD
      .flatMap {
        case ((keyHasher, _), (queriesTimeTuple, finalizedAggregations)) =>
          val queries = queriesTimeTuple.map { TimeTuple.getTs }
          queries.indices.map { idx =>
            (keyHasher.data ++ queriesTimeTuple(idx).toArray, finalizedAggregations(idx))
          }
      }
    toDf(outputRdd, Seq(Constants.TimeColumn -> LongType, Constants.PartitionColumn -> StringType))
  }

  // Use another dataframe with the same key columns and time columns to
  // generate aggregates within the Sawtooth of the time points
  // we expect queries to contain the partition column
  def temporalEvents(queriesUnfilteredDf: DataFrame,
                     queryTimeRange: Option[TimeRange] = None,
                     resolution: Resolution = FiveMinuteResolution): DataFrame = {

    val queriesDf = skewFilter
      .map { queriesUnfilteredDf.filter }
      .getOrElse(queriesUnfilteredDf.removeNulls(keyColumns))

    val TimeRange(minQueryTs, maxQueryTs) = queryTimeRange.getOrElse(queriesDf.timeRange)
    val hopsRdd = hopsAggregate(minQueryTs, resolution)

    def headStart(ts: Long): Long = TsUtils.round(ts, resolution.hopSizes.min)
    queriesDf.validateJoinKeys(inputDf, keyColumns)

    val queriesKeyGen = FastHashing.generateKeyBuilder(keyColumns.toArray, queriesDf.schema)
    val queryTsIndex = queriesDf.schema.fieldIndex(Constants.TimeColumn)
    val queryTsType = queriesDf.schema(queryTsIndex).dataType
    assert(queryTsType == LongType, s"ts column needs to be long type, but found $queryTsType")
    val partitionIndex = queriesDf.schema.fieldIndex(Constants.PartitionColumn)

    // group the data to collect all the timestamps by key and headStart
    // key, headStart -> timestamps in [headStart, nextHeadStart)
    // nextHeadStart = headStart + minHopSize
    val queriesByHeadStarts = queriesDf.rdd
      .map { row =>
        val tsVal = row.get(queryTsIndex)
        assert(tsVal != null, "ts column cannot be null in left source or query df")
        val ts = tsVal.asInstanceOf[Long]
        val partition = row.getString(partitionIndex)
        ((queriesKeyGen(row), headStart(ts)), TimeTuple.make(ts, partition))
      }
      .groupByKey()
      .mapValues { _.toArray.uniqSort(TimeTuple) }
    // uniqSort to produce one row per key
    // otherwise the mega-join will produce square number of rows.

    val sawtoothAggregator =
      new SawtoothAggregator(aggregations, selectedSchema, resolution)

    // create the IRs up to minHop accuracy
    val headStartsWithIrs = queriesByHeadStarts.keys
      .groupByKey()
      .leftOuterJoin(hopsRdd)
      .flatMap {
        case (keys, (headStarts, hopsOpt)) =>
          val headStartsArray = headStarts.toArray
          util.Arrays.sort(headStartsArray)
          val headStartIrs = sawtoothAggregator.computeWindows(hopsOpt.orNull, headStartsArray)
          headStartsArray.indices.map { i => (keys, headStartsArray(i)) -> headStartIrs(i) }
      }

    // this can be fused into hop generation
    val inputKeyGen = FastHashing.generateKeyBuilder(keyColumns.toArray, inputDf.schema)
    val minHeadStart = headStart(minQueryTs)
    val eventsByHeadStart = inputDf
      .filter(s"${Constants.TimeColumn} between $minHeadStart and $maxQueryTs")
      .rdd
      .groupBy { (row: Row) => inputKeyGen(row) -> headStart(row.getLong(tsIndex)) }

    // three-way join
    // queries by headStart, events by headStart, IR values as of headStart.
    val outputRdd = queriesByHeadStarts
      .leftOuterJoin(headStartsWithIrs)
      .leftOuterJoin(eventsByHeadStart)
      .flatMap {
        case ((keys: KeyWithHash, _: Long),
              ((queriesWithPartition: Array[TimeTuple.typ], headStartIrOpt: Option[Array[Any]]),
               eventsOpt: Option[Iterable[Row]])) =>
          val inputsIt: Iterator[RowWrapper] = {
            eventsOpt.map(_.map(Conversions.toChrononRow(_, tsIndex)).toIterator).orNull
          }
          val queries = queriesWithPartition.map { TimeTuple.getTs }
          val irs = sawtoothAggregator.cumulate(inputsIt, queries, headStartIrOpt.orNull)
          queries.indices.map { i =>
            (keys.data ++ queriesWithPartition(i).toArray, normalizeOrFinalize(irs(i)))
          }
      }

    toDf(outputRdd, Seq(Constants.TimeColumn -> LongType, Constants.PartitionColumn -> StringType))
  }

  // convert raw data into IRs, collected by hopSizes
  // TODO cache this into a table: interface below
  // Class HopsCacher(keySchema, irSchema, resolution) extends RddCacher[(KeyWithHash, HopsOutput)]
  //  buildTableRow((keyWithHash, hopsOutput)) -> GenericRowWithSchema
  //  buildRddRow(GenericRowWithSchema) -> (keyWithHash, hopsOutput)
  def hopsAggregate(minQueryTs: Long, resolution: Resolution): RDD[(KeyWithHash, HopsAggregator.OutputArrayType)] = {
    val hopsAggregator =
      new HopsAggregator(minQueryTs, aggregations, selectedSchema, resolution)
    val keyBuilder: Row => KeyWithHash =
      FastHashing.generateKeyBuilder(keyColumns.toArray, inputDf.schema)

    inputDf.rdd
      .keyBy(keyBuilder)
      .mapValues(Conversions.toChrononRow(_, tsIndex))
      .aggregateByKey(zeroValue = hopsAggregator.init())(
        seqOp = hopsAggregator.update,
        combOp = hopsAggregator.merge
      )
      .mapValues { hopsAggregator.toTimeSortedArray }
  }

  protected[spark] def toDf(aggregateRdd: RDD[(Array[Any], Array[Any])],
                            additionalFields: Seq[(String, DataType)]): DataFrame = {
    val finalKeySchema = StructType(keySchema ++ additionalFields.map { case (name, typ) => StructField(name, typ) })
    KvRdd(aggregateRdd, finalKeySchema, postAggSchema).toFlatDf
  }

  private def normalizeOrFinalize(ir: Array[Any]): Array[Any] =
    if (finalize) {
      windowAggregator.finalize(ir)
    } else {
      windowAggregator.normalize(ir)
    }

}

// TODO: truncate queryRange for caching
object GroupBy {

  @transient private val logger: Logger = LoggerFactory.getLogger(this.getClass)

  def from(groupByConf: api.GroupBy,
           queryRange: PartitionRange,
           tableUtils: BaseTableUtils,
           bloomMapOpt: Option[Map[String, BloomFilter]] = None,
           skewFilter: Option[String] = None,
           finalize: Boolean = true): GroupBy = {
    logger.info(s"\n----[Processing GroupBy: ${groupByConf.metaData.name}]----")
    val inputDf = groupByConf.sources.asScala
      .map { source =>
        renderDataSourceQuery(source,
                              groupByConf.getKeyColumns.asScala,
                              queryRange,
                              tableUtils,
                              groupByConf.maxWindow,
                              groupByConf.inferredAccuracy)
      }
      .map { tableUtils.sql }
      .reduce { (df1, df2) =>
        // align the columns by name - when one source has select * the ordering might not be aligned
        val columns1 = df1.schema.fields.map(_.name)
        df1.union(df2.selectExpr(columns1: _*))
      }

    def doesNotNeedTime = !Option(groupByConf.getAggregations).exists(_.asScala.needsTimestamp)
    def hasValidTimeColumn = inputDf.schema.find(_.name == Constants.TimeColumn).exists(_.dataType == LongType)
    assert(
      doesNotNeedTime || hasValidTimeColumn,
      s"Time column, ts doesn't exists (or is not a LONG type) for groupBy ${groupByConf.metaData.name}, but you either have windowed aggregation(s) or time based aggregation(s) like: " +
        "first, last, firstK, lastK. \n" +
        "Please note that for the entities case, \"ts\" needs to be explicitly specified in the selects."
    )
    val logPrefix = s"gb:{${groupByConf.metaData.name}}:"
    val keyColumns = groupByConf.getKeyColumns.asScala
    val skewFilteredDf = skewFilter
      .map { sf =>
        logger.info(s"$logPrefix filtering using skew filter:\n    $sf")
        val filtered = inputDf.filter(sf)
        filtered
      }
      .getOrElse(inputDf)

    val processedInputDf = bloomMapOpt.map { skewFilteredDf.filterBloom }.getOrElse { skewFilteredDf }

    // at-least one of the keys should be present in the row.
    val nullFilterClause = groupByConf.keyColumns.asScala.map(key => s"($key IS NOT NULL)").mkString(" OR ")
    val nullFiltered = processedInputDf.filter(nullFilterClause)

    // Generate mutation Df if required, align the columns with inputDf so no additional schema is needed by aggregator.
    val mutationSources = groupByConf.sources.asScala.filter { _.isSetEntities }
    val mutationsColumnOrder = inputDf.columns ++ Constants.MutationFields.map(_.name)
    val mutationDf =
      if (groupByConf.inferredAccuracy == api.Accuracy.TEMPORAL && mutationSources.nonEmpty) {
        val mutationDf = mutationSources
          .map {
            renderDataSourceQuery(_,
                                  groupByConf.getKeyColumns.asScala,
                                  queryRange.shift(1),
                                  tableUtils,
                                  groupByConf.maxWindow,
                                  groupByConf.inferredAccuracy,
                                  mutations = true)
          }
          .map {
            tableUtils.sql
          }
          .reduce { (df1, df2) =>
            val columns1 = df1.schema.fields.map(_.name)
            df1.union(df2.selectExpr(columns1: _*))
          }
          .selectExpr(mutationsColumnOrder: _*)
        bloomMapOpt.map { mutationDf.filterBloom }.getOrElse { mutationDf }
      } else null

    new GroupBy(Option(groupByConf.getAggregations).map(_.asScala).orNull,
                keyColumns,
                nullFiltered,
                Option(mutationDf).orNull,
                finalize = finalize)
  }

  def renderDataSourceQuery(source: api.Source,
                            keys: Seq[String],
                            queryRange: PartitionRange,
                            tableUtils: BaseTableUtils,
                            window: Option[api.Window],
                            accuracy: api.Accuracy,
                            mutations: Boolean = false): String = {
    val PartitionRange(queryStart, queryEnd) = queryRange

    val effectiveEnd = (Option(queryRange.end) ++ Option(source.query.endPartition))
      .reduceLeftOption(Ordering[String].min)
      .orNull

    // Need to use a case class here to allow null matching
    case class SourceDataProfile(earliestRequired: String, earliestPresent: String, latestAllowed: String)

    val dataProfile: SourceDataProfile = source.dataModel match {
      case Entities => SourceDataProfile(queryStart, source.query.startPartition, effectiveEnd)
      case Events =>
        if (Option(source.getEvents.isCumulative).getOrElse(false)) {
          lazy val latestAvailable: Option[String] = tableUtils.lastAvailablePartition(source.table)
          val latestValid: String = Option(source.query.endPartition).getOrElse(latestAvailable.orNull)
          SourceDataProfile(latestValid, latestValid, latestValid)
        } else {
          val minQuery = Constants.Partition.before(queryStart)
          val windowStart: String = window.map(Constants.Partition.minus(minQuery, _)).orNull
          lazy val firstAvailable = tableUtils.firstAvailablePartition(source.table)
          val sourceStart = Option(source.query.startPartition).getOrElse(firstAvailable.orNull)
          SourceDataProfile(windowStart, sourceStart, effectiveEnd)
        }
    }

    val sourceRange = PartitionRange(dataProfile.earliestPresent, dataProfile.latestAllowed)
    val queryableDataRange = PartitionRange(dataProfile.earliestRequired, Seq(queryEnd, dataProfile.latestAllowed).max)
    val intersectedRange = sourceRange.intersect(queryableDataRange)
    // CumulativeEvent => (latestValid, queryEnd) , when endPartition is null
    var metaColumns: Map[String, String] = Map(Constants.PartitionColumn -> null)
    if (mutations) {
      metaColumns ++= Map(
        Constants.ReversalColumn -> source.query.reversalColumn,
        Constants.MutationTimeColumn -> source.query.mutationTimeColumn
      )
    }
    val timeMapping = if (source.dataModel == Entities) {
      Option(source.query.timeColumn).map(Constants.TimeColumn -> _)
    } else {
      if (accuracy == api.Accuracy.TEMPORAL) {
        Some(Constants.TimeColumn -> source.query.timeColumn)
      } else {
        val dsBasedTimestamp = // 1 millisecond before ds + 1
          s"(((UNIX_TIMESTAMP(${Constants.PartitionColumn}, '${Constants.Partition.format}') + 86400) * 1000) - 1)"
        Some(Constants.TimeColumn -> Option(source.query.timeColumn).getOrElse(dsBasedTimestamp))
      }
    }
    metaColumns ++= timeMapping

    logger.info(s"""
         |Rendering source query:
         |   query range: $queryRange
         |   query window: $window
         |   source table: ${source.table}
         |   source data range: $sourceRange
         |   source start/end: ${source.query.startPartition}/${source.query.endPartition}
         |   source data model: ${source.dataModel}
         |   queryable data range: $queryableDataRange
         |   intersected/effective scan range: $intersectedRange
         |   metaColumns: $metaColumns
         |""".stripMargin)

    val query = api.QueryUtils.build(
      Option(source.query.selects).map(_.asScala.toMap).orNull,
      if (mutations) source.getEntities.mutationTable.cleanSpec else source.table,
      Option(source.query.wheres).map(_.asScala).getOrElse(Seq.empty[String]) ++ intersectedRange.whereClauses,
      metaColumns ++ keys.map(_ -> null)
    )
    query
  }

  def computeBackfill(groupByConf: api.GroupBy,
                      endPartition: String,
                      tableUtils: BaseTableUtils,
                      stepDays: Option[Int] = None): Unit = {
    assert(
      groupByConf.backfillStartDate != null,
      s"GroupBy:${groupByConf.metaData.name} has null backfillStartDate. This needs to be set for offline backfilling.")
    groupByConf.setups.foreach(tableUtils.sql)
    val outputTable = groupByConf.metaData.outputTable
    val tableProps = Option(groupByConf.metaData.tableProperties)
      .map(_.asScala.toMap)
      .orNull
    val inputTables = groupByConf.getSources.asScala.map(_.table)
    val groupByUnfilledRangesOpt =
      tableUtils.unfilledRanges(outputTable, PartitionRange(groupByConf.backfillStartDate, endPartition), Some(inputTables))
    if (groupByUnfilledRangesOpt.isEmpty) {
      logger.info(s"""Nothing to backfill for $outputTable - given
           |endPartition of $endPartition
           |backfill start of ${groupByConf.backfillStartDate}
           |Exiting...""".stripMargin)
      return
    }
    val groupByUnfilledRanges = groupByUnfilledRangesOpt.get
    logger.info(s"group by unfilled ranges: $groupByUnfilledRanges")
    val exceptions = mutable.Buffer.empty[String]
    groupByUnfilledRanges.foreach {
      case groupByUnfilledRange =>
        try {
          val stepRanges = stepDays.map(groupByUnfilledRange.steps).getOrElse(Seq(groupByUnfilledRange))
          logger.info(s"Group By ranges to compute: ${stepRanges.map { _.toString }.pretty}")
          stepRanges.zipWithIndex.foreach {
            case (range, index) =>
              logger.info(s"Computing group by for range: $range [${index + 1}/${stepRanges.size}]")
              val groupByBackfill = from(groupByConf, range, tableUtils)
              (groupByConf.dataModel match {
                // group by backfills have to be snapshot only
                case Entities => groupByBackfill.snapshotEntities
                case Events   => groupByBackfill.snapshotEvents(range)
              }).save(outputTable, tableProps)
              logger.info(s"Wrote to table $outputTable, into partitions: $range")
          }
          logger.info(s"Wrote to table $outputTable for range: $groupByUnfilledRange")
        } catch {
          case err: Throwable =>
            exceptions += s"Error handling range ${groupByUnfilledRange} : ${err.getMessage}"
        }
    }
    if (exceptions.nonEmpty) {
      throw new RuntimeException(exceptions.mkString("\n"))
    }
  }

  def main(args: Array[String]): Unit = {
    val parsedArgs = new Args(args)
    parsedArgs.verify()
    println(s"Parsed Args: $parsedArgs")
    val groupByConf = parsedArgs.parseConf[api.GroupBy]
    computeBackfill(
      groupByConf,
      parsedArgs.endDate(),
      TableUtils(SparkSessionBuilder.build(s"groupBy_${groupByConf.metaData.name}_backfill")),
      parsedArgs.stepDays.toOption
    )
  }
}
