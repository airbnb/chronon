package ai.zipline.spark

import ai.zipline.aggregator.base.TimeTuple
import ai.zipline.aggregator.row.RowAggregator
import ai.zipline.aggregator.windowing._
import ai.zipline.api.DataModel.{Entities, Events}
import ai.zipline.api.Extensions._
import ai.zipline.api.{Aggregation, Constants, QueryUtils, Source, ThriftJsonCodec, Window, GroupBy => GroupByConf}
import ai.zipline.spark.Extensions._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.util.sketch.BloomFilter
import org.rogach.scallop._

import java.util
import scala.collection.JavaConverters._

class GroupBy(val aggregations: Seq[Aggregation],
              val keyColumns: Seq[String],
              val inputDf: DataFrame,
              skewFilter: Option[String] = None,
              finalize: Boolean = true)
    extends Serializable {

  protected[spark] val tsIndex: Int = inputDf.schema.fieldNames.indexOf(Constants.TimeColumn)
  protected val inputZiplineSchema = Conversions.toZiplineSchema(inputDf.schema)

  val keySchema: StructType = StructType(keyColumns.map(inputDf.schema.apply).toArray)
  implicit val sparkSession = inputDf.sparkSession
  // distinct inputs to aggregations - post projection types that needs to match with
  // streaming's post projection types etc.,
  val preAggSchema = if (aggregations != null) {
    StructType(aggregations.map(_.inputColumn).distinct.map(inputDf.schema.apply))
  } else {
    val values = inputDf.schema.map(_.name).filterNot((keyColumns ++ Constants.ReservedColumns).contains)
    val valuesIndices = values.map(inputDf.schema.fieldIndex).toArray
    StructType(valuesIndices.map(inputDf.schema))
  }

  protected lazy val valueZiplineSchema = if (finalize) windowAggregator.outputSchema else windowAggregator.irSchema
  lazy val postAggSchema = Conversions.fromZiplineSchema(valueZiplineSchema)

  @transient
  protected[spark] lazy val windowAggregator: RowAggregator =
    new RowAggregator(inputZiplineSchema, aggregations.flatMap(_.unpack))

  def snapshotEntitiesBase: RDD[(Array[Any], Array[Any])] = {
    val keyBuilder = FastHashing.generateKeyBuilder((keyColumns :+ Constants.PartitionColumn).toArray, inputDf.schema)
    val (preppedInputDf, irUpdateFunc) = if (aggregations.hasWindows) {
      val partitionTs = "ds_ts"
      val inputWithPartitionTs = inputDf.withPartitionBasedTimestamp(partitionTs)
      val partitionTsIndex = inputWithPartitionTs.schema.fieldIndex(partitionTs)
      val updateFunc = (ir: Array[Any], row: Row) => {
        // update when ts < tsOf(ds)
        windowAggregator.updateWindowed(ir, Conversions.toZiplineRow(row, tsIndex), row.getLong(partitionTsIndex))
        ir
      }
      inputWithPartitionTs -> updateFunc
    } else {
      val updateFunc = (ir: Array[Any], row: Row) => {
        windowAggregator.update(ir, Conversions.toZiplineRow(row, tsIndex))
        ir
      }
      inputDf -> updateFunc
    }

    preppedInputDf.rdd
      .keyBy(keyBuilder)
      .aggregateByKey(windowAggregator.init)(seqOp = irUpdateFunc, combOp = windowAggregator.merge)
      .map { case (keyWithHash, ir) => keyWithHash.data -> sparkify(ir) }
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
    val sawtoothAggregator = new SawtoothAggregator(aggregations, inputZiplineSchema, resolution)
    hopsAggregate(endTimes.min, resolution)
      .flatMap {
        case (keys, hopsArrays) =>
          val irs = sawtoothAggregator.computeWindows(hopsArrays, endTimes)
          irs.indices.map { i =>
            (keys.data :+ Constants.Partition.at(endTimes(i)), sparkify(irs(i)))
          }
      }
  }
  // Calculate snapshot accurate windows for ALL keys at pre-defined "endTimes"
  // At this time, we hardcode the resolution to Daily, but it is straight forward to support
  // hourly resolution.
  def snapshotEvents(partitionRange: PartitionRange): DataFrame =
    toDf(snapshotEventsBase(partitionRange), Seq((Constants.PartitionColumn, StringType)))

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
    val partitionIndex = queriesDf.schema.fieldIndex(Constants.PartitionColumn)

    // group the data to collect all the timestamps by key and headStart
    // key, headStart -> timestamps in [headStart, nextHeadStart)
    // nextHeadStart = headStart + minHopSize
    val queriesByHeadStarts = queriesDf.rdd
      .map { row =>
        val ts = row.getLong(queryTsIndex)
        val partition = row.getString(partitionIndex)
        ((queriesKeyGen(row), headStart(ts)), TimeTuple.make(ts, partition))
      }
      .groupByKey()
      .mapValues { _.toArray.uniqSort(TimeTuple) }
    // uniqSort to produce one row per key
    // otherwise we the mega-join will produce square number of rows.

    val sawtoothAggregator =
      new SawtoothAggregator(aggregations, inputZiplineSchema, resolution)

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
          val inputsIt: Iterator[ArrayRow] = {
            eventsOpt.map(_.map(Conversions.toZiplineRow(_, tsIndex)).toIterator).orNull
          }
          val queries = queriesWithPartition.map { TimeTuple.getTs }
          val irs = sawtoothAggregator.cumulate(inputsIt, queries, headStartIrOpt.orNull)
          queries.indices.map { i => (keys.data ++ queriesWithPartition(i).toArray, sparkify(irs(i))) }
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
      new HopsAggregator(minQueryTs, aggregations, inputZiplineSchema, resolution)
    val keyBuilder: Row => KeyWithHash =
      FastHashing.generateKeyBuilder(keyColumns.toArray, inputDf.schema)

    inputDf.rdd
      .keyBy(keyBuilder)
      .mapValues(Conversions.toZiplineRow(_, tsIndex))
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

  private def sparkify(ir: Array[Any]): Array[Any] =
    if (finalize) {
      Conversions.fromZiplineRow(windowAggregator.finalize(ir), valueZiplineSchema)
    } else {
      Conversions.fromZiplineRow(windowAggregator.denormalize(ir), valueZiplineSchema)
    }

}

// TODO: truncate queryRange for caching
object GroupBy {

  def from(groupByConf: GroupByConf,
           queryRange: PartitionRange,
           tableUtils: TableUtils,
           bloomMapOpt: Option[Map[String, BloomFilter]] = None,
           skewFilter: Option[String] = None): GroupBy = {
    println(s"\n----[Processing GroupBy: ${groupByConf.metaData.name}]----")
    val inputDf = groupByConf.sources.asScala
      .map {
        renderDataSourceQuery(_, groupByConf.getKeyColumns.asScala, queryRange, tableUtils, groupByConf.maxWindow)
      }
      .map { tableUtils.sql }
      .reduce { (df1, df2) =>
        // align the columns by name - when one source has select * the ordering might not be aligned
        val columns1 = df1.schema.fields.map(_.name)
        df1.union(df2.selectExpr(columns1: _*))
      }

    assert(
      !Option(groupByConf.getAggregations).exists(_.asScala.needsTimestamp) || inputDf.schema.names
        .contains(Constants.TimeColumn),
      "Time column, \"ts\" doesn't exists, but you either have windowed aggregation(s) or time based aggregation(s) like: " +
        "first, last, firstK, lastK. \n" +
        "Please note that for the entities case, \"ts\" needs to be explicitly specified in the selects."
    )

    val logPrefix = s"gb:{${groupByConf.metaData.name}}:"
    val keyColumns = groupByConf.getKeyColumns.asScala
    println(s"$logPrefix input data count: ${inputDf.count()}")
    val skewFilteredDf = skewFilter
      .map { sf =>
        println(s"$logPrefix filtering using skew filter:\n    $sf")
        val filtered = inputDf.filter(sf)
        println(s"$logPrefix post skew filter count: ${filtered.count()}")
        filtered
      }
      .getOrElse(inputDf)

    val processedInputDf = bloomMapOpt
      .map { bloomMap =>
        val bloomFilteredDf = skewFilteredDf.filterBloom(bloomMap)
        println(s"$logPrefix bloom filtered data count: ${bloomFilteredDf.count()}")
        bloomFilteredDf
      }
      .getOrElse { skewFilteredDf }
    new GroupBy(Option(groupByConf.getAggregations).map(_.asScala).orNull, keyColumns, processedInputDf)
  }

  def renderDataSourceQuery(source: Source,
                            keys: Seq[String],
                            queryRange: PartitionRange,
                            tableUtils: TableUtils,
                            window: Option[Window]): String = {
    val PartitionRange(queryStart, queryEnd) = queryRange
    val (scanStart, startPartition) = source.dataModel match {
      case Entities => (queryStart, source.query.startPartition)
      case Events =>
        val windowStart = window.map(Constants.Partition.minus(queryStart, _)).orNull
        val sourceStart = (Option(source.query.startPartition) ++ tableUtils.firstAvailablePartition(source.table))
          .reduceLeftOption(Ordering[String].min)
          .orNull
        (windowStart, sourceStart)
    }

    val sourceRange = PartitionRange(startPartition, source.query.endPartition)
    val queryableDataRange = PartitionRange(scanStart, queryEnd)
    val intersectedRange = sourceRange.intersect(queryableDataRange)

    val metaColumns = source.dataModel match {
      case Entities =>
        Map(Constants.PartitionColumn -> null) ++ Option(source.query.timeColumn)
          .map(Constants.TimeColumn -> _)
      case Events =>
        Map(Constants.TimeColumn -> source.query.timeColumn, Constants.PartitionColumn -> null)
    }

    println(s"""
         |Rendering source query:
         |   query range: $queryRange
         |   query window: $window
         |   source table: ${source.table}
         |   source data range: $sourceRange
         |   source data model: ${source.dataModel}
         |   queryable data range: $queryableDataRange
         |   intersected/effective scan range: $intersectedRange 
         |   metaColumns: $metaColumns
         |""".stripMargin)

    val query = QueryUtils.build(
      Option(source.query.selects).map(_.asScala.toMap).orNull,
      source.table,
      Option(source.query.wheres).map(_.asScala).getOrElse(Seq.empty[String]) ++ intersectedRange.whereClauses,
      metaColumns ++ keys.map(_ -> null)
    )

    println(s"""
        |Rendered query:
        |$query
        |""".stripMargin)
    query
  }

  def computeBackfill(groupByConf: GroupByConf, endPartition: String, tableUtils: TableUtils): Unit = {
    val sources = groupByConf.sources.asScala
    groupByConf.setups.foreach(tableUtils.sql)
    val outputTable = s"${groupByConf.metaData.outputNamespace}.${groupByConf.metaData.cleanName}"
    val tableProps = Option(groupByConf.metaData.tableProperties)
      .map(_.asScala.toMap)
      .orNull
    val inputTables = sources.map(_.table)
    val minStartPartition = sources.map(src => Option(src.query.startPartition)).min.orNull
    val groupByUnfilledRange: PartitionRange =
      tableUtils.unfilledRange(outputTable, PartitionRange(minStartPartition, endPartition), inputTables)
    println(s"group by unfilled range: $groupByUnfilledRange")

    val groupByBackfill = from(groupByConf, groupByUnfilledRange, tableUtils)
    (groupByConf.dataModel match {
      // group by backfills have to be snapshot only
      case Entities => groupByBackfill.snapshotEntities
      case Events   => groupByBackfill.snapshotEvents(groupByUnfilledRange)
    }).save(outputTable, tableProps)
    println(s"Wrote to table $outputTable for range: $groupByUnfilledRange")
  }

  // args = conf path, end date, output namespace
  class ParsedArgs(args: Seq[String]) extends ScallopConf(args) {
    val confPath: ScallopOption[String] =
      opt[String](required = true, descr = "Absolute Path to TSimpleJson serialized GroupBy object")
    val endDate: ScallopOption[String] =
      opt[String](required = true, descr = "End date / end partition for computing groupBy")
    verify()
    def groupByConf =
      ThriftJsonCodec.fromJsonFile[GroupByConf](confPath(), check = true, clazz = classOf[GroupByConf])
  }

  def main(args: Array[String]): Unit = {
    val parsedArgs = new ParsedArgs(args)
    println(s"Parsed Args: $parsedArgs")
    computeBackfill(
      parsedArgs.groupByConf,
      parsedArgs.endDate(),
      TableUtils(SparkSessionBuilder.build(s"groupBy_${parsedArgs.groupByConf.metaData.name}_backfill"))
    )
  }
}
