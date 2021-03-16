package ai.zipline.spark

import ai.zipline.aggregator.base.TimeTuple
import ai.zipline.aggregator.row.RowAggregator
import ai.zipline.aggregator.windowing._
import ai.zipline.api.DataModel.{Entities, Events}
import ai.zipline.api.Extensions._
import ai.zipline.api.{Aggregation, Constants, QueryUtils, Source, ThriftJsonDecoder, Window, GroupBy => GroupByConf}
import ai.zipline.spark.Extensions._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.util.sketch.BloomFilter
import org.rogach.scallop._

import java.util
import scala.collection.JavaConverters._

class GroupBy(aggregations: Seq[Aggregation],
              keyColumns: Seq[String],
              inputDf: DataFrame,
              skewFilter: Option[String] = None,
              finalize: Boolean = true)
    extends Serializable {

  protected[spark] val tsIndex: Int = inputDf.schema.fieldNames.indexOf(Constants.TimeColumn)
  private val ziplineSchema = Conversions.toZiplineSchema(inputDf.schema)
  private lazy val valueZiplineSchema = if (finalize) windowAggregator.outputSchema else windowAggregator.irSchema
  @transient
  protected[spark] lazy val windowAggregator: RowAggregator =
    new RowAggregator(ziplineSchema, aggregations.flatMap(_.unpack))

  def snapshotEntities: DataFrame = {
    if (aggregations == null || aggregations.isEmpty) return inputDf //data is pre-aggregated

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

    val outputRdd = preppedInputDf.rdd
      .keyBy(keyBuilder)
      .aggregateByKey(windowAggregator.init)(
        seqOp = irUpdateFunc,
        combOp = {
          case (ir1, ir2) =>
            windowAggregator.merge(ir1, ir2)
            ir1
        }
      )
      .map { case (keyWithHash, ir) => keyWithHash.data -> ir }
    toDataFrame(outputRdd, Seq((Constants.PartitionColumn, StringType)))
  }
  // Calculate snapshot accurate windows for ALL keys at pre-defined "endTimes"
  // At this time, we hardcode the resolution to Daily, but it is straight forward to support
  // hourly resolution.
  def snapshotEvents(partitionRange: PartitionRange, resolution: Resolution = DailyResolution): DataFrame = {
    val endTimes: Array[Long] = partitionRange.toTimePoints
    val sawtoothAggregator = new SawtoothAggregator(aggregations, ziplineSchema, resolution)

    val outputRdd = hopsAggregate(endTimes.min, resolution)
      .flatMap {
        case (keys, hopsArrays) =>
          val irs = sawtoothAggregator.computeWindows(hopsArrays, endTimes)
          irs.indices.map { i =>
            (keys.data :+ Constants.Partition.at(endTimes(i)), irs(i))
          }
      }
    toDataFrame(outputRdd, Seq((Constants.PartitionColumn, StringType)))
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
      new SawtoothAggregator(aggregations, ziplineSchema, resolution)

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
          queries.indices.map { i => (keys.data ++ queriesWithPartition(i).toArray, irs(i)) }
      }
    toDataFrame(outputRdd, Seq((Constants.TimeColumn, LongType), (Constants.PartitionColumn, StringType)))
  }

  // convert raw data into IRs, collected by hopSizes
  // TODO cache this into a table: interface below
  // Class HopsCacher(keySchema, irSchema, resolution) extends RddCacher[(KeyWithHash, HopsOutput)]
  //  buildTableRow((keyWithHash, hopsOutput)) -> GenericRowWithSchema
  //  buildRddRow(GenericRowWithSchema) -> (keyWithHash, hopsOutput)
  def hopsAggregate(minQueryTs: Long, resolution: Resolution): RDD[(KeyWithHash, HopsAggregator.OutputArrayType)] = {
    val hopsAggregator =
      new HopsAggregator(minQueryTs, aggregations, ziplineSchema, resolution)
    val keyBuilder: Row => KeyWithHash =
      FastHashing.generateKeyBuilder(keyColumns.toArray, inputDf.schema)

    inputDf.rdd
      .keyBy {
        keyBuilder
      }
      .aggregateByKey(zeroValue = hopsAggregator.init())(
        seqOp = {
          case (ir: HopsAggregator.IrMapType, input: Row) =>
            hopsAggregator.update(ir, Conversions.toZiplineRow(input, tsIndex))
        },
        combOp = {
          case (ir1: HopsAggregator.IrMapType, ir2: HopsAggregator.IrMapType) => hopsAggregator.merge(ir1, ir2)
        }
      )
      .mapValues {
        hopsAggregator.toTimeSortedArray
      }
  }

  protected[spark] def toDataFrame(aggregateRdd: RDD[(Array[Any], Array[Any])],
                                   additionalFields: Seq[(String, DataType)]): DataFrame = {
    val flattened = aggregateRdd.map { case (keys, ir) => makeRow(keys, ir) }
    inputDf.sparkSession.createDataFrame(flattened, outputSchema(additionalFields))
  }

  private def sparkify(ir: Array[Any]): Array[Any] =
    if (finalize) {
      Conversions.fromZiplineRow(windowAggregator.finalize(ir), valueZiplineSchema)
    } else {
      Conversions.fromZiplineRow(windowAggregator.denormalize(ir), valueZiplineSchema)
    }

  protected[spark] def makeRow(keys: Array[Any], values: Array[Any]): Row = {
    val result = new Array[Any](keys.length + values.length)
    System.arraycopy(keys, 0, result, 0, keys.length)
    System.arraycopy(sparkify(values), 0, result, keys.length, values.length)
    new GenericRow(result)
  }

  protected[spark] def outputSchema(additionalFields: Seq[(String, DataType)]): StructType = {
    val keyIndices = keyColumns.map(inputDf.schema.fieldIndex).toArray
    val keySchema: Array[StructField] = keyIndices.map(inputDf.schema)
    val valueSchema: Array[StructField] = Conversions.fromZiplineSchema(valueZiplineSchema).fields
    val additionalSchema =
      additionalFields.map(field => StructField(field._1, field._2))
    StructType((keySchema ++ additionalSchema) ++ valueSchema)
  }
}

// TODO: truncate queryRange for caching
object GroupBy {

  def from(groupByConf: GroupByConf,
           queryRange: PartitionRange,
           tableUtils: TableUtils,
           bloomMap: Map[String, BloomFilter],
           skewFilter: Option[String] = None): GroupBy = {
    println(s"\n----[Processing GroupBy: ${groupByConf.metaData.name}]----")
    val inputDf = groupByConf.sources.asScala
      .map {
        renderDataSourceQuery(_, groupByConf.getKeyColumns.asScala, queryRange, groupByConf.maxWindow)
      }
      .map { tableUtils.sql }
      .reduce(mergeDataFrame)

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

    val bloomFilteredDf = skewFilteredDf.filterBloom(bloomMap)
    println(s"$logPrefix bloom filtered data count: ${bloomFilteredDf.count()}")
    println(s"\nGroup-by raw data schema:")
    println(bloomFilteredDf.schema.pretty)

    new GroupBy(groupByConf.getAggregations.asScala, keyColumns, bloomFilteredDf)
  }

  def renderDataSourceQuery(source: Source,
                            keys: Seq[String],
                            queryRange: PartitionRange,
                            window: Option[Window]): String = {
    val PartitionRange(queryStart, queryEnd) = queryRange
    val scanStart = source.dataModel match {
      case Entities => queryStart
      case Events   => window.map(Constants.Partition.minus(queryStart, _)).orNull
    }

    val sourceRange = PartitionRange(source.query.startPartition, source.query.endPartition)
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

  def computeGroupBy(groupByConf: GroupByConf, endPartition: String, tableUtils: TableUtils, stepDays: Option[Int] = None): DataFrame = {
    val outputTable = s"${groupByConf.metaData.outputNamespace}.${groupByConf.metaData.cleanName}"
    val tableProps = Option(groupByConf.metaData.tableProperties)
      .map(_.asScala.toMap)
      .orNull

    val inputTables = groupByConf.sources.asScala.map(_.table)
    val groupByUnfilledRange: PartitionRange = tableUtils.unfilledRange(
      outputTable,
      PartitionRange(groupByConf.sources.asScala.flatMap(Option(_.query.startPartition)).minOption.orNull, endPartition),
      inputTables)
    println(s"group by unfilled range: $groupByUnfilledRange")

    val stepRanges = stepDays.map(groupByUnfilledRange.steps).getOrElse(Seq(groupByUnfilledRange))
    println(s"Group By ranges to compute: ${
      stepRanges.map {
        _.toString
      }.pretty
    }")
    stepRanges.zipWithIndex.foreach {
      case (range, index) =>
        val progress = s"| [${index + 1}/${stepRanges.size}]"
        println(s"Computing group by for range: $range  $progress")
        // todo: support skew keys filter
        val groupByBackfill = from(groupByConf, groupByUnfilledRange, tableUtils, Map.empty)
        (groupByConf.dataModel match {
          // group by backfills have to be snapshot only
          case Entities => groupByBackfill.snapshotEntities
          case Events => groupByBackfill.snapshotEvents(groupByUnfilledRange)
        }).save(outputTable, tableProps)
        println(s"Wrote to table $outputTable, into partitions: $range $progress")
    }
    println(s"Wrote to table $outputTable, into partitions: $groupByUnfilledRange")
    tableUtils.sql(groupByUnfilledRange.genScanQuery(null, outputTable))
  }

  class ParsedArgs(args: Seq[String]) extends ScallopConf(args) {
    val confPath: ScallopOption[String] = opt[String](required = true)
    val endDate: ScallopOption[String] = opt[String](required = true)
    // todo: is stepDays needed here
    val stepDays: ScallopOption[Int] = opt[Int](required = false)
    verify()
  }

  def main(args: Array[String]): Unit = {
    // args = conf path, end date, output namespace
    val parsedArgs = new ParsedArgs(args)
    println(s"Parsed Args: $parsedArgs")
    val groupByConf =
      ThriftJsonDecoder.fromJsonFile[GroupByConf](parsedArgs.confPath(), check = true, clazz = classOf[GroupByConf])

    computeGroupBy(
      groupByConf,
      parsedArgs.endDate(),
      TableUtils(SparkSessionBuilder.build(s"groupBy_${groupByConf.metaData.name}", local = false)),
      parsedArgs.stepDays.toOption
    )
  }
}
