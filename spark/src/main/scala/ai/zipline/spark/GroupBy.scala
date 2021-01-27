package ai.zipline.spark

import java.util

import ai.zipline.aggregator.row.RowAggregator
import ai.zipline.aggregator.windowing._
import ai.zipline.api.DataModel.{Entities, Events}
import ai.zipline.api.Extensions._
import ai.zipline.api.{Aggregation, Constants, QueryUtils, Source, Window, GroupBy => GroupByConf}
import ai.zipline.spark.Extensions._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.apache.spark.sql.types.{DataType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row}

import scala.collection.JavaConverters._

class GroupBy(aggregations: Seq[Aggregation], keyColumns: Seq[String], inputDf: DataFrame, finalize: Boolean = true)
    extends Serializable {

  protected[spark] val tsIndex: Int = inputDf.schema.fieldNames.indexOf(Constants.TimeColumn)
  private val ziplineSchema = Conversions.toZiplineSchema(inputDf.schema)
  private val valueZiplineSchema = if (finalize) windowAggregator.outputSchema else windowAggregator.irSchema
  @transient
  protected[spark] lazy val windowAggregator: RowAggregator =
    new RowAggregator(ziplineSchema, aggregations.flatMap(_.unpack))

  def preAggregated: DataFrame = inputDf

  def snapshotEntities: DataFrame = {

    val keyBuilder = FastHashing.generateKeyBuilder((keyColumns :+ Constants.PartitionColumn).toArray, inputDf.schema)

    val (preppedInputDf, irUpdateFunc) = if (aggregations.hasWindows) {
      val partitionTs = "ds_ts"
      val inputWithPartitionTs = inputDf.withPartitionBasedTimestamp(partitionTs)
      val partitionTsIndex = inputWithPartitionTs.schema.fieldIndex(partitionTs)
      val updateFunc = (ir: Array[Any], row: Row) => {
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
    toDataFrame(outputRdd, Constants.PartitionColumn, Some(StringType))
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
    toDataFrame(outputRdd, Constants.PartitionColumn, Some(StringType))
  }

  // Use another dataframe with the same key columns and time columns to
  // generate aggregates within the Sawtooth of the time points
  def temporalEvents(queriesDf: DataFrame,
                     queryTimeRange: Option[TimeRange] = None,
                     resolution: Resolution = FiveMinuteResolution): DataFrame = {
    val TimeRange(minQueryTs, maxQueryTs) = queryTimeRange.getOrElse(queriesDf.timeRange)
    val hopsRdd = hopsAggregate(minQueryTs, resolution)

    def headStart(ts: Long): Long = TsUtils.round(ts, resolution.hopSizes.min)
    queriesDf.validateJoinKeys(inputDf, keyColumns)

    val queriesKeyGen = FastHashing.generateKeyBuilder(keyColumns.toArray, queriesDf.schema)
    val queryTsIndex = queriesDf.schema.fieldIndex(Constants.TimeColumn)
    // group the data to collect all the timestamps by key and headStart
    // key, headStart -> timestamps in [headStart, nextHeadStart)
    // nextHeadStart = headStart + minHopSize
    val queriesByHeadStarts = queriesDf.rdd
      .map { row =>
        val ts = row.getLong(queryTsIndex)
        ((queriesKeyGen(row), headStart(ts)), ts)
      }
      .groupByKey()
      .mapValues { queryTimesItr =>
        val sorted = queryTimesItr.toArray.distinct
        util.Arrays.sort(sorted)
        sorted
      }

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
              ((queries: Array[Long], headStartIrOpt: Option[Array[Any]]), eventsOpt: Option[Iterable[Row]])) =>
          val inputsIt: Iterator[ArrayRow] =
            eventsOpt.map(_.map(Conversions.toZiplineRow(_, tsIndex)).toIterator).orNull
          val irs = sawtoothAggregator.cumulateUnsorted(inputsIt, queries, headStartIrOpt.orNull)
          queries.indices.map { i => (keys.data :+ queries(i), irs(i)) }
      }
    toDataFrame(outputRdd, Constants.TimeColumn)
  }

  // convert raw data into IRs, collected by hopSizes
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
                                   additionalKey: String,
                                   additionalKeyType: Option[DataType] = None): DataFrame = {
    val flattened = aggregateRdd.map { case (keys, ir) => makeRow(keys, ir) }
    inputDf.sparkSession.createDataFrame(flattened, outputSchema(additionalKey, additionalKeyType))
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

  protected[spark] def outputSchema(additionalKey: String, additionKeyType: Option[DataType]): StructType = {
    val keyIndices = keyColumns.map(inputDf.schema.fieldIndex).toArray
    val keySchema: Array[StructField] = keyIndices.map(inputDf.schema)
    val valueSchema: Array[StructField] = Conversions.fromZiplineSchema(valueZiplineSchema).fields
    val additionalKeySchema =
      StructField(additionalKey, additionKeyType.getOrElse(inputDf.typeOf(additionalKey)))
    StructType((keySchema :+ additionalKeySchema) ++ valueSchema)
  }
}

// TODO: truncate queryRange for caching
object GroupBy {

  def from(groupByConf: GroupByConf, queryRange: PartitionRange, tableUtils: TableUtils): GroupBy = {
    val inputDf = groupByConf.sources.asScala
      .flatMap {
        renderDataSourceQuery(_, groupByConf.getKeyColumns.asScala, queryRange, groupByConf.maxWindow)
      }
      .map { query =>
        val df = tableUtils.sql(query)
        println(s"----[GroupBy data source schema: ${groupByConf.metaData.name}]----")
        println(df.schema.pretty)
        df
      }
      .reduce { (df1, df2) =>
        // align the columns by name - when one source has select * the ordering might not be aligned
        val columns1 = df1.schema.fields.map(_.name)
        df1.union(df2.selectExpr(columns1: _*))
      }

    assert(
      !groupByConf.getAggregations.asScala.needsTimestamp || inputDf.schema.names.contains(Constants.TimeColumn),
      "Time column, \"ts\" doesn't exists, but you either have windowed aggregation(s) or time based aggregation(s) like: " +
        "first, last, firstK, lastK. \n" +
        "Please note that for the entities case, \"ts\" needs to be explicitly specified in the selects."
    )
    println(s"\n----[GroupBy input data sample: ${groupByConf.metaData.name}]----")
    inputDf.show()
    println(inputDf.schema.fields.map(field => s"${field.name} -> ${field.dataType.simpleString}").mkString(", "))
    println("----[End input data sample]----")

    new GroupBy(groupByConf.getAggregations.asScala, groupByConf.getKeyColumns.asScala, inputDf)
  }

  def renderDataSourceQuery(source: Source,
                            keys: Seq[String],
                            queryRange: PartitionRange,
                            window: Option[Window]): Option[String] = {
    val PartitionRange(queryStart, queryEnd) = queryRange
    val scanStart = source.dataModel match {
      case Entities => queryStart
      case Events   => window.map(Constants.Partition.minus(queryStart, _)).orNull
    }

    val sourceRange = PartitionRange(source.query.startPartition, source.query.endPartition)
    val queryableDataRange = PartitionRange(scanStart, queryEnd)
    val intersectedRange: Option[PartitionRange] = sourceRange.intersect(queryableDataRange)

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

    intersectedRange.map { effectiveRange =>
      QueryUtils.build(
        Option(source.query.selects).map(_.asScala.toMap).orNull,
        source.table,
        Option(source.query.wheres).map(_.asScala).getOrElse(Seq.empty[String]) ++ effectiveRange.whereClauses,
        metaColumns ++ keys.map(_ -> null)
      )
    }
  }
}
