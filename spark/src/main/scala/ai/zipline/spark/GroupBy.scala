package ai.zipline.spark

import java.util

import ai.zipline.aggregator.row.RowAggregator
import ai.zipline.aggregator.windowing._
import ai.zipline.api.Config.{Aggregation, Constants}
import ai.zipline.spark.Extensions._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.apache.spark.sql.functions.unix_timestamp
import org.apache.spark.sql.types.{DataType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row}

class GroupBy(aggregations: Seq[Aggregation], keyColumns: Seq[String], inputDf: DataFrame, finalize: Boolean = true)
    extends Serializable {

  protected[spark] val tsIndex: Int = inputDf.schema.fieldIndex(Constants.TimeColumn)
  private val ziplineSchema = Conversions.toMooliSchema(inputDf.schema)
  private val valueMooliSchema = if (finalize) windowAggregator.outputSchema else windowAggregator.irSchema
  @transient
  protected[spark] lazy val windowAggregator: RowAggregator =
    new RowAggregator(ziplineSchema, aggregations.flatMap(_.unpack))

  def snapshotEntities: DataFrame = {
    val keyBuilder = FastHashing.generateKeyBuilder((keyColumns :+ Constants.PartitionColumn).toArray, inputDf.schema)
    val partitionTsColumnName = "partition_ts"
    val inputDfWithPartitionDs = inputDf.attachPartitionTimestamp(partitionTsColumnName)
    val partitionTsIndex = inputDfWithPartitionDs.schema.fieldIndex(partitionTsColumnName)

    val outputRdd = inputDfWithPartitionDs.rdd
      .keyBy(keyBuilder)
      .aggregateByKey(windowAggregator.init)(
        seqOp = {
          case (ir, row) =>
            windowAggregator.updateWindowed(ir, Conversions.toMooliRow(row, tsIndex), row.getLong(partitionTsIndex))
            ir
        },
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
  def temporalEvents(queriesDf: DataFrame, resolution: Resolution = FiveMinuteResolution): DataFrame = {

    val (minQueryTs, maxQueryTs) = queriesDf.timeRange
    val hopsRdd = hopsAggregate(minQueryTs, resolution)

    def headStart(ts: Long): Long = TsUtils.round(ts, resolution.hopSizes.min)
    queriesDf.matchColumns(keyColumns, inputDf)

    val keyGen = FastHashing.generateKeyBuilder(keyColumns.toArray, queriesDf.schema)

    val queryTsIndex = queriesDf.schema.fieldIndex(Constants.TimeColumn)
    val queriesByHeadStarts = queriesDf.rdd
      .map { row =>
        val ts = row.getLong(queryTsIndex)
        ((keyGen(row), headStart(ts)), ts)
      }
      .groupByKey()
      .mapValues { queryTimesItr =>
        val sorted = queryTimesItr.toArray.distinct
        util.Arrays.sort(sorted)
        sorted
      }

    val sawtoothAggregator =
      new SawtoothAggregator(aggregations, ziplineSchema, resolution)

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

    val minHeadStart = headStart(minQueryTs)
    val eventsByHeadStart = inputDf
      .filter(s"${Constants.TimeColumn} between $minHeadStart and $maxQueryTs")
      .rdd
      .groupBy { (row: Row) =>
        keyGen(row) -> headStart(row.getLong(tsIndex))
      }

    val outputRdd = queriesByHeadStarts
      .leftOuterJoin(headStartsWithIrs)
      .leftOuterJoin(eventsByHeadStart)
      .flatMap {
        case ((keys: KeyWithHash, _: Long),
              ((queries: Array[Long], headStartIrOpt: Option[Array[Any]]), eventsOpt: Option[Iterable[Row]])) =>
          val inputsIt: Iterator[ArrayRow] =
            eventsOpt.map(_.map(Conversions.toMooliRow(_, tsIndex)).toIterator).orNull
          val irs = sawtoothAggregator.cumulateUnsorted(
            inputsIt,
            queries,
            headStartIrOpt.orNull
          )
          queries.indices.map { i =>
            (keys.data :+ queries(i), irs(i))
          }
      }
    toDataFrame(outputRdd, Constants.TimeColumn)
  }

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
            hopsAggregator.update(ir, Conversions.toMooliRow(input, tsIndex))
        },
        combOp = {
          case (ir1: HopsAggregator.IrMapType, ir2: HopsAggregator.IrMapType) =>
            hopsAggregator.merge(ir1, ir2)
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
      Conversions.fromMooliRow(windowAggregator.finalize(ir), valueMooliSchema)
    } else {
      Conversions.fromMooliRow(windowAggregator.denormalize(ir), valueMooliSchema)
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
    val valueSchema: Array[StructField] = Conversions.fromMooliSchema(valueMooliSchema).fields
    val additionalKeySchema =
      StructField(additionalKey, additionKeyType.getOrElse(inputDf.typeOf(additionalKey).dataType))
    StructType((keySchema :+ additionalKeySchema) ++ valueSchema)
  }
}
