package ai.chronon.flink.window

import ai.chronon.aggregator.row.RowAggregator
import ai.chronon.api.Extensions.GroupByOps
import ai.chronon.api.{Constants, DataType, GroupBy, Row}
import ai.chronon.flink.SparkExprOutput
import ai.chronon.online.{ArrayRow, TileCodec}
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.configuration.Configuration
import org.apache.flink.metrics.Counter
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import org.slf4j.LoggerFactory

import java.util.Objects
import scala.util.{Failure, Success, Try}

/**
 * Combines the IR (intermediate result) with the timestamp of the event being processed.
 * We need the timestamp of the event processed so we can calculate processing lag down the line.
 *
 * Example: for a GroupBy with 2 windows, we'd have TimestampedTile( [IR for window 1, IR for window 2], timestamp ).
 *
 * Note: This is a POJO class to allow us to safely evolve the schema without breaking Flink jobs. See: go/flink-pojo-types
 * for details on what safe evolution is.
 *
 *
 * @param ir the array of partial aggregates
 * @param latestTsMillis timestamp of the current event being processed
 */
class TimestampedIRState(
                          var ir: Array[Any],
                          var latestTsMillis: Option[Long],
                          var kafkaTimestamp: Option[Long]
                        ) {
  def this() = this(Array(), None, None)

  override def toString: String =
    s"TimestampedIR(ir=${ir.mkString(", ")}, latestTsMillis=$latestTsMillis), kafkaTimestamp=$kafkaTimestamp)"

  override def hashCode(): Int =
    Objects.hash(ir.deep, latestTsMillis, kafkaTimestamp)

  override def equals(other: Any): Boolean =
    other match {
      case e: TimestampedIRState =>
        ir.sameElements(e.ir) && latestTsMillis == e.latestTsMillis && kafkaTimestamp == e.kafkaTimestamp
      case _ => false
    }
}

/**
 * Wrapper Flink aggregator around Chronon's RowAggregator. Relies on Flink to pass in
 * the correct set of events for the tile. As the aggregates produced by this function
 * are used on the serving side along with other pre-aggregates, we don't 'finalize' the
 * Chronon RowAggregator and instead return the intermediate representation.
 *
 * This cannot be a RichAggregateFunction because Flink does not support Rich functions in windows.
 */
class FlinkRowAggregationFunction(
                                   groupBy: GroupBy,
                                   inputSchema: Seq[(String, DataType)]
                                 ) extends AggregateFunction[SparkExprOutput, TimestampedIRState, TimestampedIRState] {
  @transient private[flink] var rowAggregator: RowAggregator = _
  @transient lazy val logger = LoggerFactory.getLogger(getClass)
  private val valueColumns: Array[String] = inputSchema.map(_._1).toArray // column order matters
  private val timeColumnAlias: String = Constants.TimeColumn

  /**
   * Initialize the transient rowAggregator.
   * This method is idempotent:
   *   1. The initialized RowAggregator is always the same given a `groupBy` and `inputSchema`.
   *   2. The RowAggregator doens't hold state; we (Flink) keep track of the state of the IRs.
   *  */
  private def initializeRowAggregator(): Unit =
    rowAggregator = TileCodec.buildRowAggregator(groupBy, inputSchema)

  override def createAccumulator(): TimestampedIRState = {
    initializeRowAggregator()
    new TimestampedIRState(rowAggregator.init, None, None)
  }

  override def add(
      element: SparkExprOutput,
      accumulatorIr: TimestampedIRState
  ): TimestampedIRState = {
    // Most times, the time column is a Long, but it could be a Double.
    val tsMills = Try(element.data(timeColumnAlias).asInstanceOf[Long])
      .getOrElse(element.data(timeColumnAlias).asInstanceOf[Double].toLong)
    val row = toChrononRow(element.data, tsMills)

    // Given that the rowAggregator is transient, it may be null when a job is restored from a checkpoint
    if (rowAggregator == null) {
      logger.debug(
        f"The Flink RowAggregator was null for groupBy=${groupBy.getMetaData.getName} tsMills=$tsMills"
      )
      initializeRowAggregator()
    }

    logger.debug(
      f"Flink pre-aggregates BEFORE adding new element: accumulatorIr=[${accumulatorIr.ir
        .mkString(", ")}] groupBy=${groupBy.getMetaData.getName} tsMills=$tsMills element=$element"
    )

    val partialAggregates = Try {
      rowAggregator.update(accumulatorIr.ir, row)
    }

    partialAggregates match {
      case Success(v) => {
        logger.debug(
          f"Flink pre-aggregates AFTER adding new element [${v.mkString(", ")}] " +
            f"groupBy=${groupBy.getMetaData.getName} tsMills=$tsMills element=$element"
        )
        new TimestampedIRState(v, Some(tsMills), element.metadata.kafkaTimestamp)
      }
      case Failure(e) =>
        logger.error(
          s"Flink error calculating partial row aggregate. " +
            s"groupBy=${groupBy.getMetaData.getName} tsMills=$tsMills element=$element",
          e
        )
        throw e
    }
  }

  // Note we return intermediate results here as the results of this
  // aggregator are used on the serving side along with other pre-aggregates
  override def getResult(accumulatorIr: TimestampedIRState): TimestampedIRState =
    accumulatorIr

  override def merge(aIr: TimestampedIRState, bIr: TimestampedIRState): TimestampedIRState =
    new TimestampedIRState(
      rowAggregator.merge(aIr.ir, bIr.ir),
      aIr.latestTsMillis
        .flatMap(aL => bIr.latestTsMillis.map(bL => Math.max(aL, bL)))
        .orElse(aIr.latestTsMillis.orElse(bIr.latestTsMillis)),
      aIr.kafkaTimestamp
        .flatMap(aKL => bIr.kafkaTimestamp.map(bKL => Math.max(aKL, bKL)))
        .orElse(aIr.kafkaTimestamp.orElse(bIr.kafkaTimestamp))
    )

  def toChrononRow(value: Map[String, Any], tsMills: Long): Row = {
    // The row values need to be in the same order as the input schema columns
    // The reason they are out of order in the first place is because the CatalystUtil does not return values in the
    // same order as the schema
    val values: Array[Any] = valueColumns.map(value(_))
    new ArrayRow(values, tsMills)
  }
}

/**
 * Combines the entity keys, the encoded IR (intermediate result), and the timestamp of the event being processed.
 *
 * We need the timestamp of the event processed so we can calculate processing lag down the line.
 *
 * Note: This is a POJO class to allow us to safely evolve the schema without breaking Flink jobs. See: go/flink-pojo-types
 * for details on what safe evolution is.
 *
 * @param keys the GroupBy entity keys
 * @param tileBytes encoded tile IR
 * @param latestTsMillis timestamp of the current event being processed
 */
class TimestampedTileState(
                            var keys: List[Any],
                            var tileBytes: Array[Byte],
                            var latestTsMillis: Long,
                            var kafkaTimestamp: Option[Long]
                          ) {
  def this() = this(List(), Array(), 0L, None)

  override def toString: String =
    s"TimestampedTile(keys=${keys.mkString(", ")}, tileBytes=${java.util.Base64.getEncoder
      .encodeToString(tileBytes)}, latestTsMillis=$latestTsMillis), kafkaTimestamp=$kafkaTimestamp)"

  override def hashCode(): Int =
    Objects.hash(keys, tileBytes.deep, latestTsMillis.asInstanceOf[java.lang.Long], kafkaTimestamp)

  override def equals(other: Any): Boolean =
    other match {
      case e: TimestampedTileState =>
        keys == e.keys && tileBytes.sameElements(e.tileBytes) && latestTsMillis == e.latestTsMillis && kafkaTimestamp == e.kafkaTimestamp
      case _ => false
    }
}

// This process function is only meant to be used downstream of the ChrononFlinkAggregationFunction
class FlinkRowAggProcessFunction(
    groupBy: GroupBy,
    inputSchema: Seq[(String, DataType)]
) extends ProcessWindowFunction[TimestampedIRState, TimestampedTileState, List[Any], TimeWindow] {

  @transient private[flink] var tileCodec: TileCodec = _
  @transient lazy val logger = LoggerFactory.getLogger(getClass)

  @transient private var rowProcessingErrorCounter: Counter = _
  @transient private var eventProcessingErrorCounter: Counter =
    _ // Shared metric for errors across the entire Flink app.

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    tileCodec = new TileCodec(groupBy, inputSchema)

    val metricsGroup = getRuntimeContext.getMetricGroup
      .addGroup("chronon")
      .addGroup("feature_group", groupBy.getMetaData.getName)
    rowProcessingErrorCounter = metricsGroup.counter("tiling_process_function_error")
    eventProcessingErrorCounter = metricsGroup.counter("event_processing_error")
  }

  /**
    * Process events emitted from the aggregate function.
    * Output format: (keys, encoded tile IR, timestamp of the event being processed)
    * */
  override def process(
      keys: List[Any],
      context: Context,
      elements: Iterable[TimestampedIRState],
      out: Collector[TimestampedTileState]
  ): Unit = {
    val windowEnd = context.window.getEnd
    val irEntry = elements.head
    val isComplete = context.currentWatermark >= windowEnd

    val tileBytes = Try {
      tileCodec.makeTileIr(irEntry.ir, isComplete)
    }

    tileBytes match {
      case Success(v) => {
        logger.debug(
          s"""
                |Flink aggregator processed element irEntry=$irEntry
                |tileBytes=${java.util.Base64.getEncoder.encodeToString(v)}
                |windowEnd=$windowEnd groupBy=${groupBy.getMetaData.getName}
                |keys=$keys isComplete=$isComplete tileAvroSchema=${tileCodec.tileAvroSchema}"""
        )
        out.collect(
          new TimestampedTileState(keys, v, irEntry.latestTsMillis.get, irEntry.kafkaTimestamp)
        )
      }
      case Failure(e) =>
        // To improve availability, we don't rethrow the exception. We just drop the event
        // and track the errors in a metric. Alerts should be set up on this metric.
        logger.error(s"Flink process error making tile IR", e)
        eventProcessingErrorCounter.inc()
        rowProcessingErrorCounter.inc()
    }
  }
}
