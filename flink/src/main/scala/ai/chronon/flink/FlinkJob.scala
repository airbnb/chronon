package ai.chronon.flink

import ai.chronon.aggregator.windowing.ResolutionUtils
import ai.chronon.api.{DataType}
import ai.chronon.api.Extensions.{GroupByOps, SourceOps}
import ai.chronon.flink.window.{
  AlwaysFireOnElementTrigger,
  FlinkRowAggProcessFunction,
  FlinkRowAggregationFunction,
  KeySelector,
  TimestampedTile
}
import ai.chronon.online.{GroupByServingInfoParsed, SparkConversions}
import ai.chronon.online.KVStore.PutRequest
import org.apache.flink.streaming.api.scala.{DataStream, OutputTag, StreamExecutionEnvironment}
import org.apache.spark.sql.Encoder
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction
import org.apache.flink.streaming.api.windowing.assigners.{TumblingEventTimeWindows, WindowAssigner}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.slf4j.LoggerFactory

/**
  * Flink job that processes a single streaming GroupBy and writes out the results to the KV store.
  *
  * There are two versions of the job, tiled and untiled. The untiled version writes out raw events while the tiled
  * version writes out pre-aggregates. See the `runGroupByJob` and `runTiledGroupByJob` methods for more details.
  *
  * @param eventSrc - Provider of a Flink Datastream[T] for the given topic and feature group
  * @param sinkFn - Async Flink writer function to help us write to the KV store
  * @param groupByServingInfoParsed - The GroupBy we are working with
  * @param encoder - Spark Encoder for the input data type
  * @param parallelism - Parallelism to use for the Flink job
  * @tparam T - The input data type
  */
class FlinkJob[T](eventSrc: FlinkSource[T],
                  sinkFn: RichAsyncFunction[PutRequest, WriteResponse],
                  groupByServingInfoParsed: GroupByServingInfoParsed,
                  encoder: Encoder[T],
                  parallelism: Int) {
  private[this] val logger = LoggerFactory.getLogger(getClass)

  val featureGroupName: String = groupByServingInfoParsed.groupBy.getMetaData.getName
  logger.info(f"Creating Flink job. featureGroupName=${featureGroupName}")

  protected val exprEval: SparkExpressionEvalFn[T] =
    new SparkExpressionEvalFn[T](encoder, groupByServingInfoParsed.groupBy)

  if (groupByServingInfoParsed.groupBy.streamingSource.isEmpty) {
    throw new IllegalArgumentException(
      s"Invalid feature group: $featureGroupName. No streaming source"
    )
  }

  // The source of our Flink application is a Kafka topic
  val kafkaTopic: String = groupByServingInfoParsed.groupBy.streamingSource.get.topic

  /**
    * The "untiled" version of the Flink app.
    *
    *  At a high level, the operators are structured as follows:
    *   Kafka source -> Spark expression eval -> Avro conversion -> KV store writer
    *   Kafka source - Reads objects of type T (specific case class, Thrift / Proto) from a Kafka topic
    *   Spark expression eval - Evaluates the Spark SQL expression in the GroupBy and projects and filters the input data
    *   Avro conversion - Converts the Spark expr eval output to a form that can be written out to the KV store
    *      (PutRequest object)
    *   KV store writer - Writes the PutRequest objects to the KV store using the AsyncDataStream API
    *
    *  In this untiled version, there are no shuffles and thus this ends up being a single node in the Flink DAG
    *  (with the above 4 operators and parallelism as injected by the user).
    */
  def runGroupByJob(env: StreamExecutionEnvironment): DataStream[WriteResponse] = {
    logger.info(
      f"Running Flink job for featureGroupName=${featureGroupName}, kafkaTopic=${kafkaTopic}. " +
        f"Tiling is disabled.")

    val sourceStream: DataStream[T] =
      eventSrc
        .getDataStream(kafkaTopic, featureGroupName)(env, parallelism)

    val sparkExprEvalDS: DataStream[Map[String, Any]] = sourceStream
      .flatMap(exprEval)
      .uid(s"spark-expr-eval-flatmap-$featureGroupName")
      .name(s"Spark expression eval for $featureGroupName")
      .setParallelism(sourceStream.parallelism) // Use same parallelism as previous operator

    val putRecordDS: DataStream[PutRequest] = sparkExprEvalDS
      .flatMap(AvroCodecFn[T](groupByServingInfoParsed))
      .uid(s"avro-conversion-$featureGroupName")
      .name(s"Avro conversion for $featureGroupName")
      .setParallelism(sourceStream.parallelism)

    AsyncKVStoreWriter.withUnorderedWaits(
      putRecordDS,
      sinkFn,
      featureGroupName
    )
  }

  /**
    * The "tiled" version of the Flink app.
    *
    * The operators are structured as follows:
    *  1. Kafka source - Reads objects of type T (specific case class, Thrift / Proto) from a Kafka topic
    *  2. Spark expression eval - Evaluates the Spark SQL expression in the GroupBy and projects and filters the input
    *      data
    *  3. Window/tiling - This window aggregates incoming events, keeps track of the IRs, and sends them forward so
    *      they are written out to the KV store
    *  4. Avro conversion - Finishes converting the output of the window (the IRs) to a form that can be written out
    *      to the KV store (PutRequest object)
    *  5. KV store writer - Writes the PutRequest objects to the KV store using the AsyncDataStream API
    *
    *  The window causes a split in the Flink DAG, so there are two nodes, (1+2) and (3+4+5).
    */
  def runTiledGroupByJob(env: StreamExecutionEnvironment): DataStream[WriteResponse] = {
    logger.info(
      f"Running Flink job for featureGroupName=${featureGroupName}, kafkaTopic=${kafkaTopic}. " +
        f"Tiling is enabled.")

    val tilingWindowSizeInMillis: Option[Long] =
      ResolutionUtils.getSmallestWindowResolutionInMillis(groupByServingInfoParsed.groupBy)

    val sourceStream: DataStream[T] =
      eventSrc
        .getDataStream(kafkaTopic, featureGroupName)(env, parallelism)

    val sparkExprEvalDS: DataStream[Map[String, Any]] = sourceStream
      .flatMap(exprEval)
      .uid(s"spark-expr-eval-flatmap-$featureGroupName")
      .name(s"Spark expression eval for $featureGroupName")
      .setParallelism(sourceStream.parallelism) // Use same parallelism as previous operator

    val inputSchema: Seq[(String, DataType)] =
      exprEval.getOutputSchema.fields
        .map(field => (field.name, SparkConversions.toChrononType(field.name, field.dataType)))
        .toSeq

    val window = TumblingEventTimeWindows
      .of(Time.milliseconds(tilingWindowSizeInMillis.get))
      .asInstanceOf[WindowAssigner[Map[String, Any], TimeWindow]]

    // An alternative to AlwaysFireOnElementTrigger can be used: BufferedProcessingTimeTrigger.
    // The latter will buffer writes so they happen at most every X milliseconds per GroupBy & key.
    val trigger = new AlwaysFireOnElementTrigger()

    // We use Flink "Side Outputs" to track any late events that aren't computed.
    val tilingLateEventsTag = OutputTag[Map[String, Any]]("tiling-late-events")

    // The tiling operator works the following way:
    // 1. Input: Spark expression eval (previous operator)
    // 2. Key by the entity key(s) defined in the groupby
    // 3. Window by a tumbling window
    // 4. Use our custom trigger that will "FIRE" on every element
    // 5. the AggregationFunction merges each incoming element with the current IRs which are kept in state
    //    - Each time a "FIRE" is triggered (i.e. on every event), getResult() is called and the current IRs are emitted
    // 6. A process window function does additional processing each time the AggregationFunction emits results
    //    - The only purpose of this window function is to mark tiles as closed so we can do client-side caching in SFS
    // 7. Output: TimestampedTile, containing the current IRs (Avro encoded) and the timestamp of the current element
    val tilingDS: DataStream[TimestampedTile] =
      sparkExprEvalDS
        .keyBy(KeySelector.getKeySelectionFunction(groupByServingInfoParsed.groupBy))
        .window(window)
        .trigger(trigger)
        .sideOutputLateData(tilingLateEventsTag)
        .aggregate(
          // See Flink's "ProcessWindowFunction with Incremental Aggregation"
          preAggregator = new FlinkRowAggregationFunction(groupByServingInfoParsed.groupBy, inputSchema),
          windowFunction = new FlinkRowAggProcessFunction(groupByServingInfoParsed.groupBy, inputSchema)
        )
        .uid(s"tiling-01-$featureGroupName")
        .name(s"Tiling for $featureGroupName")
        .setParallelism(sourceStream.parallelism)

    // Track late events
    val sideOutputStream: DataStream[Map[String, Any]] =
      tilingDS
        .getSideOutput(tilingLateEventsTag)
        .flatMap(new LateEventCounter(featureGroupName))
        .uid(s"tiling-side-output-01-$featureGroupName")
        .name(s"Tiling Side Output Late Data for $featureGroupName")
        .setParallelism(sourceStream.parallelism)

    val putRecordDS: DataStream[PutRequest] = tilingDS
      .flatMap(new TiledAvroCodecFn[T](groupByServingInfoParsed))
      .uid(s"avro-conversion-01-$featureGroupName")
      .name(s"Avro conversion for $featureGroupName")
      .setParallelism(sourceStream.parallelism)

    AsyncKVStoreWriter.withUnorderedWaits(
      putRecordDS,
      sinkFn,
      featureGroupName
    )
  }
}
