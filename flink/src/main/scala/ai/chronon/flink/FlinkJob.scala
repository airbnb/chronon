package ai.chronon.flink

import ai.chronon.aggregator.windowing.ResolutionUtils
import ai.chronon.api.Extensions.{GroupByOps, SourceOps}
import ai.chronon.online.GroupByServingInfoParsed
import ai.chronon.online.KVStore.PutRequest
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.spark.sql.Encoder
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction

/**
  * Flink job that processes a single streaming GroupBy and writes out the results
  * (raw events in untiled, pre-aggregates in case of tiled) to the KV store.
  *  At a high level, the operators are structured as follows:
  *  Kafka source -> Spark expression eval -> Avro conversion -> KV store writer
  *  Kafka source - Reads objects of type T (specific case class, Thrift / Proto) from a Kafka topic
  *  Spark expression eval - Evaluates the Spark SQL expression in the GroupBy and projects and filters the input data
  *  Avro conversion - Converts the Spark expr eval output to a form that can be written out to the KV store (PutRequest object)
  *  KV store writer - Writes the PutRequest objects to the KV store using the AsyncDataStream API
  *
  *  In the untiled version there are no-shuffles and thus this ends up being a single node in the Flink DAG
  *  (with the above 4 operators and parallelism as injected by the user)
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
  val featureGroupName: String = groupByServingInfoParsed.groupBy.getMetaData.getName
  println(f"Creating Flink job. featureGroupName=${featureGroupName}")

  protected val exprEval: SparkExpressionEvalFn[T] =
    new SparkExpressionEvalFn[T](encoder, groupByServingInfoParsed.groupBy)

  if (groupByServingInfoParsed.groupBy.streamingSource.isEmpty) {
    throw new IllegalArgumentException(
      s"Invalid feature group: $featureGroupName. No streaming source"
    )
  }

  // The source of our Flink application is a Kafka topic
  val kafkaTopic: String = groupByServingInfoParsed.groupBy.streamingSource.get.topic

  def runGroupByJob(env: StreamExecutionEnvironment): DataStream[WriteResponse] = {
    println(f"Running Flink job for featureGroupName=${featureGroupName}, kafkaTopic=${kafkaTopic}, window=OFF.")

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

  def runTiledGroupByJob(env: StreamExecutionEnvironment): DataStream[WriteResponse] = {
    println(f"Running Flink job for featureGroupName=${featureGroupName}, kafkaTopic=${kafkaTopic}, window=ON.")

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
}
