package ai.chronon.flink

import ai.chronon.api.Extensions.{GroupByOps, SourceOps}
import ai.chronon.online.GroupByServingInfoParsed
import ai.chronon.online.KVStore.PutRequest
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.spark.sql.Encoder
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction

class FlinkJob[T](eventSrc: FlinkSource[T],
                  sinkFn: RichAsyncFunction[PutRequest, WriteResponse],
                  groupByServingInfoParsed: GroupByServingInfoParsed,
                  encoder: Encoder[T],
                  parallelism: Int) {

  protected val exprEval: SparkExpressionEvalFn[T] =
    new SparkExpressionEvalFn[T](encoder, groupByServingInfoParsed.groupBy)
  val featureGroupName: String = groupByServingInfoParsed.groupBy.getMetaData.getName

  if (groupByServingInfoParsed.groupBy.streamingSource.isEmpty) {
    throw new IllegalArgumentException(
      s"Invalid feature group: $featureGroupName. No streaming source"
    )
  }

  // The source of our Flink application is a Kafka topic
  val kafkaTopic: String = groupByServingInfoParsed.groupBy.streamingSource.get.topic

  def runGroupByJob(env: StreamExecutionEnvironment): DataStream[WriteResponse] = {
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
