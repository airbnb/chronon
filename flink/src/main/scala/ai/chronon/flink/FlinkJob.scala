package ai.chronon.flink

import ai.chronon.api.Extensions.{GroupByOps, SourceOps}
import ai.chronon.api.GroupBy
import ai.chronon.online.KVStore.PutRequest
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.spark.sql.Encoder
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction

class FlinkJob[T](eventSrc: FlinkSource[T], sinkFn: RichAsyncFunction[PutRequest, Option[Long]], groupBy: GroupBy, encoder: Encoder[T], parallelism: Int) {

  protected val exprEval: SparkExpressionEvalFn[T] = new SparkExpressionEvalFn[T](encoder, groupBy)
  val featureGroupName: String = groupBy.getMetaData.getName

  if (groupBy.streamingSource.isEmpty) {
    throw new IllegalArgumentException(
      s"Invalid feature group: $featureGroupName. No streaming source"
    )
  }

  // The source of our Flink application is a Kafka topic
  val kafkaTopic: String = groupBy.streamingSource.get.topic

  def runGroupByJob(env: StreamExecutionEnvironment): DataStream[Option[Long]] = {
    val sourceStream: DataStream[T] =
      eventSrc
        .getDataStream(kafkaTopic, featureGroupName)(env, parallelism)

    val sparkExprEvalDS: DataStream[Map[String, Any]] = sourceStream
      .flatMap(exprEval)
      .uid(s"spark-expr-eval-flatmap-$featureGroupName")
      .name(s"Spark expression eval for $featureGroupName")
      .setParallelism(sourceStream.parallelism) // Use same parallelism as previous operator

    val inputSchema = encoder.schema
    val putRecordDS: DataStream[PutRequest] = sparkExprEvalDS
      .flatMap(AvroCodecFn[T](groupBy, inputSchema, exprEval.getOutputSchema))
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
