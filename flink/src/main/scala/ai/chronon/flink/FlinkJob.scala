package ai.chronon.flink

import ai.chronon.api.Extensions.{GroupByOps, SourceOps}
import ai.chronon.api.{GroupBy, Source}
import ai.chronon.online.Api
import ai.chronon.online.KVStore.PutRequest
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.spark.sql.Encoder
import org.apache.flink.api.scala._

class FlinkJob[T](eventSrc: FlinkSource[T], onlineImpl: Api, groupBy: GroupBy, encoder: Encoder[T], parallelism: Int) {

  protected val exprEval: SparkExpressionEvalFn[T] = new SparkExpressionEvalFn[T](encoder, groupBy)
  val featureGroupName: String = groupBy.getMetaData.getName

  if (groupBy.streamingSource.isEmpty) {
    throw new IllegalArgumentException(
      s"Invalid feature group: $featureGroupName. No streaming source"
    )
  }

  // The source of our Flink application is a Kafka topic
  val kafkaTopic: String = groupBy.streamingSource.get.topic

  def runGroupByJob(env: StreamExecutionEnvironment): Unit = {
    val sourceStream: DataStream[T] =
      eventSrc
        .getDataStream(kafkaTopic, featureGroupName)(env, parallelism)

    val sparkExprEvalDS: DataStream[Map[String, Any]] = sourceStream
      .flatMap(exprEval)
      .uid(s"spark-expr-eval-flatmap-01-$featureGroupName")
      .name(s"Spark expression eval for $featureGroupName")
      .slotSharingGroup(featureGroupName)
      .setParallelism(sourceStream.parallelism) // Use same parallelism as previous operator

    val inputSchema = encoder.schema
    val putRecordDS: DataStream[PutRequest] = sparkExprEvalDS
      .flatMap(AvroCodecFn[T](groupBy, inputSchema, exprEval.getOutputSchema))
      .uid(s"avro-conversion-$featureGroupName")
      .name(s"Avro conversion for $featureGroupName")
      .slotSharingGroup(featureGroupName)
      .setParallelism(sourceStream.parallelism)

    AsyncKVStoreWriter.withUnorderedWaits(
      putRecordDS,
      new AsyncKVStoreWriter(onlineImpl),
      featureGroupName
    )
  }
}
