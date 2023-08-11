package ai.chronon.spark.streaming

import ai.chronon.api
import ai.chronon.api.Extensions.{GroupByOps, MetadataOps, SourceOps}
import ai.chronon.online._
import ai.chronon.spark.GenericRowHandler
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.streaming.{StreamingQuery, Trigger}
import org.apache.spark.sql._

import scala.concurrent.duration.DurationInt

class GroupByRunner(groupByConf: api.GroupBy,
                    session: SparkSession,
                    conf: Map[String, String],
                    apiImpl: Api,
                    debug: Boolean) {

  @transient lazy val context = Metrics.Context(Metrics.Environment.GroupByStreaming, groupByConf)
  // input data stream needs to be a dataframe - Array[Byte]
  private def decode(dataStream: DataStream): DataStream = {
    val servingInfoProxy = PutRequestBuilder.buildProxyServingInfo(groupByConf, session)
    val streamDecoder = apiImpl.streamDecoder(servingInfoProxy)
    val df = dataStream.df
    val ingressContext = context.withSuffix("ingress")
    import session.implicits._
    implicit val structTypeEncoder: Encoder[Mutation] = Encoders.kryo[Mutation]
    val deserialized: Dataset[Mutation] = df
      .as[Array[Byte]]
      .map { arr =>
        ingressContext.increment(Metrics.Name.RowCount)
        ingressContext.count(Metrics.Name.Bytes, arr.length)
        try {
          apiImpl.streamDecoder(servingInfoProxy).decode(arr)
        } catch {
          case ex: Throwable =>
            println(s"Error while decoding streaming events ${ex.printStackTrace()}")
            ingressContext.incrementException(ex)
            null
        }
      }
      .filter { mutation =>
        val beforeAndAfterAreNull = mutation.before != null && mutation.after != null
        val beforeAndAfterAreSame = mutation.before sameElements mutation.after
        mutation != null && (!beforeAndAfterAreNull || !beforeAndAfterAreSame)
      }
    val streamSchema = SparkConversions.fromChrononSchema(streamDecoder.schema)
    val des = deserialized
      .flatMap { mutation =>
        Seq(mutation.after, mutation.before)
          .filter(_ != null)
          .map(SparkConversions.toSparkRow(_, streamDecoder.schema, GenericRowHandler.func).asInstanceOf[Row])
      }(RowEncoder(streamSchema))
    dataStream.copy(df = des)
  }

  private def buildStreamForSource(source: api.Source): DataStream = {
    if (source.isSetEvents || source.isSetEntities) { // boundary condition
      val topic = TopicInfo.parse(source.topic)
      val stream = buildStream(topic)
      val decoded = decode(stream)
      decoded.apply(source.query)
    } else {
      throw new IllegalStateException(s"Source need to be either events or entities to create data stream from")
    }
  }

  private def internalStreamBuilder(streamType: String): DataStreamBuilder = {
    val suppliedBuilder = apiImpl.generateStreamBuilder(streamType)
    if (suppliedBuilder == null) {
      if (streamType == "kafka") {
        KafkaStreamBuilder
      } else {
        throw new RuntimeException(
          s"Couldn't access builder for type $streamType. Please implement one by overriding Api.generateStreamBuilder")
      }
    } else {
      suppliedBuilder
    }
  }

  private def buildStream(topic: TopicInfo): DataStream =
    internalStreamBuilder(topic.topicType).from(topic)(session, conf)

  @transient lazy val fetcher = apiImpl.buildFetcher(debug)

  def startWriting: StreamingQuery = {
    assert(groupByConf.streamingSource.isDefined, s"No streaming source in groupBy: ${groupByConf.metaData.cleanName}")
    val source = groupByConf.streamingSource.get
    val putRequestBuilder = PutRequestBuilder.from(groupByConf, session)
    if (source.isSetEvents || source.isSetEntities) {
      val dataWriter = new DataWriter(apiImpl, context.withSuffix("egress"), 120, debug)
      implicit val putRequestEncoder: Encoder[KVStore.PutRequest] = Encoders.kryo[KVStore.PutRequest]
      buildStreamForSource(source).df
        .map { putRequestBuilder.from(_, debug) }
        .writeStream
        .outputMode("append")
        .trigger(Trigger.Continuous(2.minute))
        .foreach(dataWriter)
        .start()
    } else if (source.isSetJoinSource) { // recursive condition
      val joinWriter = JoinWriter.from(groupByConf, debug)(session, apiImpl)
      buildStreamForSource(source.getJoinSource.join.left).df.writeStream
        .outputMode("append")
        .trigger(Trigger.Continuous(2.minute))
        .foreach(joinWriter)
        .start()
    } else {
      throw new IllegalStateException("Either events or entities or joinSource needs to be set in streaming source")
    }
  }
}
