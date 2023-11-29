/*
 *    Copyright (C) 2023 The Chronon Authors.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package ai.chronon.spark.streaming

import ai.chronon.online.{
  Api,
  DataStream,
  GroupByServingInfoParsed,
  Metrics,
  Mutation,
  SparkConversions,
  StreamBuilder,
  TopicInfo
}
import ai.chronon.spark.GenericRowHandler
import ai.chronon.api
import ai.chronon.api.Extensions.GroupByOps
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.{Dataset, Encoder, Encoders, Row, SparkSession}

// This class is used both in joinSource based compute and regular compute
case class DecodedStream(groupByConf: api.GroupBy,
                         apiImpl: Api,
                         cliProps: Map[String, String],
                         session: SparkSession,
                         context: Metrics.Context,
                         debug: Boolean) {
  private def internalStreamBuilder(streamType: String): StreamBuilder = {
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

  private def decode(dataStream: DataStream, servingInfo: GroupByServingInfoParsed): DataStream = {
    val streamDecoder = apiImpl.streamDecoder(servingInfo)

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
          streamDecoder.decode(arr)
        } catch {
          case ex: Throwable =>
            println(s"Error while decoding streaming events from stream: ${dataStream.topicInfo.name}")
            ex.printStackTrace()
            ingressContext.incrementException(ex)
            null
        }
      }
      .filter { mutation =>
        lazy val bothNull = mutation.before != null && mutation.after != null
        lazy val bothSame = mutation.before sameElements mutation.after
        (mutation != null) && (!bothNull || !bothSame)
      }
    val streamSchema = SparkConversions.fromChrononSchema(streamDecoder.schema)
    println(s"""
         | streaming source: ${groupByConf.streamingSource.get}
         | streaming dataset: ${groupByConf.streamingDataset}
         | stream schema: ${streamSchema.catalogString}
         |""".stripMargin)

    val des = deserialized
      .flatMap { mutation =>
        Seq(mutation.after, mutation.before)
          .filter(_ != null)
          .map(SparkConversions.toSparkRow(_, streamDecoder.schema, GenericRowHandler.func).asInstanceOf[Row])
      }(RowEncoder(streamSchema))
    dataStream.copy(df = des)
  }

  private def servingInfo: GroupByServingInfoParsed =
    apiImpl.buildFetcher(debug).getGroupByServingInfo(groupByConf.getMetaData.getName).get

  def getTopic: String = {

  }
  def build(topic: String): DataStream = {
    val topicInfo = TopicInfo.parse(topic)
    val byteStream = internalStreamBuilder(topicInfo.topicType).from(topicInfo)(session, cliProps)
    val decodedStream = decode(byteStream, servingInfo)
  }
}
