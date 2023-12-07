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

import org.slf4j.LoggerFactory
import ai.chronon.online.{DataStream, StreamBuilder, TopicInfo}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.StreamingQueryListener
import org.apache.spark.sql.streaming.StreamingQueryListener.{
  QueryProgressEvent,
  QueryStartedEvent,
  QueryTerminatedEvent
}

object KafkaStreamBuilder extends StreamBuilder {
  @transient lazy val logger = LoggerFactory.getLogger(getClass)
  override def from(topicInfo: TopicInfo)(implicit session: SparkSession, conf: Map[String, String]): DataStream = {
    val conf = topicInfo.params
    val bootstrap = conf.getOrElse("bootstrap", conf("host") + conf.get("port").map(":" + _).getOrElse(""))
    TopicChecker.topicShouldExist(topicInfo.name, bootstrap)
    session.streams.addListener(new StreamingQueryListener() {
      override def onQueryStarted(queryStarted: QueryStartedEvent): Unit = {
        logger.info("Query started: " + queryStarted.id)
      }

      override def onQueryTerminated(queryTerminated: QueryTerminatedEvent): Unit = {
        logger.info("Query terminated: " + queryTerminated.id)
      }

      override def onQueryProgress(queryProgress: QueryProgressEvent): Unit = {
        logger.info("Query made progress: " + queryProgress.progress)
      }
    })
    val df = session.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", bootstrap)
      .option("subscribe", topicInfo.name)
      .option("enable.auto.commit", "true")
      .load()
      .selectExpr("value")
    DataStream(df, partitions = TopicChecker.getPartitions(topicInfo.name, bootstrap = bootstrap), topicInfo)
  }
}
