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
import ai.chronon.aggregator.base.BottomK
import ai.chronon.api
import ai.chronon.api.Extensions.{GroupByOps, SourceOps}
import ai.chronon.api.{ThriftJsonCodec, UnknownType}
import ai.chronon.spark.Driver
import ai.chronon.spark.Driver.OnlineSubcommand
import org.apache.kafka.clients.admin.{AdminClient, AdminClientConfig, ListTopicsOptions}
import ai.chronon.spark.stats.EditDistance
import org.apache.thrift.TBase
import org.rogach.scallop.{ScallopConf, ScallopOption, Subcommand}

import java.util
import java.util.Properties
import scala.collection.JavaConverters.{asScalaBufferConverter, asScalaIteratorConverter}
import scala.reflect.ClassTag
import scala.util.Try

object TopicChecker {
  @transient lazy val logger = LoggerFactory.getLogger(getClass)

  def getPartitions(topic: String, bootstrap: String): Int = {
    val props = new Properties()
    props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap)
    val adminClient = AdminClient.create(props)
    val topicDescription = adminClient.describeTopics(util.Arrays.asList(topic)).values().get(topic);
    topicDescription.get().partitions().size()
  }

  def topicShouldExist(topic: String, bootstrap: String): Unit = {
    val props = new Properties()
    props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap)
    try {
      val adminClient = AdminClient.create(props)
      val options = new ListTopicsOptions()
      options.listInternal(true)
      val topicsList = adminClient.listTopics(options)
      val topicsResult = topicsList.namesToListings().get()
      if (!topicsResult.containsKey(topic)) {
        val closestK = new BottomK[(Double, String)](UnknownType(), 5)
        val result = new util.ArrayList[(Double, String)]()
        topicsResult // find closestK matches based on edit distance.
          .entrySet()
          .iterator()
          .asScala
          .map { topicListing =>
            val existing = topicListing.getValue.name()
            EditDistance.betweenStrings(existing, topic).total / existing.length.toDouble -> existing
          }
          .foldLeft(result)((cnt, elem) => closestK.update(cnt, elem))
        closestK.finalize(result)
        throw new RuntimeException(s"""
                                      |Requested topic: $topic is not found in broker: $bootstrap.
                                      |Either the bootstrap is incorrect or the topic is. 
                                      |
                                      | ------ Most similar topics are ------
                                      |
                                      |  ${result.asScala.map(_._2).mkString("\n  ")}
                                      |
                                      | ------ End ------
                                      |""".stripMargin)
      } else {
        logger.info(s"Found topic $topic in bootstrap $bootstrap.")
      }
    } catch {
      case ex: Exception => throw new RuntimeException(s"Failed to check for topic ${topic} in ${bootstrap}", ex)
    }
  }

  class Args(arguments: Seq[String]) extends ScallopConf(arguments) {
    @transient lazy val logger = LoggerFactory.getLogger(getClass)
    val conf: ScallopOption[String] = opt[String](descr = "Conf to pull topic and bootstrap server information")
    val bootstrap: ScallopOption[String] = opt[String](descr = "Kafka bootstrap server in host:port format")
    val topic: ScallopOption[String] = opt[String](descr = "kafka topic to check metadata for")
    verify()
  }

  // print out number of partitions and exit
  def main(argSeq: Array[String]) {
    val args = new Args(argSeq)
    val (topic, bootstrap) = if (args.conf.isDefined) {
      val confPath = args.conf()
      val groupBy = Driver.parseConf[api.GroupBy](confPath)
      val source = groupBy.streamingSource.get
      val topic = source.cleanTopic
      val tokens = source.topicTokens
      lazy val host = tokens.get("host")
      lazy val port = tokens.get("port")
      lazy val hostPort = s"${host.get}:${port.get}"
      topic -> args.bootstrap.getOrElse(hostPort)
    } else {
      args.topic() -> args.bootstrap()
    }
    logger.info(getPartitions(topic, bootstrap).toString)
    System.exit(0)
  }
}
