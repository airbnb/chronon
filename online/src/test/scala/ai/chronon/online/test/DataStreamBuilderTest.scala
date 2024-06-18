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

package ai.chronon.online.test

import org.slf4j.LoggerFactory
import ai.chronon.api.{Builders, DataModel, LongType, StringType, StructField, StructType}
import ai.chronon.online.{DataStream, SparkConversions, TopicInfo}
import ai.chronon.online.TopicInfo.parse
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.junit.Assert.assertTrue
import org.junit.Test

import scala.util.ScalaJavaConversions.JListOps

class DataStreamBuilderTest {
  @transient lazy val logger = LoggerFactory.getLogger(getClass)
  lazy val spark: SparkSession = {
    System.setSecurityManager(null)
    val spark = SparkSession
      .builder()
      .appName("DataStreamBuilderTest")
      .master("local")
      .getOrCreate()
    spark
  }

  @Test
  def testDataStreamQueryEvent(): Unit = {
    val topicInfo = TopicInfo.parse("kafka://topic_name/schema=my_schema/host=X/port=Y")
    val df = testDataFrame()
    // todo: test start/ end partition in where clause
    val query = Builders.Query(
      selects = Builders.Selects("listing_id", "ts", "host_id"),
      startPartition = "2022-09-30",
      endPartition = "2022-10-30",
      timeColumn = "ts"
    )
    val dataStream = DataStream(df, 1, topicInfo).apply(query, Seq("listing_id", "host_id"), DataModel.Events)
    assertTrue(dataStream.topicInfo == topicInfo)
    assertTrue(dataStream.partitions == 1)
    assertTrue(dataStream.df.count() == 6)
  }

  @Test
  def testTopicInfoParsing(): Unit = {
    checkTopicInfo(parse("kafka://topic_name/schema=test_schema/host=X/port=Y"),
                   TopicInfo("topic_name", "kafka", Map("schema" -> "test_schema", "host" -> "X", "port" -> "Y")))
    checkTopicInfo(parse("topic_name/host=X/port=Y"),
                   TopicInfo("topic_name", "kafka", Map("host" -> "X", "port" -> "Y")))
    checkTopicInfo(parse("topic_name"), TopicInfo("topic_name", "kafka", Map.empty))
  }

  def checkTopicInfo(actual: TopicInfo, expected: TopicInfo): Unit = {
    if (actual != expected) {
      logger.info(s"Actual topicInfo != expected topicInfo. Actual: $actual, expected: $expected")
    }
    assert(actual == expected)
  }

  def testDataFrame(): DataFrame = {
    val schema = StructType(
      "testDataFrame",
      Array(
        StructField("listing_id", LongType),
        StructField("host_id", LongType),
        StructField("ts", StringType),
        StructField("ds", StringType)
      )
    )
    val rows = List(
      Row(1L, 201L, "2022-09-29 10:00:00", "2022-09-39"),
      Row(2L, 303L, "2022-10-02 10:00:00", "2022-10-02"),
      Row(3L, 105L, "2022-10-03 10:00:00", "2022-10-03"),
      Row(4L, 206L, "2022-10-18 10:00:00", "2022-10-18"),
      Row(5L, 357L, "2022-10-30 10:00:00", "2022-10-30"),
      Row(6L, 158L, "2022-11-15 10:00:00", "2022-11-15")
    )

    spark.createDataFrame(rows.toJava, SparkConversions.fromChrononSchema(schema))
  }
}
