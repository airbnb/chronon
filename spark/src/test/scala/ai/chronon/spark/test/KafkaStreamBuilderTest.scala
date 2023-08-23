package ai.chronon.spark.test

import ai.chronon.online.TopicInfo
import ai.chronon.spark.streaming.KafkaStreamBuilder
import ai.chronon.spark.{SparkSessionBuilder}
import org.apache.spark.sql.SparkSession
import org.junit.{Test}

class KafkaStreamBuilderTest {

  private val spark: SparkSession = SparkSessionBuilder.build("KafkaStreamBuilderTest", local = true)

  @Test(expected = classOf[RuntimeException])
  def testKafkaStreamDoesNotExist(): Unit = {
    val topicInfo = TopicInfo.parse("kafka://test_topic/schema=my_schema/host=X/port=Y")
    KafkaStreamBuilder.from(topicInfo)(spark, Map.empty)
  }
}
