package ai.chronon.spark.streaming

import ai.chronon.online.{DataStream, DataStreamBuilder, TopicInfo}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.StreamingQueryListener
import org.apache.spark.sql.streaming.StreamingQueryListener.{QueryProgressEvent, QueryStartedEvent, QueryTerminatedEvent}

object KafkaStreamBuilder extends DataStreamBuilder {
  override def from(topicInfo: TopicInfo)(implicit session: SparkSession, conf: Map[String, String]): DataStream = {
    val conf = topicInfo.params
    val bootstrap = conf.getOrElse("bootstrap", conf("host") + conf.get("port").map(":" + _).getOrElse(""))
    TopicChecker.topicShouldExist(topicInfo.name, bootstrap)
    session.streams.addListener(new StreamingQueryListener() {
      override def onQueryStarted(queryStarted: QueryStartedEvent): Unit = {
        println("Query started: " + queryStarted.id)
      }

      override def onQueryTerminated(queryTerminated: QueryTerminatedEvent): Unit = {
        println("Query terminated: " + queryTerminated.id)
      }

      override def onQueryProgress(queryProgress: QueryProgressEvent): Unit = {
        println("Query made progress: " + queryProgress.progress)
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
