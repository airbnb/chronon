package ai.chronon.spark.streaming

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.streaming.StreamingQueryListener
import org.apache.spark.sql.streaming.StreamingQueryListener.{QueryProgressEvent, QueryStartedEvent, QueryTerminatedEvent}

import scala.collection.Seq

case class DataStream(df: DataFrame, partitions: Int, topicInfo: TopicInfo) {
  def apply(query: Query): DataStream = {
    val dfWithQuery = null
      val streamingSource = groupByConf.streamingSource.get
    val query = streamingSource.query
    val selects = Option(query.selects).map(_.asScala.toMap).orNull
    val timeColumn = Option(query.timeColumn).getOrElse(Constants.TimeColumn)
    val fillIfAbsent = groupByConf.dataModel match {
      case DataModel.Entities =>
        Map(Constants.TimeColumn -> timeColumn, Constants.ReversalColumn -> null, Constants.MutationTimeColumn -> null)
      case chronon.api.DataModel.Events => Map(Constants.TimeColumn -> timeColumn)
    }
    val keys = groupByConf.getKeyColumns.asScala

    val baseWheres = Option(query.wheres).map(_.asScala).getOrElse(Seq.empty[String])
    val selectMap = Option(selects).getOrElse(Map.empty[String, String])
    val keyWhereOption = keys
      .map { key =>
        s"${selectMap.getOrElse(key, key)} IS NOT NULL"
      }
      .mkString(" OR ")
    val timeWheres = groupByConf.dataModel match {
      case chronon.api.DataModel.Entities => Seq(s"${Constants.MutationTimeColumn} is NOT NULL")
      case chronon.api.DataModel.Events   => Seq(s"$timeColumn is NOT NULL")
    }
    QueryUtils.build(
      selects,
      Constants.StreamingInputTable,
      baseWheres ++ timeWheres :+ s"($keyWhereOption)",
      fillIfAbsent = if (selects == null) null else fillIfAbsent
    )

    DataStream(dfWithQuery, partitions, topicInfo)
  }
}

trait DataStreamBuilder {
  def from(topic: String, conf: Map[String, String])(implicit session: SparkSession): DataStream
}

case class TopicInfo(name: String, topicType: String, params: Map[String, String])
object TopicInfo {
  // default topic type is kafka
  // kafka://topic_name/host=X/port=Y should parse into TopicInfo(topic_name, kafka, {host: X, port Y})
  def parse(topic: String): TopicInfo = {
    assert(topic.nonEmpty, s"invalid topic: $topic")
    val (topicType, rest) = if(topic.contains("://")) {
      val tokens = topic.split("://",2)
      tokens.head -> tokens.last
    } else {
      "kafka" -> topic
    }
    assert(rest.nonEmpty, s"invalid topic: $topic")
    val fields = rest.split("/")
    val topicName = fields.head
    val params = fields.tail.map{f => val kv = f.split("=", 2); kv.head -> kv.last}.toMap
    TopicInfo(topicName, topicType, params)
  }

  // TODO move this to tests
  def main(args: Array[String]): Unit = {
    def check(a: TopicInfo, b: TopicInfo): Boolean = {
      if(a != b) {
        print(a)
        print(b)
      }
      a == b
    }
    check(parse("kafka://topic_name/host=X/port=Y"), TopicInfo("topic_name", "kafka", Map("host" -> "X", "port" -> "Y")))
    check(parse("topic_name/host=X/port=Y"), TopicInfo("topic_name", "kafka", Map("host" -> "X", "port" -> "Y")))
    check(parse("topic_name"), TopicInfo("topic_name", "kafka", Map.empty))
  }
}

object DataStreamBuilder {
  val registry: Map[String, DataStreamBuilder] = Map(
    "kafka" -> new KafkaStreamBuilder()
    // TODO add kinesis support
    // TODO make this part of online api
  )
}

class KafkaStreamBuilder extends DataStreamBuilder {
  override def from(session: SparkSession, topic: String, conf: Map[String, String]): DataStream = {
    val bootstrap = conf.getOrElse("bootstrap", conf("host") + conf.get("port").map(":" + _).getOrElse(""))
    val topicInfo = TopicInfo.parse(topic)
    TopicChecker.topicShouldExist(topic, bootstrap)
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
      .option("subscribe", topic)
      .option("enable.auto.commit", "true")
      .load()
      .selectExpr("value")
    DataStream(df, partitions = TopicChecker.getPartitions(topic, bootstrap = bootstrap), topicInfo)
  }
}