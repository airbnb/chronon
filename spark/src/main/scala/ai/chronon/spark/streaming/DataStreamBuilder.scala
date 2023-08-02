package ai.chronon.spark.streaming

import ai.chronon.api
import ai.chronon.api.DataModel.DataModel
import ai.chronon.api.Extensions.SourceOps
import ai.chronon.api.{Constants, DataModel, QueryUtils, ThriftJsonCodec}
import ai.chronon.online.{Metrics, Mutation, SparkConversions, StreamDecoder}
import ai.chronon.spark.GenericRowHandler
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.{DataFrame, Dataset, Encoder, Encoders, Row, SparkSession}
import org.apache.spark.sql.streaming.StreamingQueryListener
import org.apache.spark.sql.streaming.StreamingQueryListener.{
  QueryProgressEvent,
  QueryStartedEvent,
  QueryTerminatedEvent
}

import scala.collection.Seq
import scala.util.ScalaJavaConversions.{ListOps, MapOps}
import scala.util.{Failure, Success, Try}

case class DataStream(df: DataFrame, partitions: Int, topicInfo: TopicInfo) {
  def apply(query: api.Query, keys: Seq[String] = null, dataModel: DataModel = DataModel.Events): DataStream = {

    // apply setups
    Option(query.setups).map(_.toScala.map { setup =>
      Try(df.sparkSession.sql(setup)) match {
        case Failure(ex) =>
          println(s"[Failure] Setup command: ($setup) failed with exception: ${ex.toString}")
          ex.printStackTrace(System.out)
        case Success(value) => println(s"[SUCCESS] Setup command: $setup")
      }
    })

    // enrich selects with time columns & keys
    val timeColumn = Option(query.timeColumn).getOrElse(Constants.TimeColumn)
    val timeSelects = Map(Constants.TimeColumn -> timeColumn) ++ dataModel match {
      // these are derived from Mutation class for streaming case - we ignore what is set in conf
      case DataModel.Entities => Map(Constants.ReversalColumn -> null, Constants.MutationTimeColumn -> null)
      case DataModel.Events   => None
    }
    val selectsOption: Option[Map[String, String]] = for {
      selectMap <- Option(query.selects).map(_.toScala.toMap)
      keyMap = Option(keys).map(_.map(k => k -> k).toMap).getOrElse(Map.empty)
    } yield (keyMap ++ selectMap ++ timeSelects)
    val selectClauses = selectsOption.map { _.map { case (name, expr) => s"($expr) AS `$name`" }.toSeq }

    println(s"Applying select clauses: $selectClauses")
    val selectedDf = selectClauses.map { selects => df.selectExpr(selects: _*) }.getOrElse(df)

    // enrich where clauses
    val timeIsPresent = dataModel match {
      case api.DataModel.Entities => s"${Constants.MutationTimeColumn} is NOT NULL"
      case api.DataModel.Events   => s"$timeColumn is NOT NULL"
    }
    val atLeastOneKeyIsPresent =
      keys.map { key => s"${selectsOption.map(_(key)).getOrElse(key)} IS NOT NULL" }.mkString(" OR ")
    val baseWheres = Option(query.wheres).map(_.toScala).getOrElse(Seq.empty[String])
    val whereClauses = baseWheres :+ timeIsPresent :+ s"($atLeastOneKeyIsPresent)"

    println(s"Applying where clauses: $whereClauses")
    val filteredDf = whereClauses.foldLeft(selectedDf)(_.where(_))

    DataStream(filteredDf, partitions, topicInfo)
  }
}

trait DataStreamBuilder {
  def from(topicInfo: TopicInfo)(implicit session: SparkSession, conf: Map[String, String]): DataStream
}

case class TopicInfo(name: String, topicType: String, params: Map[String, String])
object TopicInfo {
  // default topic type is kafka
  // kafka://topic_name/host=X/port=Y should parse into TopicInfo(topic_name, kafka, {host: X, port Y})
  def parse(topic: String): TopicInfo = {
    assert(topic.nonEmpty, s"invalid topic: $topic")
    val (topicType, rest) = if (topic.contains("://")) {
      val tokens = topic.split("://", 2)
      tokens.head -> tokens.last
    } else {
      "kafka" -> topic
    }
    assert(rest.nonEmpty, s"invalid topic: $topic")
    val fields = rest.split("/")
    val topicName = fields.head
    val params = fields.tail.map { f =>
      val kv = f.split("=", 2); kv.head -> kv.last
    }.toMap
    TopicInfo(topicName, topicType, params)
  }

  // TODO move this to tests
  def main(args: Array[String]): Unit = {
    def check(a: TopicInfo, b: TopicInfo): Boolean = {
      if (a != b) {
        print(a)
        print(b)
      }
      a == b
    }
    check(parse("kafka://topic_name/host=X/port=Y"),
          TopicInfo("topic_name", "kafka", Map("host" -> "X", "port" -> "Y")))
    check(parse("topic_name/host=X/port=Y"), TopicInfo("topic_name", "kafka", Map("host" -> "X", "port" -> "Y")))
    check(parse("topic_name"), TopicInfo("topic_name", "kafka", Map.empty))

    // testing selects
    val selectsA: Map[String, String] = Map("a" -> "b", "key1" -> "key1+expr")
    val keysA: Seq[String] = null // Seq("key1", "key2")
    val timesA = Map(Constants.TimeColumn -> "ts")

    val result = for {
      selects <- Option(selectsA)
      keys = Option(keysA).map(_.map(k => k -> k).toMap).getOrElse(Map.empty)
    } yield keys ++ selects ++ timesA

    println(result)
  }
}

object DataStreamBuilder {
  val registry: Map[String, DataStreamBuilder] = Map(
    "kafka" -> KafkaStreamBuilder
    // TODO add kinesis support
    // TODO make this part of online api
  )
}

object KafkaStreamBuilder extends DataStreamBuilder {
  override def from(topicInfo: TopicInfo)(implicit session: SparkSession): DataStream = {
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

class SourceStreamBuilder(sources: Seq[api.Source], streamDecoder: StreamDecoder, streamBuilder: DataStreamBuilder)(
    implicit
    session: SparkSession,
    conf: Map[String, String],
    context: Metrics.Context
) {

  private def decode(dataStream: DataStream): DataStream = {
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
            println(s"Error while decoding streaming events ${ex.printStackTrace()}")
            ingressContext.incrementException(ex)
            null
        }
      }
      .filter(mutation =>
        mutation != null && (!(mutation.before != null && mutation.after != null) || !(mutation.before sameElements mutation.after)))
    val streamSchema = SparkConversions.fromChrononSchema(streamDecoder.schema)
    val des = deserialized
      .flatMap { mutation =>
        Seq(mutation.after, mutation.before)
          .filter(_ != null)
          .map(SparkConversions.toSparkRow(_, streamDecoder.schema, GenericRowHandler.func).asInstanceOf[Row])
      }(RowEncoder(streamSchema))

    dataStream.copy(df = des)
  }

  def build: DataStream = {
    val streamingSources = sources.filter(_.topic != null)
    val topics = streamingSources.map(_.topic)
    assert(
      topics.size == 1,
      s"There should be exactly 1 topic present in a UNION of sources but found ${topics.size}. Topics are $topics")

    val source = streamingSources.head
    val topic = TopicInfo.parse(source.topic)

    val stream = if (source.isSetEvents) {
      val events = source.getEvents
      val stream = streamBuilder.from(topic)
      stream.apply(events.query)
    } else if (source.isSetJoinSource) { // recursively walk underlying joins
      val joinSource = source.getJoinSource
      val query = joinSource.getQuery

      val stream = new JoinSource(joinSource).getDataStream(streamBuilder, streamDecoder, conf)
      stream.apply(joinSource.query)
    }
  }
}
