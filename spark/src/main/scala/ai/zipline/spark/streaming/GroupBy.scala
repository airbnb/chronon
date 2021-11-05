package ai.zipline.spark.streaming

import ai.zipline.api
import ai.zipline.api.Extensions.{GroupByOps, SourceOps}
import ai.zipline.api.{Constants, Mutation, OnlineImpl, QueryUtils, ThriftJsonCodec}
import ai.zipline.fetcher.{Metrics => FetcherMetrics, Fetcher}
import ai.zipline.spark.Conversions
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.{DataFrame, Dataset, Encoder, Encoders, Row, SparkSession}
import org.apache.thrift.TBase
import org.rogach.scallop.{ScallopConf, ScallopOption}

import scala.collection.JavaConverters._
import scala.reflect.ClassTag

class StreamingArgs(args: Seq[String]) extends ScallopConf(args) {
  val userConf: Map[String, String] = props[String]('D')
  val confPath: ScallopOption[String] = opt[String](required = true)
  val kafkaHost: ScallopOption[String] = opt[String](required = true)
  val mockWrites: ScallopOption[Boolean] = opt[Boolean](required = false, default = Some(false))
  val debug: ScallopOption[Boolean] = opt[Boolean](required = false, default = Some(false))
  val local: ScallopOption[Boolean] = opt[Boolean](required = false, default = Some(false))
  def parseConf[T <: TBase[_, _]: Manifest: ClassTag]: T =
    ThriftJsonCodec.fromJsonFile[T](confPath(), check = true)
}

object GroupBy {
  def buildSession(appName: String, local: Boolean): SparkSession = {
    val baseBuilder = SparkSession
      .builder()
      .appName(appName)
      .config("spark.sql.session.timeZone", "UTC")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.kryo.registrator", "ai.zipline.spark.ZiplineKryoRegistrator")
      .config("spark.kryoserializer.buffer.max", "2000m")
      .config("spark.kryo.referenceTracking", "false")

    val builder = if (local) {
      baseBuilder
      // use all threads - or the tests will be slow
        .master("local[*]")
        .config("spark.local.dir", s"/tmp/zipline-spark-streaming")
        .config("spark.kryo.registrationRequired", "true")
    } else {
      baseBuilder
    }
    builder.getOrCreate()
  }

  def dataStream(session: SparkSession, host: String, topic: String): DataFrame = {
    session.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", host)
      .option("subscribe", topic)
      .option("enable.auto.commit", "true")
      .load()
      .selectExpr("value")
  }

  def run(streamingArgs: StreamingArgs, onlineImpl: OnlineImpl): Unit = {
    val groupByConf: api.GroupBy = streamingArgs.parseConf[api.GroupBy]
    val session: SparkSession = buildSession(groupByConf.metaData.name, streamingArgs.local())
    val streamingSource = groupByConf.streamingSource
    assert(streamingSource.isDefined, "There is no valid streaming source - with a valid topic, and endDate < today")
    val inputStream: DataFrame = dataStream(session, streamingArgs.kafkaHost(), streamingSource.get.topic)
    val streamingRunner =
      new GroupBy(inputStream, session, groupByConf, onlineImpl, streamingArgs.debug(), streamingArgs.mockWrites())
    streamingRunner.run()
  }
}

class GroupBy(inputStream: DataFrame,
              session: SparkSession,
              groupByConf: api.GroupBy,
              onlineImpl: api.OnlineImpl,
              debug: Boolean = false,
              mockWrites: Boolean = false)
    extends Serializable {

  private def buildStreamingQuery(): String = {
    val streamingSource = groupByConf.streamingSource.get
    val query = streamingSource.query
    val selects = Option(query.selects).map(_.asScala.toMap).orNull
    val timeColumn = Option(query.timeColumn).getOrElse(Constants.TimeColumn)
    val fillIfAbsent = if (selects == null) null else Map(Constants.TimeColumn -> timeColumn)
    val keys = groupByConf.getKeyColumns.asScala

    val baseWheres = Option(query.wheres).map(_.asScala).getOrElse(Seq.empty[String])
    val keyWhereOption =
      Option(selects)
        .map { selectsMap =>
          keys
            .map(key => s"(${selectsMap(key)} is NOT NULL)")
            .mkString(" OR ")
        }
    val timeWheres = Seq(s"$timeColumn is NOT NULL")

    QueryUtils.build(
      selects,
      Constants.StreamingInputTable,
      baseWheres ++ timeWheres ++ keyWhereOption,
      fillIfAbsent = fillIfAbsent
    )
  }

  class UnCopyableRow(row: api.Row) extends Row {
    override def length: Int = row.length

    override def get(i: Int): Any = row.get(i)

    // copy is shallow, so real mutations to the contents are not allowed
    // There is no copy requirement in streaming.
    override def copy(): Row = new UnCopyableRow(row)
  }

  def run(): Unit = {
    val kvStore = onlineImpl.genKvStore
    val fetcher = new Fetcher(kvStore)
    val groupByServingInfo = fetcher.getGroupByServingInfo(groupByConf.getMetaData.getName)
    val streamDecoder = onlineImpl.streamDecoder(groupByServingInfo.groupBy, groupByServingInfo.inputZiplineSchema)
    assert(groupByConf.streamingSource.isDefined,
           "No streaming source defined in GroupBy. Please set a topic/mutationTopic.")
    val streamingSource = groupByConf.streamingSource.get
    val streamingQuery = buildStreamingQuery()

    val context = FetcherMetrics.Context(groupBy = groupByConf.getMetaData.getName)

    import session.implicits._
    implicit val structTypeEncoder: Encoder[Mutation] = Encoders.kryo[api.Mutation]

    val deserialized: Dataset[api.Mutation] = inputStream
      .as[Array[Byte]]
      .map { streamDecoder.decode }
      .filter(mutation => mutation.before != mutation.after)

    val streamSchema = Conversions.fromZiplineSchema(streamDecoder.schema)
    println(s"""
        | group by serving info: $groupByServingInfo
        | Streaming source: $streamingSource
        | streaming Query: $streamingQuery
        | streaming dataset: ${groupByConf.streamingDataset}
        | input zipline schema: ${groupByServingInfo.inputZiplineSchema}
        |""".stripMargin)

    val des = deserialized
      .map { mutation => new UnCopyableRow(mutation.after).asInstanceOf[Row] }(RowEncoder(streamSchema))
    des.createOrReplaceTempView(Constants.StreamingInputTable)
    val selectedDf = session.sql(streamingQuery)
    assert(selectedDf.schema.fieldNames.contains(Constants.TimeColumn),
           s"time column ${Constants.TimeColumn} must be included in the selects")
    val fields = groupByServingInfo.selectedZiplineSchema.fields.map(_.name)
    val keys = groupByConf.keyColumns.asScala.toArray
    val keyIndices = keys.map(selectedDf.schema.fieldIndex)
    val valueIndices = fields.map(selectedDf.schema.fieldIndex)
    val tsIndex = selectedDf.schema.fieldIndex(Constants.TimeColumn)
    val streamingDataset = groupByConf.streamingDataset

    selectedDf
      .map { row =>
        val keys = keyIndices.map(row.get)
        val keyBytes = groupByServingInfo.keyCodec.encodeArray(keys)
        val values = valueIndices.map(row.get)
        val valueBytes = groupByServingInfo.selectedCodec.encodeArray(values)
        val ts = row.get(tsIndex).asInstanceOf[Long]
        api.KVStore.PutRequest(keyBytes, valueBytes, streamingDataset, Option(ts))
      }
      .writeStream
      .outputMode("append")
      .foreach(new DataWriter(onlineImpl, groupByServingInfo, context, debug, mockWrites))
      .start()
  }
}
