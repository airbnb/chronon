package ai.chronon.spark.streaming

import ai.chronon.api
import ai.chronon.api.{Constants, DataModel, GroupByServingInfo, JoinSource, Query, QueryUtils, Source}
import ai.chronon.api.Extensions.{GroupByOps, SourceOps}
import ai.chronon.online.Extensions.{ChrononStructTypeOps, StructTypeOps}
import ai.chronon.online.Fetcher.Request
import ai.chronon.online.{
  Api,
  AvroConversions,
  DataStream,
  Fetcher,
  GroupByServingInfoParsed,
  JoinCodec,
  KVStore,
  Metrics,
  Mutation,
  SparkConversions,
  StreamBuilder,
  TopicInfo
}
import ai.chronon.spark.{GenericRowHandler, GroupByUpload, PartitionRange, TableUtils}
import com.google.gson.Gson
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.streaming.{DataStreamWriter, StreamingQuery, Trigger}
import org.apache.spark.sql.types.{BooleanType, LongType, StructField, StructType}
import org.apache.spark.sql.{Dataset, Encoder, Encoders, ForeachWriter, Row, SparkSession, types}

import java.time.{Instant, ZoneId, ZoneOffset}
import java.time.format.DateTimeFormatter
import java.util.Base64
import scala.concurrent.Await
import scala.concurrent.duration.{Duration, DurationInt, TimeUnit}
import scala.util.ScalaJavaConversions.{ListOps, MapOps}

class JoinSourceRunner(groupByConf: api.GroupBy, conf: Map[String, String] = Map.empty, debug: Boolean)(implicit
    session: SparkSession,
    apiImpl: Api)
    extends Serializable {

  val context: Metrics.Context = Metrics.Context(Metrics.Environment.GroupByStreaming, groupByConf)

  case class Schemas(leftSchema: StructType,
                     leftStreamSchema: StructType,
                     leftSourceSchema: StructType,
                     joinSchema: StructType,
                     joinSourceSchema: StructType)
      extends Serializable

  val valueZSchema: api.StructType = groupByConf.dataModel match {
    case api.DataModel.Events   => servingInfoProxy.valueChrononSchema
    case api.DataModel.Entities => servingInfoProxy.mutationValueChrononSchema
  }
  val (additionalColumns, eventTimeColumn) = groupByConf.dataModel match {
    case api.DataModel.Entities => Constants.MutationAvroColumns -> Constants.MutationTimeColumn
    case api.DataModel.Events   => Seq.empty[String] -> Constants.TimeColumn
  }
  val valueColumns = groupByConf.aggregationInputs ++ additionalColumns

  case class PutRequestHelper(inputSchema: StructType) extends Serializable {
    val keyColumns = groupByConf.keyColumns.toScala.toArray
    val keyIndices: Array[Int] = keyColumns.map(inputSchema.fieldIndex).toArray

    val tsIndex: Int = inputSchema.fieldIndex(eventTimeColumn)

    val keySparkSchema: StructType = StructType(keyIndices.map(inputSchema))
    val keySchema: api.StructType = SparkConversions.toChrononStruct("key", keySparkSchema)

    @transient lazy val keyToBytes: Any => Array[Byte] = AvroConversions.encodeBytes(keySchema, null)
    @transient lazy val valueToBytes: Any => Array[Byte] =
      AvroConversions.encodeBytes(valueZSchema, null)
    val streamingDataset: String = groupByConf.streamingDataset

    def toPutRequest(input: Map[String, Any]): KVStore.PutRequest = {
      val keys = keyColumns.map(k => input.get(k).orNull)
      val values = valueColumns.map(v => input.get(v).orNull)
      val ts = input.get(eventTimeColumn).asInstanceOf[Option[Long]].get
      val keyBytes = keyToBytes(keys)
      val valueBytes = valueToBytes(values)
      if (debug) {
        val gson = new Gson()
        val formatter = DateTimeFormatter.ISO_LOCAL_DATE_TIME.withZone(ZoneId.from(ZoneOffset.UTC))
        val pstFormatter = DateTimeFormatter.ISO_LOCAL_DATE_TIME.withZone(ZoneId.of("America/Los_Angeles"))
        println(s"""
             |dataset: $streamingDataset
             |keys: ${gson.toJson(keys)}
             |values: ${gson.toJson(values)}
             |keyBytes: ${Base64.getEncoder.encodeToString(keyBytes)}
             |valueBytes: ${Base64.getEncoder.encodeToString(valueBytes)}
             |ts: $ts|  UTC: ${formatter.format(Instant.ofEpochMilli(ts))}| PST: ${pstFormatter.format(
          Instant.ofEpochMilli(ts))}
             |""".stripMargin)
      }
      // UNDO
      println(s"chained value schema for encoding : ${valueZSchema.catalogString}")
      KVStore.PutRequest(keyBytes, valueBytes, streamingDataset, Option(ts))
    }
  }

  def outputSchema(inputSchema: StructType, query: api.Query)(implicit session: SparkSession): StructType = {
    if (query.selects == null) {
      inputSchema
    } else {
      val allSelects = query.selects.toScala
      val selects = allSelects.map { case (name, expr) => s"($expr) AS $name" }.toSeq
      session.createDataFrame(session.sparkContext.emptyRDD[Row], inputSchema).selectExpr(selects: _*).schema
    }
  }

  def enrichQuery(query: Query): Query = {
    val enrichedQuery = query.deepCopy()
    if (groupByConf.streamingSource.get.getJoinSource.getJoin.getLeft.isSetEntities) {
      enrichedQuery.selects.put(Constants.ReversalColumn, Constants.ReversalColumn)
      enrichedQuery.selects.put(Constants.MutationTimeColumn, Constants.MutationTimeColumn)
    } else {
      enrichedQuery.selects.put(Constants.TimeColumn, enrichedQuery.timeColumn)
    }
    enrichedQuery
  }

  def buildSchemas: Schemas = {
    val source = groupByConf.streamingSource
    assert(source.get.isSetJoinSource, s"No JoinSource found in the groupBy: ${groupByConf.metaData.name}")
    assert(source.isDefined, s"No streaming source present in the groupBy: ${groupByConf.metaData.name}")

    val joinSource: JoinSource = source.get.getJoinSource
    val left: Source = joinSource.getJoin.getLeft
    assert(left.topic != null, s"join source left side should have a topic")
    val tableUtils: TableUtils = TableUtils(session)
    val leftSchema: StructType = tableUtils.getSchemaFromTable(left.table)

    // for entities there is reversal and mutation column additionally
    val reversalField: StructField = StructField(Constants.ReversalColumn, BooleanType)
    val mutationTsField: StructField = StructField(Constants.MutationTimeColumn, LongType)
    val mutationFields: StructType = StructType(Seq(reversalField, mutationTsField))
    var leftStreamSchema: StructType = leftSchema
    if (left.isSetEntities) {
      leftStreamSchema = StructType(mutationFields ++ leftStreamSchema)
    }
    val leftSourceSchema: StructType = outputSchema(leftStreamSchema, enrichQuery(left.query)) // apply same thing

    // joinSchema = leftStreamSchema ++ joinCodec.valueSchema
    val joinCodec: JoinCodec = apiImpl.buildFetcher(debug).buildJoinCodec(joinSource.getJoin)
    val joinValueSchema: StructType = SparkConversions.fromChrononSchema(joinCodec.valueSchema)
    val joinSchema: StructType = StructType(leftSourceSchema ++ joinValueSchema)
    val joinSourceSchema: StructType = outputSchema(joinSchema, enrichQuery(joinSource.query))

    // GroupBy -> JoinSource (Join + outer_query)
    // Join ->
    //   Join.left -> (left.(table, mutation_stream, etc) + inner_query)
    println(s"""
       |Schemas across chain of transformations
       |leftSchema:
       |  ${leftSchema.catalogString}
       |left stream Schema:
       |  ${leftStreamSchema.catalogString}
       |left schema after applying left query:
       |  ${leftSourceSchema.catalogString}
       |join schema:
       |  ${joinSchema.catalogString}
       |join schema after applying joinSource.query:
       |  ${joinSourceSchema.catalogString}
       |""".stripMargin)

    Schemas(leftSchema, leftStreamSchema, leftSourceSchema, joinSchema, joinSourceSchema)
  }

  private def servingInfoProxy: GroupByServingInfoParsed =
    GroupByUpload.buildServingInfo(groupByConf,
                                   session,
                                   TableUtils(session).partitionSpec.at(System.currentTimeMillis()))

  private def decode(dataStream: DataStream): DataStream = {
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
         | Streaming source: ${groupByConf.streamingSource.get}
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

  case class QueryParts(selects: Option[Seq[String]], wheres: Seq[String])
  private def buildQueryParts(query: Query): QueryParts = {
    val selects = Option(query.selects).map(_.toScala.toMap).orNull
    val timeColumn = Option(query.timeColumn).getOrElse(Constants.TimeColumn)
    val keys = groupByConf.getKeyColumns.toScala

    val fillIfAbsent = (groupByConf.dataModel match {
      case DataModel.Entities =>
        Map(Constants.ReversalColumn -> Constants.ReversalColumn,
            Constants.MutationTimeColumn -> Constants.MutationTimeColumn)
      case DataModel.Events => Map(Constants.TimeColumn -> timeColumn)
    })

    val baseWheres = Option(query.wheres).map(_.toScala).getOrElse(Seq.empty[String])
    val timeWheres = groupByConf.dataModel match {
      case DataModel.Entities => Seq(s"${Constants.MutationTimeColumn} is NOT NULL")
      case DataModel.Events   => Seq(s"$timeColumn is NOT NULL")
    }
    val wheres = baseWheres ++ timeWheres

    val allSelects = Option(selects).map(fillIfAbsent ++ _).map { m =>
      m.map {
        case (name, expr) => s"($expr) AS $name"
      }.toSeq
    }
    QueryParts(allSelects, wheres)
  }

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

  private def buildStream(topic: TopicInfo): DataStream =
    internalStreamBuilder(topic.topicType).from(topic)(session, conf)

  // enrich left with fetchJoin + apply joinSource.query + put kv bytes
  class ChainedWriter extends ForeachWriter[Row] {
    private val joinSource = groupByConf.streamingSource.get.getJoinSource
    private val schemas = buildSchemas
    private val leftColumns = schemas.leftSourceSchema.fieldNames
    private val leftTimeIndex = leftColumns.indexWhere(_ == eventTimeColumn)
    private val joinRequestName = joinSource.join.metaData.getName
    private val joinOverrides = Map(joinRequestName -> joinSource.join)
    var joinCodec: JoinCodec = _
    var fetcher: Fetcher = _
    var kvStore: KVStore = _
    var putRequestHelper: PutRequestHelper = _

    override def open(partitionId: Long, epochId: Long): Boolean = {
      println("initialized chained writer")
      fetcher = apiImpl.buildFetcher(debug)
      joinCodec = fetcher.buildJoinCodec(joinSource.getJoin)
      kvStore = apiImpl.genKvStore
      putRequestHelper = PutRequestHelper(schemas.joinSourceSchema)
      true
    }

    override def process(row: Row): Unit = {
      val keyMap = row.getValuesMap[AnyRef](leftColumns)
      // name matches putJoinConf/getJoinConf logic in MetadataStore.scala
      val responsesFuture =
        fetcher.fetchJoin(requests = Seq(Request(joinRequestName, keyMap, Option(row.getLong(leftTimeIndex)))))
      implicit val ec = fetcher.executionContext
      // we don't exit the future land - because awaiting will stall the calling thread in spark streaming
      // we instead let the future run its course asynchronously - we apply all the sql using catalyst instead.
      responsesFuture.foreach { responses =>
        responses.foreach { response =>
          val ts = response.request.atMillis.get
          response.values.failed.foreach { ex =>
            ex.printStackTrace(System.out); context.incrementException(ex)
          }
          response.values
            .foreach { valuesMap =>
              val derived = joinCodec.deriveFunc(Map.empty, keyMap ++ Map(Constants.TimeColumn -> ts) ++ valuesMap)
              val putRequest =
                putRequestHelper.toPutRequest(keyMap ++ valuesMap ++ Map(Constants.TimeColumn -> ts) ++ derived)
              if (debug) {
                println(s"""
                     |input keys: $keyMap
                     |input values: $valuesMap
                     |derived map: $derived
                     |""".stripMargin)
              }
              kvStore.put(putRequest)
            }
        }
      }
      if (debug) {
        Await.result(responsesFuture, 5.second)
      }
    }

    override def close(errorOrNull: Throwable): Unit = {}
  }

  def chainedStreamingQuery: DataStreamWriter[Row] = {
    val joinSource = groupByConf.streamingSource.get.getJoinSource
    val left = joinSource.join.left
    val topic = TopicInfo.parse(left.topic)

    val stream = buildStream(topic)
    val decoded = decode(stream)
    val queryParts = buildQueryParts(left.query)
    println(s"""
         |decoded schema: ${decoded.df.schema.catalogString}
         |Left QueryParts: $queryParts
         |""".stripMargin)

    // apply left.query
    val selected = queryParts.selects.map(exprs => decoded.df.selectExpr(exprs: _*)).getOrElse(decoded.df)
    val filtered = selected.filter(queryParts.wheres.map("(" + _ + ")").mkString(" AND "))

    filtered.writeStream
      .outputMode("append")
      .trigger(Trigger.Continuous(2.minute))
      .foreach(new ChainedWriter)
  }
}
