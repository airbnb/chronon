package ai.chronon.spark.streaming

import ai.chronon.api
import ai.chronon.api.Extensions.{GroupByOps, SourceOps}
import ai.chronon.api._
import ai.chronon.online.Fetcher.Request
import ai.chronon.online.KVStore.PutRequest
import ai.chronon.online._
import ai.chronon.spark.{GenericRowHandler, TableUtils}
import com.google.gson.Gson
import org.apache.spark.api.java.function.{MapPartitionsFunction, VoidFunction2}
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.streaming.DataStreamWriter
import org.apache.spark.sql.types.{BooleanType, LongType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, Encoder, Encoders, Row, SparkSession}

import java.time.format.DateTimeFormatter
import java.time.{Instant, ZoneId, ZoneOffset}
import java.util.Base64
import java.{lang, util}
import scala.concurrent.Await
import scala.concurrent.duration.DurationInt
import scala.util.ScalaJavaConversions.{IteratorOps, JIteratorOps, ListOps, MapOps}

class JoinSourceRunner(groupByConf: api.GroupBy, conf: Map[String, String] = Map.empty, debug: Boolean, lagMillis: Int)(
    implicit
    session: SparkSession,
    apiImpl: Api)
    extends Serializable {

  val context: Metrics.Context = Metrics.Context(Metrics.Environment.GroupByStreaming, groupByConf)

  private case class Schemas(leftStreamSchema: StructType,
                             leftSourceSchema: StructType,
                             joinSchema: StructType,
                             joinSourceSchema: StructType)
      extends Serializable

  val valueZSchema: api.StructType = groupByConf.dataModel match {
    case api.DataModel.Events   => servingInfoProxy.valueChrononSchema
    case api.DataModel.Entities => servingInfoProxy.mutationValueChrononSchema
  }
  val (additionalColumns, eventTimeColumn) = groupByConf.dataModel match {
    case api.DataModel.Entities => Constants.MutationFields.map(_.name) -> Constants.MutationTimeColumn
    case api.DataModel.Events   => Seq.empty[String] -> Constants.TimeColumn
  }

  val keyColumns: Array[String] = groupByConf.keyColumns.toScala.toArray
  val valueColumns: Array[String] = groupByConf.aggregationInputs ++ additionalColumns

  private case class PutRequestHelper(inputSchema: StructType) extends Serializable {
    private val keyIndices: Array[Int] = keyColumns.map(inputSchema.fieldIndex)
    private val valueIndices: Array[Int] = valueColumns.map(inputSchema.fieldIndex)
    private val tsIndex: Int = inputSchema.fieldIndex(eventTimeColumn)
    private val keySparkSchema: StructType = StructType(keyIndices.map(inputSchema))
    private val keySchema: api.StructType = SparkConversions.toChrononStruct("key", keySparkSchema)

    @transient private lazy val keyToBytes: Any => Array[Byte] = AvroConversions.encodeBytes(keySchema, null)
    @transient private lazy val valueToBytes: Any => Array[Byte] =
      AvroConversions.encodeBytes(valueZSchema, null)
    private val streamingDataset: String = groupByConf.streamingDataset

    def toPutRequest(input: Row): KVStore.PutRequest = {
      val keys = keyIndices.map(input.get)
      val values = valueIndices.map(input.get)
      val ts = input.get(tsIndex).asInstanceOf[Long]
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
      KVStore.PutRequest(keyBytes, valueBytes, streamingDataset, Option(ts))
    }
  }

  def outputSchema(inputSchema: StructType, query: api.Query)(implicit session: SparkSession): StructType = {
    if (query.selects == null) {
      inputSchema
    } else {
      val allSelects = query.selects.toScala
      val selects = allSelects.map { case (name, expr) => s"(${expr.toLowerCase}) AS $name" }.toSeq
      session.createDataFrame(session.sparkContext.emptyRDD[Row], inputSchema).selectExpr(selects: _*).schema
    }
  }

  private def enrichQuery(query: Query): Query = {
    val enrichedQuery = query.deepCopy()
    if (groupByConf.streamingSource.get.getJoinSource.getJoin.getLeft.isSetEntities) {
      enrichedQuery.selects.put(Constants.ReversalColumn, Constants.ReversalColumn)
      enrichedQuery.selects.put(Constants.MutationTimeColumn, Constants.MutationTimeColumn)
    } else if (query.isSetTimeColumn) {
      enrichedQuery.selects.put(Constants.TimeColumn, enrichedQuery.timeColumn)
    }
    enrichedQuery
  }

  private def buildSchemas(leftSchema: StructType): Schemas = {
    val source = groupByConf.streamingSource
    assert(source.get.isSetJoinSource, s"No JoinSource found in the groupBy: ${groupByConf.metaData.name}")
    assert(source.isDefined, s"No streaming source present in the groupBy: ${groupByConf.metaData.name}")

    val joinSource: JoinSource = source.get.getJoinSource
    val left: Source = joinSource.getJoin.getLeft
    assert(left.topic != null, s"join source left side should have a topic")

    // for entities there is reversal and mutation column additionally
    val reversalField: StructField = StructField(Constants.ReversalColumn, BooleanType)
    val mutationTsField: StructField = StructField(Constants.MutationTimeColumn, LongType)
    val mutationFields: StructType = StructType(Seq(reversalField, mutationTsField))
    var leftStreamSchema: StructType = leftSchema
    if (left.isSetEntities) {
      leftStreamSchema = StructType(mutationFields ++ leftStreamSchema)
    }
    val leftSourceSchema: StructType = outputSchema(leftStreamSchema, enrichQuery(left.query)) // apply same thing

    // joinSchema = leftSourceSchema ++ joinCodec.valueSchema
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

    Schemas(leftStreamSchema, leftSourceSchema, joinSchema, joinSourceSchema)
  }

  private def servingInfoProxy: GroupByServingInfoParsed =
    apiImpl.buildFetcher(debug).getGroupByServingInfo(groupByConf.getMetaData.getName).get

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

  private case class QueryParts(selects: Option[Seq[String]], wheres: Seq[String])
  private def buildQueryParts(query: Query): QueryParts = {
    val selects = Option(query.selects).map(_.toScala.toMap).orNull
    val timeColumn = Option(query.timeColumn).getOrElse(Constants.TimeColumn)

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

  def buildLeftStreamingQuery(query: api.Query, defaultFieldNames: Seq[String]): String = {
    val queryParts = buildQueryParts(query)
    val streamingInputTable =
      Option(groupByConf.metaData.name).map(_.replaceAll("[^a-zA-Z0-9_]", "_")).orNull + "_stream"
    s"""SELECT
       |  ${queryParts.selects.getOrElse(defaultFieldNames).mkString(",\n  ")}
       |FROM $streamingInputTable
       |WHERE ${queryParts.wheres.map("(" + _ + ")").mkString(" AND ")}
       |""".stripMargin
  }
  def chainedStreamingQuery: DataStreamWriter[Row] = {
    val joinSource = groupByConf.streamingSource.get.getJoinSource
    val left = joinSource.join.left
    val topic = TopicInfo.parse(left.topic)

    val stream = buildStream(topic)
    val decoded = decode(stream)

    val leftStreamingQuery = buildLeftStreamingQuery(left.query, decoded.df.schema.fieldNames.toSeq)

    def applyQuery(df: DataFrame, query: api.Query): DataFrame = {
      val queryParts = buildQueryParts(query)
      println(s"""
           |decoded schema: ${decoded.df.schema.catalogString}
           |queryParts: $queryParts
           |df schema: ${df.schema.prettyJson}
           |""".stripMargin)

      // apply left.query
      val selected = queryParts.selects.map(exprs => df.selectExpr(exprs: _*)).getOrElse(df)
      selected.filter(queryParts.wheres.map("(" + _ + ")").mkString(" AND "))
    }

    val leftSource: Dataset[Row] = applyQuery(decoded.df, left.query)
    // key format joins/<team>/join_name
    val joinRequestName = joinSource.join.metaData.getName.replaceFirst("\\.", "/")
    println(s"Upstream join request name: $joinRequestName")

    val tableUtils = TableUtils(session)
    val reqColumns = tableUtils.getColumnsFromQuery(leftStreamingQuery)

    val leftSchema = StructType(
      decoded.df.schema.filter(field =>
        reqColumns
          // handle nested struct, only the parent struct is needed here
          .map(col => if (col.contains(".")) col.split("\\.")(0) else col)
          // the decoded schema is in lower case
          .map(_.toLowerCase)
          .contains(field.name))
    )

    val leftColumns = leftSchema.fieldNames

    val schemas = buildSchemas(leftSchema)
    val joinChrononSchema = SparkConversions.toChrononSchema(schemas.joinSchema)
    val joinEncoder: Encoder[Row] = RowEncoder(schemas.joinSchema)
    val joinFields = schemas.joinSchema.fieldNames

    println(s"Fetching upstream join to enrich the stream... Fetching lag time: $lagMillis")
    // todo: add proper timestamp to the fetcher
    // leftTimeIndex = leftColumns.indexWhere(_ == eventTimeColumn)
    val enriched = leftSource.mapPartitions(
      new MapPartitionsFunction[Row, Row] {
        var fetcher: Fetcher = null
        override def call(rows: util.Iterator[Row]): util.Iterator[Row] = {
          if (fetcher == null) { fetcher = apiImpl.buildFetcher(debug = debug) }
          val requests = rows.toScala.map { row =>
            val keyMap = row.getValuesMap[AnyRef](leftColumns)
            Request(joinRequestName, keyMap)
          }

          //Wait for parent stream to complete
          Thread.sleep(lagMillis)
          val responsesFuture = fetcher.fetchJoin(requests = requests.toSeq)
          // this might be potentially slower, but spark doesn't work when the internal derivation functionality triggers
          // its own spark session, or when it passes around objects
          val responses = Await.result(responsesFuture, 5.second)

          responses.iterator.map { response =>
            val allFields = response.request.keys ++ response.values.get
            SparkConversions
              .toSparkRow(joinFields.map(f => allFields.getOrElse(f, null)),
                          api.StructType.from("record", joinChrononSchema))
              .asInstanceOf[Row]
          }.toJava
        }
      },
      joinEncoder
    )

    val joinSourceDf = applyQuery(enriched, joinSource.query)
    val writer = joinSourceDf.writeStream.outputMode("append")
    val putRequestHelper = PutRequestHelper(joinSourceDf.schema)

    def emitRequestMetric(request: PutRequest, context: Metrics.Context): Unit = {
      request.tsMillis.foreach { ts: Long =>
        context.distribution(Metrics.Name.FreshnessMillis, System.currentTimeMillis() - ts)
        context.increment(Metrics.Name.RowCount)
        context.distribution(Metrics.Name.ValueBytes, request.valueBytes.length)
        context.distribution(Metrics.Name.KeyBytes, request.keyBytes.length)
      }
    }

    writer.foreachBatch {
      new VoidFunction2[DataFrame, java.lang.Long] {
        var kvStore: KVStore = null
        override def call(df: DataFrame, l: lang.Long): Unit = {
          if (kvStore == null) { kvStore = apiImpl.genKvStore }
          val data = df.collect()
          val putRequests = data.map(putRequestHelper.toPutRequest)
          if (debug) {
            println(s" Final df size to write: ${data.length}")
            println(s" Size of putRequests to kv store- ${putRequests.length}")
          } else {
            putRequests.map(request => emitRequestMetric(request, context.withSuffix("egress")))
            kvStore.multiPut(putRequests)
          }
        }
      }
    }
  }
}
