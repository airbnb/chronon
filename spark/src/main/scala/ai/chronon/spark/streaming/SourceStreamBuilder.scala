package ai.chronon.spark.streaming

import ai.chronon
import ai.chronon.api
import ai.chronon.api.{Constants, GroupByServingInfo, StructField, StructType}
import ai.chronon.api.Extensions.{GroupByOps, JoinOps, MetadataOps, SourceOps}
import ai.chronon.online.Fetcher.Request
import ai.chronon.online.{Api, AvroConversions, CatalystUtil, DataStream, DataStreamBuilder, Fetcher, GroupByServingInfoParsed, Metrics, Mutation, SparkConversions, StreamDecoder, TopicInfo}
import ai.chronon.spark.{GenericRowHandler, GroupBy, PartitionRange, TableUtils}
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.apache.spark.sql.{Dataset, Encoder, Encoders, Row, SparkSession}
import org.apache.spark.sql.streaming.StreamingQueryListener
import org.apache.spark.sql.streaming.StreamingQueryListener.{QueryProgressEvent, QueryStartedEvent, QueryTerminatedEvent}
import org.apache.spark.sql.{types => stypes}

import scala.collection.Seq
import scala.concurrent.Await
import scala.util.ScalaJavaConversions.{ListOps, MapOps}
import scala.util.{Failure, Success}

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

class SourceStreamBuilder(streamDecoder: StreamDecoder, debug: Boolean)(implicit
    session: SparkSession,
    apiImpl: Api,
    conf: Map[String, String],
    context: Metrics.Context
) {
  // input data stream needs to be a dataframe - Array[Byte]
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
      .filter { mutation =>
        val beforeAndAfterAreNull = mutation.before != null && mutation.after != null
        val beforeAndAfterAreSame = mutation.before sameElements mutation.after
        mutation != null && (!beforeAndAfterAreNull || !beforeAndAfterAreSame)
      }
    val streamSchema = SparkConversions.fromChrononSchema(streamDecoder.schema)
    val des = deserialized
      .flatMap { mutation =>
        Seq(mutation.after, mutation.before)
          .filter(_ != null)
          .map(SparkConversions.toSparkRow(_, streamDecoder.schema, GenericRowHandler.func).asInstanceOf[Row])
      }(RowEncoder(streamSchema))
    dataStream.copy(df = des)
  }

  def build(sources: Seq[api.Source]): DataStream = {
    val streamingSources = sources.filter(_.topic != null)
    val topics = streamingSources.map(_.topic)
    assert(
      topics.size == 1,
      s"There should be exactly 1 topic present in a UNION of sources but found ${topics.size}. Topics are $topics")
    build(streamingSources.head)
  }

  private def internalStreamBuilder(streamType: String): DataStreamBuilder = {
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

  def buildStream(topic: TopicInfo): DataStream = internalStreamBuilder(topic.topicType).from(topic)

  case class GroupByMeta(servingInfo: GroupByServingInfoParsed) {
    val groupByConf: api.GroupBy = servingInfo.groupBy
    val keys: Array[String] = groupByConf.keyColumns.toScala.toArray
    def selectedFieldIndex(fieldName: String): Int = {
      Constants.
      lazy val groupBy = GroupBy.from(groupByConf, PartitionRange(endDs, endDs), TableUtils(session))
      servingInfo.selectedChrononSchema.indexWhere(_.name == fieldName)
    }

    val keyIndices: Array[Int] = keys.map(selectedFieldIndex)
    val (additionalColumns, eventTimeColumn) = groupByConf.dataModel match {
      case chronon.api.DataModel.Entities => servingInfo.MutationAvroColumns -> Constants.MutationTimeColumn
      case chronon.api.DataModel.Events   => Seq.empty[String] -> Constants.TimeColumn
    }
    val valueColumns = groupByConf.aggregationInputs ++ additionalColumns
    val valueIndices = valueColumns.map(selectedFieldIndex)

    val tsIndex = selectedFieldIndex.schema.fieldIndex(eventTimeColumn)
    val streamingDataset = groupByConf.streamingDataset

    val keyZSchema: api.StructType = servingInfo.keyChrononSchema
    val valueZSchema: api.StructType = groupByConf.dataModel match {
      case chronon.api.DataModel.Events   => servingInfo.valueChrononSchema
      case chronon.api.DataModel.Entities => servingInfo.mutationValueChrononSchema
    }

    val keyToBytes = AvroConversions.encodeBytes(keyZSchema, GenericRowHandler.func)
    val valueToBytes = AvroConversions.encodeBytes(valueZSchema, GenericRowHandler.func)
  }

  @transient lazy val fetcher = apiImpl.buildFetcher(debug)

  def build(source: api.Source): DataStream = {
    val topic = TopicInfo.parse(source.topic)
    val stream = if (source.isSetEvents || source.isSetEntities) { // boundary condition
      val stream = buildStream(topic)
      val decoded = decode(stream)
      decoded.apply(source.query)
    } else if (source.isSetJoinSource) { // recursive condition
      val joinSource = source.getJoinSource
      val join = joinSource.getJoin
      val leftStream = build(joinSource.getJoin.getLeft)
      val keyColumns = new JoinOps(join).leftKeyCols.toSeq
      val leftSchema = leftStream.df.schema
      val leftColumns = leftSchema.fieldNames.toSeq
      assert(keyColumns.forall(leftSchema.fieldNames.contains),
             s"All keys needed by joinParts are not present in left. Keys: $keyColumns, left: ${leftColumns}")

      val joinRequestName = join.metaData.nameToFilePath
      val joinOverrides = Map(joinRequestName -> join)
      val timeIndex = leftSchema.fieldIndex(Constants.TimeColumn)

      // schemas
      val joinCodec = fetcher.buildJoinCodec(join)
      val joinFields: Seq[stypes.StructField] = leftSchema.fields.toSeq ++ joinCodec.valueSchema.fields
      val joinFieldNames = joinFields.map(_.name).toArray
      val joinSparkSchema: stypes.StructType = stypes.StructType(joinFields.toArray)
      val joinChrononSchema = SparkConversions.toChrononSchema(joinSparkSchema)

      // catalyst util
      val joinSourceSelects = joinSource.getQuery.selects
      assert(joinSourceSelects != null,
             s"Please specify selects of necessary columns for your join source $joinRequestName")
      lazy val postJoinCatalystUtil = new CatalystUtil(
        expressions = joinSourceSelects.toScala.toSeq,
        inputSchema = StructType("join_output", joinChrononSchema.map { case (n, t) => StructField(n, t) }),
        filters = Option(joinSource.getQuery.getWheres).map(_.toScala.toSeq).getOrElse(Seq.empty)
      )

      val enrichedDf = leftStream.df.rdd.map { row =>
        val keyMap = row.getValuesMap[AnyRef](leftColumns)
        // name matches putJoinConf/getJoinConf logic in MetadataStore.scala
        val responsesFuture =
          fetcher.fetchJoin(requests = Seq(Request(joinRequestName, keyMap, Option(row.getLong(timeIndex)))),
                            joinOverrides = joinOverrides)
        implicit val ec = fetcher.executionContext
        // we don't exit the future land - because awaiting will stall the calling thread in spark streaming
        // we instead let the future run its course asynchronously - we apply all the sql using catalyst instead.
        responsesFuture.foreach { responses =>
          responses.foreach { response =>
            val ts = response.request.atMillis.get
            val rowTry = response.values.map { valuesMap =>
              val fullMap = valuesMap ++ keyMap ++ Map(Constants.TimeColumn -> ts)
              val postQueryMapOpt = postJoinCatalystUtil.performSql(fullMap)
              // TODO: write to kv store
            }
            rowTry match {
              case Failure(exception) => exception.printStackTrace(System.out); None
              case Success(value)     => Some(value)
            }
          }
        }

      }
      val postQuery = joinSource.getQuery

      val stream = new JoinSource(joinSource).getDataStream(streamBuilder, streamDecoder, conf)
      stream.apply(joinSource.query)
    }
  }
}
