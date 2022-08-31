package ai.chronon.online

import ai.chronon.aggregator.row.ColumnAggregator
import ai.chronon.aggregator.windowing
import ai.chronon.aggregator.windowing.{FinalBatchIr, SawtoothOnlineAggregator}
import ai.chronon.api.Constants.ChrononMetadataKey
import ai.chronon.api.Extensions.JoinOps
import ai.chronon.api._
import ai.chronon.online.Fetcher._
import ai.chronon.online.KVStore.{GetRequest, GetResponse, TimedValue}
import ai.chronon.online.Metrics.Name
import com.google.gson.Gson
import org.apache.avro.generic.GenericRecord

import java.io.{PrintWriter, StringWriter}
import java.util
import java.util.function.Consumer
import scala.collection.JavaConverters.asScalaIteratorConverter
import scala.collection.mutable
import scala.concurrent.Future
import scala.util.hashing.MurmurHash3
import scala.util.{Failure, Success, Try}

object Fetcher {
  case class Request(name: String,
                     keys: Map[String, AnyRef],
                     atMillis: Option[Long] = None,
                     context: Option[Metrics.Context] = None)
  case class Response(request: Request, values: Try[Map[String, AnyRef]])
}

class BaseFetcher(kvStore: KVStore,
                  metaDataSet: String = ChrononMetadataKey,
                  timeoutMillis: Long = 10000,
                  debug: Boolean = false)
    extends MetadataStore(kvStore, metaDataSet, timeoutMillis) {

  private case class GroupByRequestMeta(
      groupByServingInfoParsed: GroupByServingInfoParsed,
      batchRequest: GetRequest,
      streamingRequestOpt: Option[GetRequest],
      endTs: Option[Long],
      context: Metrics.Context
  )

  // a groupBy request is split into batchRequest and optionally a streamingRequest
  // this method decodes bytes (of the appropriate avro schema) into chronon rows aggregates further if necessary
  private def constructGroupByResponse(batchResponsesTry: Try[Seq[TimedValue]],
                                       streamingResponsesOpt: Option[Seq[TimedValue]],
                                       oldServingInfo: GroupByServingInfoParsed,
                                       queryTimeMs: Long,
                                       startTimeMs: Long,
                                       overallLatency: Long,
                                       context: Metrics.Context,
                                       totalResponseValueBytes: Int): Map[String, AnyRef] = {
    val latestBatchValue = batchResponsesTry.map(_.maxBy(_.millis))
    val servingInfo =
      latestBatchValue.map(timedVal => updateServingInfo(timedVal.millis, oldServingInfo)).getOrElse(oldServingInfo)
    batchResponsesTry.map {
      reportKvResponse(context.withSuffix("batch"), _, queryTimeMs, overallLatency, totalResponseValueBytes)
    }
    // bulk upload didn't remove an older batch value - so we manually discard
    val batchBytes: Array[Byte] = batchResponsesTry
      .map(_.maxBy(_.millis))
      .filter(_.millis >= servingInfo.batchEndTsMillis)
      .map(_.bytes)
      .getOrElse(null)
    val responseMap: Map[String, AnyRef] = if (servingInfo.groupBy.aggregations == null) { // no-agg
      servingInfo.selectedCodec.decodeMap(batchBytes)
    } else if (streamingResponsesOpt.isEmpty) { // snapshot accurate
      servingInfo.outputCodec.decodeMap(batchBytes)
    } else { // temporal accurate
      val streamingResponses = streamingResponsesOpt.get
      val mutations: Boolean = servingInfo.groupByOps.dataModel == DataModel.Entities
      val aggregator: SawtoothOnlineAggregator = servingInfo.aggregator
      val selectedCodec = servingInfo.groupByOps.dataModel match {
        case DataModel.Events   => servingInfo.valueAvroCodec
        case DataModel.Entities => servingInfo.mutationValueAvroCodec
      }
      if (batchBytes == null && (streamingResponses == null || streamingResponses.isEmpty)) {
        null
      } else {
        val streamingRows: Iterator[Row] = streamingResponses.iterator
          .filter(tVal => tVal.millis >= servingInfo.batchEndTsMillis)
          .map(tVal => selectedCodec.decodeRow(tVal.bytes, tVal.millis, mutations))
        reportKvResponse(context.withSuffix("streaming"),
                         streamingResponses,
                         queryTimeMs,
                         overallLatency,
                         totalResponseValueBytes)
        val batchIr = toBatchIr(batchBytes, servingInfo)
        val output = aggregator.lambdaAggregateFinalized(batchIr, streamingRows, queryTimeMs, mutations)
        servingInfo.outputCodec.fieldNames.zip(output.map(_.asInstanceOf[AnyRef])).toMap
      }
    }
    context.histogram("group_by.latency.millis", System.currentTimeMillis() - startTimeMs)
    responseMap
  }

  def reportKvResponse(ctx: Metrics.Context,
                       response: Seq[TimedValue],
                       queryTsMillis: Long,
                       latencyMillis: Long,
                       totalResponseBytes: Int): Unit = {
    val latestResponseTs = response.iterator.map(_.millis).reduceOption(_ max _)
    val responseBytes = response.iterator.map(_.bytes.length).sum
    val context = ctx.withSuffix("response")
    context.histogram(Name.RowCount, response.length)
    context.histogram(Name.Bytes, responseBytes)
    latestResponseTs.foreach { ts =>
      context.histogram(Name.FreshnessMillis, queryTsMillis - ts)
      context.histogram(Name.FreshnessMinutes, (queryTsMillis - ts) / 60000)
    }
    context.histogram("attributed_latency.millis",
                      (responseBytes.toDouble / totalResponseBytes.toDouble) * latencyMillis)
  }

  private def updateServingInfo(batchEndTs: Long,
                                groupByServingInfo: GroupByServingInfoParsed): GroupByServingInfoParsed = {
    val name = groupByServingInfo.groupBy.metaData.name
    if (batchEndTs > groupByServingInfo.batchEndTsMillis) {
      println(s"""$name's value's batch timestamp of $batchEndTs is
           |ahead of schema timestamp of ${groupByServingInfo.batchEndTsMillis}.
           |Forcing an update of schema.""".stripMargin)
      getGroupByServingInfo
        .force(name)
        .recover {
          case ex: Throwable =>
            println(
              s"Couldn't update GroupByServingInfo of $name due to ${ex.getMessage}. Proceeding with the old one.")
            ex.printStackTrace()
            groupByServingInfo
        }
        .get
    } else {
      groupByServingInfo
    }
  }

  // 1. fetches GroupByServingInfo
  // 2. encodes keys as keyAvroSchema
  // 3. Based on accuracy, fetches streaming + batch data and aggregates further.
  // 4. Finally converted to outputSchema
  def fetchGroupBys(requests: scala.collection.Seq[Request]): Future[scala.collection.Seq[Response]] = {
    // split a groupBy level request into its kvStore level requests
    val groupByRequestToKvRequest: Seq[(Request, Try[GroupByRequestMeta])] = requests.iterator.map { request =>
      val groupByRequestMetaTry: Try[GroupByRequestMeta] = getGroupByServingInfo(request.name)
        .map { groupByServingInfo =>
          val context =
            request.context.getOrElse(Metrics.Context(Metrics.Environment.GroupByFetching, groupByServingInfo.groupBy))
          context.increment("group_by_request.count")
          var keyBytes: Array[Byte] = null
          try {
            keyBytes = groupByServingInfo.keyCodec.encode(request.keys)
          } catch {
            // TODO: only gets hit in cli path - make this code path just use avro schema to decode keys directly in cli
            // TODO: Remove this code block
            case ex: Exception =>
              val castedKeys = groupByServingInfo.keyChrononSchema.fields.map {
                case StructField(name, typ) => name -> ColumnAggregator.castTo(request.keys.getOrElse(name, null), typ)
              }.toMap
              try {
                keyBytes = groupByServingInfo.keyCodec.encode(castedKeys)
              } catch {
                case exInner: Exception =>
                  exInner.addSuppressed(ex)
                  throw new RuntimeException("Couldn't encode request keys or casted keys", exInner)
              }
          }
          val batchRequest = GetRequest(keyBytes, groupByServingInfo.groupByOps.batchDataset)
          val streamingRequestOpt = groupByServingInfo.groupByOps.inferredAccuracy match {
            // fetch batch(ir) and streaming(input) and aggregate
            case Accuracy.TEMPORAL =>
              Some(
                GetRequest(keyBytes,
                           groupByServingInfo.groupByOps.streamingDataset,
                           Some(groupByServingInfo.batchEndTsMillis)))
            // no further aggregation is required - the value in KvStore is good as is
            case Accuracy.SNAPSHOT => None
          }
          GroupByRequestMeta(groupByServingInfo, batchRequest, streamingRequestOpt, request.atMillis, context)
        }
      if (groupByRequestMetaTry.isFailure) {
        request.context.foreach(_.increment("group_by_serving_info_failure.count"))
      }
      request -> groupByRequestMetaTry
    }.toSeq
    val allRequests: Seq[GetRequest] = groupByRequestToKvRequest.flatMap {
      case (_, Success(GroupByRequestMeta(_, batchRequest, streamingRequestOpt, _, _))) =>
        Some(batchRequest) ++ streamingRequestOpt
      case _ => Seq.empty
    }

    val startTimeMs = System.currentTimeMillis()
    val kvResponseFuture: Future[Seq[GetResponse]] = kvStore.multiGet(allRequests)
    kvResponseFuture
      .map { kvResponses: Seq[GetResponse] =>
        val multiGetMillis = System.currentTimeMillis() - startTimeMs
        val responsesMap: Map[GetRequest, Try[Seq[TimedValue]]] = kvResponses.map { response =>
          response.request -> response.values
        }.toMap
        val totalResponseValueBytes =
          responsesMap.iterator.map(_._2).filter(_.isSuccess).flatMap(_.get.map(_.bytes.length)).sum
        val responses: Seq[Response] = groupByRequestToKvRequest.iterator.map {
          case (request, requestMetaTry) =>
            val responseMapTry = requestMetaTry.map { requestMeta =>
              val GroupByRequestMeta(groupByServingInfo, batchRequest, streamingRequestOpt, _, context) = requestMeta
              context.count("multi_get.batch.size", allRequests.length)
              context.histogram("multi_get.bytes", totalResponseValueBytes)
              context.histogram("multi_get.response.length", kvResponses.length)
              context.histogram("multi_get.latency.millis", multiGetMillis)
              // pick the batch version with highest timestamp
              val batchResponseTryAll = responsesMap
                .getOrElse(batchRequest,
                           Failure(
                             new IllegalStateException(
                               s"Couldn't find corresponding response for $batchRequest in responseMap")))
              val streamingResponsesOpt =
                streamingRequestOpt.map(responsesMap.getOrElse(_, Success(Seq.empty)).getOrElse(Seq.empty))
              val queryTs = request.atMillis.getOrElse(System.currentTimeMillis())
              try {
                constructGroupByResponse(batchResponseTryAll,
                                         streamingResponsesOpt,
                                         groupByServingInfo,
                                         queryTs,
                                         startTimeMs,
                                         multiGetMillis,
                                         context,
                                         totalResponseValueBytes)
              } catch {
                case ex: Exception =>
                  // not all exceptions are due to stale schema, so we want to control how often we hit kv store
                  getGroupByServingInfo.refresh(groupByServingInfo.groupByOps.metaData.name)
                  context.incrementException(ex)
                  ex.printStackTrace()
                  throw ex
              }
            }

            Response(request, responseMapTry)
        }.toList
        responses
      }
  }

  def toBatchIr(bytes: Array[Byte], gbInfo: GroupByServingInfoParsed): FinalBatchIr = {
    if (bytes == null) return null
    val batchRecord =
      AvroConversions
        .toChrononRow(gbInfo.irCodec.decode(bytes), gbInfo.irChrononSchema)
        .asInstanceOf[Array[Any]]
    val collapsed = gbInfo.aggregator.windowedAggregator.denormalize(batchRecord(0).asInstanceOf[Array[Any]])
    val tailHops = batchRecord(1)
      .asInstanceOf[util.ArrayList[Any]]
      .iterator()
      .asScala
      .map(
        _.asInstanceOf[util.ArrayList[Any]]
          .iterator()
          .asScala
          .map(hop => gbInfo.aggregator.baseAggregator.denormalizeInPlace(hop.asInstanceOf[Array[Any]]))
          .toArray)
      .toArray
    windowing.FinalBatchIr(collapsed, tailHops)
  }

  private case class PrefixedRequest(prefix: String, request: Request)

  def fetchJoin(requests: scala.collection.Seq[Request]): Future[scala.collection.Seq[Response]] = {
    val startTimeMs = System.currentTimeMillis()
    // convert join requests to groupBy requests
    val joinDecomposed: scala.collection.Seq[(Request, Try[Seq[PrefixedRequest]])] =
      requests.map { request =>
        val joinTry = getJoinConf(request.name)
        var joinContext: Option[Metrics.Context] = None
        val decomposedTry = joinTry.map { join =>
          joinContext = Some(Metrics.Context(Metrics.Environment.JoinFetching, join.join))
          joinContext.get.increment("join_request.count")
          join.joinPartOps.map { part =>
            val joinContextInner = Metrics.Context(joinContext.get, part)
            val rightKeys = part.leftToRight.map { case (leftKey, rightKey) => rightKey -> request.keys(leftKey) }
            PrefixedRequest(
              part.fullPrefix,
              Request(part.groupBy.getMetaData.getName, rightKeys, request.atMillis, Some(joinContextInner)))
          }
        }
        request.copy(context = joinContext) -> decomposedTry
      }

    val groupByRequests = joinDecomposed.flatMap {
      case (_, gbTry) =>
        gbTry match {
          case Failure(_)        => Iterator.empty
          case Success(requests) => requests.iterator.map(_.request)
        }
    }
    val groupByResponsesFuture = fetchGroupBys(groupByRequests)

    // re-attach groupBy responses to join
    groupByResponsesFuture
      .map { groupByResponses =>
        val responseMap = groupByResponses.iterator.map { response => response.request -> response.values }.toMap
        val responses = joinDecomposed.iterator.map {
          case (joinRequest, decomposedRequestsTry) =>
            val joinValuesTry = decomposedRequestsTry.map { groupByRequestsWithPrefix =>
              val result = groupByRequestsWithPrefix.iterator.flatMap {
                case PrefixedRequest(prefix, groupByRequest) =>
                  responseMap
                    .getOrElse(groupByRequest,
                               Failure(new IllegalStateException(
                                 s"Couldn't find a groupBy response for $groupByRequest in response map")))
                    .map { valueMap =>
                      if (valueMap != null) {
                        valueMap.map { case (aggName, aggValue) => prefix + "_" + aggName -> aggValue }
                      } else {
                        Map.empty[String, AnyRef]
                      }
                    }
                    // prefix feature names
                    .recover { // capture exception as a key
                      case ex: Throwable =>
                        val stringWriter = new StringWriter()
                        val printWriter = new PrintWriter(stringWriter)
                        ex.printStackTrace(printWriter)
                        val trace = stringWriter.toString
                        if (debug || Math.random() < 0.001) {
                          println(s"Failed to fetch $groupByRequest with \n$trace")
                        }
                        Map(groupByRequest.name + "_exception" -> trace)
                    }
                    .get
              }.toMap
              result
            }
            joinValuesTry match {
              case Failure(ex) => joinRequest.context.foreach(_.incrementException(ex))
              case Success(responseMap) =>
                joinRequest.context.foreach { ctx =>
                  ctx.histogram("response.keys.count", responseMap.size)
                }
            }
            joinRequest.context.foreach { ctx =>
              ctx.histogram("overall.latency.millis", System.currentTimeMillis() - startTimeMs)
              ctx.increment("overall.request.count")
            }
            Response(joinRequest, joinValuesTry)
        }.toSeq
        responses
      }
  }
}

case class JoinCodec(conf: JoinOps,
                     keySchema: StructType,
                     valueSchema: StructType,
                     keyCodec: AvroCodec,
                     valueCodec: AvroCodec)
    extends Serializable {
  val keys: Array[String] = keySchema.fields.iterator.map(_.name).toArray
  val values: Array[String] = valueSchema.fields.iterator.map(_.name).toArray

  val keyFields: Array[StructField] = keySchema.fields
  val valueFields: Array[StructField] = valueSchema.fields
  val timeFields: Array[StructField] = Array(
    StructField("ts", LongType),
    StructField("ds", StringType)
  )
  val outputFields: Array[StructField] = keyFields ++ valueFields ++ timeFields
}

// BaseFetcher + Logging
class Fetcher(kvStore: KVStore,
              metaDataSet: String = ChrononMetadataKey,
              timeoutMillis: Long = 10000,
              logFunc: Consumer[LoggableResponse] = null,
              debug: Boolean = false)
    extends BaseFetcher(kvStore, metaDataSet, timeoutMillis, debug) {

  // key and value schemas
  lazy val getJoinCodecs = new TTLCache[String, Try[JoinCodec]]({ joinName: String =>
    val joinConfTry = getJoinConf(joinName)
    val keyFields = new mutable.ListBuffer[StructField]
    val valueFields = new mutable.ListBuffer[StructField]
    joinConfTry.map {
      joinConf =>
        joinConf.joinPartOps.foreach {
          joinPart =>
            val servingInfoTry = getGroupByServingInfo(joinPart.groupBy.metaData.getName)
            servingInfoTry
              .map {
                servingInfo =>
                  val keySchema = servingInfo.keyCodec.chrononSchema.asInstanceOf[StructType]
                  joinPart.leftToRight
                    .mapValues(right => keySchema.fields.find(_.name == right).get.fieldType)
                    .foreach {
                      case (name, dType) =>
                        val keyField = StructField(name, dType)
                        if (!keyFields.contains(keyField)) {
                          keyFields.append(keyField)
                        }
                    }

                  val baseValueSchema = if (joinPart.groupBy.aggregations == null) {
                    servingInfo.selectedChrononSchema
                  } else {
                    servingInfo.outputChrononSchema
                  }
                  baseValueSchema.fields.foreach { sf =>
                    valueFields.append(joinPart.constructJoinPartSchema(sf))
                  }
              }
        }

        val keySchema = StructType(s"${joinName}_key", keyFields.toArray)
        val keyCodec = AvroCodec.of(AvroConversions.fromChrononSchema(keySchema).toString)
        val valueSchema = StructType(s"${joinName}_value", valueFields.toArray)
        val valueCodec = AvroCodec.of(AvroConversions.fromChrononSchema(valueSchema).toString)
        JoinCodec(joinConf, keySchema, valueSchema, keyCodec, valueCodec)
    }
  })

  override def fetchJoin(requests: scala.collection.Seq[Request]): Future[scala.collection.Seq[Response]] = {
    val ts = System.currentTimeMillis()
    super
      .fetchJoin(requests)
      .map(_.iterator.map { resp =>
        val joinCodecTry = getJoinCodecs(resp.request.name)
        val loggingTry = joinCodecTry.map {
          enc =>
            val metaData = enc.conf.join.metaData
            val samplePercent = if (metaData.isSetSamplePercent) metaData.getSamplePercent else 0
            val hash = if (samplePercent > 0) Math.abs(MurmurHash3.orderedHash(resp.request.keys.values)) else -1
            if ((hash > 0) && ((hash % (100 * 1000)) <= (samplePercent * 1000))) {
              val joinName = resp.request.name
              if (debug) {
                println(s"Passed ${resp.request.keys} : $hash : ${hash % 100000}: $samplePercent")
                val gson = new Gson()
                println(s"""Sampled join fetch
                     |Key Map: ${resp.request.keys}
                     |Value Map: [${resp.values.map {
                  _.map { case (k, v) => s"$k -> ${gson.toJson(v)}" }.mkString(", ")
                }}]
                     |""".stripMargin)
              }
              val keyArr = enc.keys.map(resp.request.keys.getOrElse(_, null))
              val keys = AvroConversions.fromChrononRow(keyArr, enc.keySchema).asInstanceOf[GenericRecord]
              val keyBytes = enc.keyCodec.encodeBinary(keys)
              val valueBytes = resp.values
                .map { valueMap =>
                  val valueArr = enc.values.map(valueMap.getOrElse(_, null))
                  val valueRecord =
                    AvroConversions.fromChrononRow(valueArr, enc.valueSchema).asInstanceOf[GenericRecord]
                  enc.valueCodec.encodeBinary(valueRecord)
                }
                .getOrElse(null)
              val loggableResponse =
                LoggableResponse(keyBytes, valueBytes, joinName, resp.request.atMillis.getOrElse(ts))
              if (logFunc != null)
                logFunc.accept(loggableResponse)
              val joinContext = Metrics.Context(Metrics.Environment.JoinFetching, enc.conf.join)
              joinContext.increment("logging_request.count")
            }
        }
        if (loggingTry.isFailure && (debug || Math.random() < 0.01)) {
          println(s"logging failed due to ${loggingTry.failed.get.getStackTrace.mkString("Array(", ", ", ")")}")
        }
        resp
      }.toSeq)
  }

  override def fetchGroupBys(requests: scala.collection.Seq[Request]): Future[scala.collection.Seq[Response]] = {
    super.fetchGroupBys(requests)
  }
}
