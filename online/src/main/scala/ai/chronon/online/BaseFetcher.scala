package ai.chronon.online

import ai.chronon.aggregator.row.ColumnAggregator
import ai.chronon.aggregator.windowing
import ai.chronon.aggregator.windowing.{FinalBatchIr, SawtoothOnlineAggregator}
import ai.chronon.api.Constants.ChrononMetadataKey
import ai.chronon.api._
import ai.chronon.online.Fetcher.{Request, Response}
import ai.chronon.online.KVStore.{GetRequest, GetResponse, TimedValue}
import ai.chronon.online.Metrics.Name
import ai.chronon.api.Extensions.ThrowableOps

import java.io.{PrintWriter, StringWriter}
import java.util
import scala.collection.JavaConverters._
import scala.collection.Seq
import scala.concurrent.Future
import scala.util.{Failure, Success, Try}

// Does internal facing fetching
//   1. takes join request or groupBy requests
//   2. does the fan out and fan in from kv store in a parallel fashion
//   3. does the post aggregation
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
        if (debug) println("Both batch and streaming data are null")
        null
      } else {
        val streamingRows: Array[Row] = streamingResponses.iterator
          .filter(tVal => tVal.millis >= servingInfo.batchEndTsMillis)
          .map(tVal => selectedCodec.decodeRow(tVal.bytes, tVal.millis, mutations))
          .toArray
        reportKvResponse(context.withSuffix("streaming"),
                         streamingResponses,
                         queryTimeMs,
                         overallLatency,
                         totalResponseValueBytes)
        val batchIr = toBatchIr(batchBytes, servingInfo)
        val output = aggregator.lambdaAggregateFinalized(batchIr, streamingRows.iterator, queryTimeMs, mutations)
        if (debug) println(s"""
             |batch ir: $batchIr
             |streamingRows: $streamingRows
             |batchEnd in millis: ${servingInfo.batchEndTsMillis}
             |queryTime in millis: $queryTimeMs
             |""".stripMargin)
        servingInfo.outputCodec.fieldNames.iterator.zip(output.iterator.map(_.asInstanceOf[AnyRef])).toMap
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
    val requestTimeMillis = System.currentTimeMillis()
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
    val kvResponseFuture: Future[Seq[GetResponse]] = if (allRequests.nonEmpty) {
      kvStore.multiGet(allRequests)
    } else {
      Future(Seq.empty[GetResponse])
    }

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
                if (debug)
                  println(
                    s"Constructing response for groupBy: ${groupByServingInfo.groupByOps.metaData.getName} " +
                      s"for keys: ${request.keys}")
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

    val joinDecomposed: scala.collection.Seq[(Request, Try[Seq[Either[PrefixedRequest, KeyMissingException]]])] =
      requests.map { request =>
        val joinTry = getJoinConf(request.name)
        var joinContext: Option[Metrics.Context] = None
        val decomposedTry = joinTry.map { join =>
          joinContext = Some(Metrics.Context(Metrics.Environment.JoinFetching, join.join))
          joinContext.get.increment("join_request.count")
          join.joinPartOps.map { part =>
            val joinContextInner = Metrics.Context(joinContext.get, part)
            val missingKeys = part.leftToRight.keys.filterNot(request.keys.contains)
            if (missingKeys.nonEmpty) {
              Right(KeyMissingException(part.fullPrefix, missingKeys.toSeq))
            } else {
              val rightKeys = part.leftToRight.map { case (leftKey, rightKey) => rightKey -> request.keys(leftKey) }
              Left(
                PrefixedRequest(
                  part.fullPrefix,
                  Request(part.groupBy.getMetaData.getName, rightKeys, request.atMillis, Some(joinContextInner))))
            }
          }
        }
        request.copy(context = joinContext) -> decomposedTry
      }

    val groupByRequests = joinDecomposed.flatMap {
      case (_, gbTry) =>
        gbTry match {
          case Failure(_)        => Iterator.empty
          case Success(requests) => requests.iterator.flatMap(_.left.toOption).map(_.request)
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
              groupByRequestsWithPrefix.iterator.flatMap {
                case Right(keyMissingException) => {
                  Map(keyMissingException.requestName + "_exception" -> keyMissingException.getMessage)
                }
                case Left(PrefixedRequest(prefix, groupByRequest)) => {
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
                        if (debug || Math.random() < 0.001) {
                          println(s"Failed to fetch $groupByRequest with \n${ex.traceString}")
                        }
                        Map(groupByRequest.name + "_exception" -> ex.traceString)
                    }
                    .get
                }
              }.toMap
            }
            joinValuesTry match {
              case Failure(ex) => joinRequest.context.foreach(_.incrementException(ex))
              case Success(responseMap) =>
                joinRequest.context.foreach { ctx =>
                  ctx.histogram("response.keys.count", responseMap.size)
                }
            }
            joinRequest.context.foreach { ctx =>
              ctx.histogram("internal.latency.millis", System.currentTimeMillis() - startTimeMs)
              ctx.increment("internal.request.count")
            }
            Response(joinRequest, joinValuesTry)
        }.toSeq
        responses
      }
  }
}
