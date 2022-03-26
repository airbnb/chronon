package ai.zipline.online

import ai.zipline.aggregator.row.ColumnAggregator
import ai.zipline.aggregator.windowing.{FinalBatchIr, SawtoothOnlineAggregator}
import ai.zipline.api.Constants.ZiplineMetadataKey
import ai.zipline.api.{Row, _}
import ai.zipline.online.CompatParColls.Converters._
import ai.zipline.online.Fetcher._
import ai.zipline.online.KVStore.{GetRequest, GetResponse, TimedValue}

import java.io.{ByteArrayOutputStream, PrintStream}
import scala.collection.parallel.ExecutionContextTaskSupport
import scala.concurrent.Future
import scala.util.{Failure, Success, Try}

object Fetcher {
  case class Request(name: String, keys: Map[String, AnyRef], atMillis: Option[Long] = None)
  case class Response(request: Request, values: Try[Map[String, AnyRef]])
}

class Fetcher(kvStore: KVStore, metaDataSet: String = ZiplineMetadataKey, timeoutMillis: Long = 10000)
    extends MetadataStore(kvStore, metaDataSet, timeoutMillis) {

  private case class GroupByRequestMeta(
      groupByServingInfoParsed: GroupByServingInfoParsed,
      batchRequest: GetRequest,
      streamingRequestOpt: Option[GetRequest],
      endTs: Option[Long]
  )

  // a groupBy request is split into batchRequest and optionally a streamingRequest
  private def constructGroupByResponse(batchResponse: Try[TimedValue],
                                       streamingResponsesOpt: Option[Seq[TimedValue]],
                                       servingInfo: GroupByServingInfoParsed,
                                       queryTimeMs: Long,
                                       startTimeMs: Long,
                                       context: Metrics.Context): Map[String, AnyRef] = {
    val groupByContext = FetcherMetrics.getGroupByContext(servingInfo, Some(context))
    val batchContext = groupByContext.asBatch
    // batch request has only one value per key.
    batchResponse.foreach { value =>
      FetcherMetrics.reportDataFreshness(value.millis, batchContext)
      FetcherMetrics.reportResponseBytesSize(value.bytes.length, batchContext)
    }
    FetcherMetrics.reportResponseNumRows(batchResponse.map(_ => 1).getOrElse(0), batchContext)

    // bulk upload didn't remove an older batch value - so we manually discard
    val batchBytes: Array[Byte] = batchResponse
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
      val streamingRows: Iterator[Row] = streamingResponses.iterator
        .filter(tVal => tVal.millis >= servingInfo.batchEndTsMillis)
        .map(tVal => selectedCodec.decodeRow(tVal.bytes, tVal.millis, mutations))
      if (streamingResponses.nonEmpty) {
        val streamingContext = groupByContext.asStreaming
        // report streaming metrics.
        FetcherMetrics.reportDataFreshness(streamingResponses.maxBy(_.millis).millis - startTimeMs, streamingContext)
        FetcherMetrics.reportResponseBytesSize(streamingResponses.iterator.map(_.bytes.length).sum, streamingContext)
        FetcherMetrics.reportResponseNumRows(streamingResponses.length, streamingContext)
      }
      val batchIr = toBatchIr(batchBytes, servingInfo)
      val output = aggregator.lambdaAggregateFinalized(batchIr, streamingRows, queryTimeMs, mutations)
      servingInfo.outputCodec.fieldNames.zip(output.map(_.asInstanceOf[AnyRef])).toMap
    }
    FetcherMetrics.reportLatency(System.currentTimeMillis() - startTimeMs, groupByContext)
    responseMap
  }

  private def updateServingInfo(batchEndTs: Option[Long],
                                groupByServingInfo: GroupByServingInfoParsed): GroupByServingInfoParsed = {
    val name = groupByServingInfo.groupBy.metaData.name
    if (batchEndTs.exists(_ > groupByServingInfo.batchEndTsMillis)) {
      println(s"""${name}'s value's batch timestamp of ${batchEndTs.get} is
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
  def fetchGroupBys(requests: scala.collection.Seq[Request], contextOption: Option[Metrics.Context] = None): Future[scala.collection.Seq[Response]] = {
    val context = contextOption.getOrElse(Metrics.Context(method = "fetchGroupBys"))
    val startTimeMs = System.currentTimeMillis()
    // split a groupBy level request into its kvStore level requests
    val groupByRequestToKvRequest: Seq[(Request, Try[GroupByRequestMeta])] = requests.iterator.map { request =>
      val groupByName = request.name
      val groupByRequestMetaTry: Try[GroupByRequestMeta] = getGroupByServingInfo(groupByName)
        .map { groupByServingInfo =>
          var keyBytes: Array[Byte] = null
          try {
            keyBytes = groupByServingInfo.keyCodec.encode(request.keys)
          } catch {
            case ex: Exception =>
              val castedKeys = groupByServingInfo.keyZiplineSchema.fields.map {
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
          GroupByRequestMeta(groupByServingInfo, batchRequest, streamingRequestOpt, request.atMillis)
        }
      request -> groupByRequestMetaTry
    }.toSeq
    val allRequests: Seq[GetRequest] = groupByRequestToKvRequest.flatMap {
      case (_, Success(GroupByRequestMeta(_, batchRequest, streamingRequestOpt, _))) =>
        Some(batchRequest) ++ streamingRequestOpt
      case _ => Seq.empty
    }

    val kvResponseFuture: Future[Seq[GetResponse]] = kvStore.multiGet(allRequests)
    // map all the kv store responses back to groupBy level responses
    kvResponseFuture
      .map { responsesFuture: Seq[GetResponse] =>
        val responsesMap: Map[GetRequest, Try[Seq[TimedValue]]] = responsesFuture.iterator.map { response =>
          response.request -> response.values
        }.toMap
        // Heaviest compute is decoding bytes and merging them - so we parallelize
        val requestParFanout = groupByRequestToKvRequest.par
        requestParFanout.tasksupport = new ExecutionContextTaskSupport(executionContext)
        val responses: Seq[Response] = requestParFanout.map {
          case (request, requestMetaTry) =>
            val responseMapTry = requestMetaTry.map { requestMeta =>
              val GroupByRequestMeta(groupByServingInfo, batchRequest, streamingRequestOpt, _) = requestMeta
              // pick the batch version with highest timestamp
              val batchResponseTry = responsesMap
                .getOrElse(batchRequest,
                           Failure(new IllegalStateException(
                             s"Couldn't find corresponding response for $batchRequest in responseMap")))
                .map(_.maxBy(_.millis))
              val batchEndTs = batchResponseTry.map { timedVal => Some(timedVal.millis) }.getOrElse(None)
              val streamingResponsesOpt = streamingRequestOpt.map(responsesMap.getOrElse(_, Success(Seq.empty)).get)
              val queryTs = request.atMillis.getOrElse(System.currentTimeMillis())
              constructGroupByResponse(batchResponseTry,
                                       streamingResponsesOpt,
                                       updateServingInfo(batchEndTs, groupByServingInfo),
                                       queryTs,
                                       startTimeMs,
                                       context)
            }
            responseMapTry.failed.map(ex => reportFailure(requests, context.withGroupBy, ex))
            Response(request, responseMapTry)
        }.toList
        responses
      }
  }

  def toBatchIr(bytes: Array[Byte], gbInfo: GroupByServingInfoParsed): FinalBatchIr = {
    if (bytes == null) return null
    val batchRecord =
      RowConversions
        .fromAvroRecord(gbInfo.irCodec.decode(bytes), gbInfo.irZiplineSchema)
        .asInstanceOf[Array[Any]]
    val collapsed = gbInfo.aggregator.windowedAggregator.denormalize(batchRecord(0).asInstanceOf[Array[Any]])
    val tailHops = batchRecord(1)
      .asInstanceOf[Array[Any]]
      .map(_.asInstanceOf[Array[Any]]
        .map(hop => gbInfo.aggregator.baseAggregator.denormalizeInPlace(hop.asInstanceOf[Array[Any]])))
    FinalBatchIr(collapsed, tailHops)
  }

  def tuplesToMap[K, V](tuples: Seq[(K, V)]): Map[K, Seq[V]] = tuples.groupBy(_._1).map{case (k,v) => (k, v.map(_._2))}.toMap

  private case class PrefixedRequest(prefix: String, request: Request)

  def fetchJoin(requests: scala.collection.Seq[Request]): Future[scala.collection.Seq[Response]] = {
    val context = Metrics.Context(method = "fetchJoin")
    val startTimeMs = System.currentTimeMillis()
    // convert join requests to groupBy requests
    val joinDecomposed: Seq[(Request, Try[(Seq[PrefixedRequest], Metrics.Context)])] =
      requests.iterator.map { request =>
        val joinTry = getJoinConf(request.name)
        val decomposedTry = joinTry.map { join =>
          val prefixedRequests = join.joinPartOps.map { part =>
            val groupByName = part.groupBy.getMetaData.getName
            val rightKeys = part.leftToRight.map { case (leftKey, rightKey) => rightKey -> request.keys(leftKey) }
            PrefixedRequest(part.fullPrefix, Request(groupByName, rightKeys, request.atMillis))
          }
          val joinContext = context.withJoin(request.name).withProduction(join.isProduction).withTeam(join.team)
          (prefixedRequests, joinContext)
        }
        request -> decomposedTry
      }.toSeq

    // dedup duplicate requests
    val uniqueRequests = joinDecomposed.iterator.flatMap(_._2.map(_._1).getOrElse(Seq.empty)).map(_.request).toSet
    val groupByResponsesFuture = fetchGroupBys(uniqueRequests.toSeq)

    // re-attach groupBy responses to join
    groupByResponsesFuture
      .map { groupByResponses =>
        val responseMap = groupByResponses.iterator.map { response => response.request -> response.values }.toMap
        val responses = joinDecomposed.iterator.map {
          case (joinRequest, decomposedRequestsTry) =>
            val joinValuesTry = decomposedRequestsTry.map {
              case (groupByRequestsWithPrefix, joinContext) =>
                val result = groupByRequestsWithPrefix.iterator.flatMap {
                  case PrefixedRequest(prefix, groupByRequest) =>
                    responseMap
                      .getOrElse(groupByRequest,
                                 Failure(new IllegalStateException(
                                   s"Couldn't find a groupBy response for $groupByRequest in response map")))
                      .map {
                        _.map { case (aggName, aggValue) => prefix + "_" + aggName -> aggValue }
                      } // prefix feature names
                      .recover { // capture exception as a key
                        case ex: Throwable =>
                          val baos = new ByteArrayOutputStream()
                          ex.printStackTrace(new PrintStream(baos, true))
                          Map(groupByRequest.name + "_exception" -> baos.toString)
                      }
                      .get
                }.toMap
                FetcherMetrics.reportLatency(System.currentTimeMillis() - startTimeMs, joinContext)
                result
            }
            Response(joinRequest, joinValuesTry)
        }.toSeq
        // report latency of each join as the maximum of the latency of the joins in the request batch.
        responses.foreach { resp =>
          FetcherMetrics.reportFinalLatency(System.currentTimeMillis() - startTimeMs,
                                            context.withJoin(resp.request.name))
        }
        responses
      }
      .recover {
        case e: Exception =>
          reportFailure(requests, context.withJoin, e)
          throw e
      }
  }

  private def reportFailure(requests: scala.collection.Seq[Request], withTag: String => Metrics.Context, e: Throwable) = {
    requests.foreach { req =>
      val context = withTag(req.name)
      FetcherMetrics.reportFailure(e, context)
    }
  }
}
