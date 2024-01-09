package ai.chronon.online

import ai.chronon.aggregator.windowing
import ai.chronon.aggregator.row.ColumnAggregator
import ai.chronon.aggregator.windowing.{FinalBatchIr, SawtoothOnlineAggregator, TiledIr}
import ai.chronon.api.Constants.ChrononMetadataKey
import ai.chronon.api._
import ai.chronon.online.Fetcher.{Request, Response}
import ai.chronon.online.KVStore.{GetRequest, GetResponse, TimedValue}
import ai.chronon.online.Metrics.Name
import ai.chronon.api.Extensions.ThrowableOps
import com.github.benmanes.caffeine.cache.{Cache => CaffeineCache}
import com.google.gson.Gson

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
  import BaseFetcher._

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
                                       totalResponseValueBytes: Int,
                                       keys: Map[String, Any] // The keys is used only for caching
                                      ): Map[String, AnyRef] = {
    // Check if the batch value returned by the kv store is ahead of the one we have in the MetadataStore.
    // If so, update the MetadataStore so that future requests start fetching streaming data > this new batch data.
    val latestBatchValue: Try[TimedValue] = batchResponsesTry.map(_.maxBy(_.millis))
    val servingInfo =
      latestBatchValue.map(timedVal => updateServingInfo(timedVal.millis, oldServingInfo)).getOrElse(oldServingInfo)

    batchResponsesTry.map {
      reportKvResponse(context.withSuffix("batch"), _, queryTimeMs, overallLatency, totalResponseValueBytes)
    }

    // The bulk upload may not have removed an older batch values. We manually discard all but the latest one.
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
      if (batchBytes == null && (streamingResponses == null || streamingResponses.isEmpty)) {
        if (debug) println("Both batch and streaming data are null")
        null
      } else {
        reportKvResponse(context.withSuffix("streaming"),
          streamingResponses,
          queryTimeMs,
          overallLatency,
          totalResponseValueBytes)

        // If caching is enabled, we try to fetch the batch IR from the cache so we avoid the work of decoding it.
        val batchIr: FinalBatchIr = maybeBatchIrCache match {
          case Some(batchIrCache) =>
            val batchRequestCacheKey = BatchIrCacheKey(
              servingInfo.groupByOps.batchDataset,
              keys,
              servingInfo.batchEndTsMillis)

            batchIrCache.get(batchRequestCacheKey, (key: BatchIrCacheKey) => {
              toBatchIr(batchBytes, servingInfo)
            })
          case None => toBatchIr(batchBytes, servingInfo)
        }

        val output: Array[Any] = if (servingInfo.isTilingEnabled) {
          val streamingIrs: Iterator[TiledIr] = streamingResponses.iterator
            .filter(tVal => tVal.millis >= servingInfo.batchEndTsMillis)
            .map { tVal =>
              try {
                val (tile, _) = servingInfo.tiledCodec.decodeTileIr(tVal.bytes)
                TiledIr(tVal.millis, tile)
              } catch {
                case e: Throwable =>
                  // capture additional info we need and rethrow so that we can log upstream
                  val base64Bytes = java.util.Base64.getEncoder.encodeToString(tVal.bytes)
                  val avroSchema = servingInfo.tiledCodec.tileAvroSchema
                  throw new RuntimeException(
                    s"Failed to decode tile for ${servingInfo.groupBy.metaData.getName} at ${tVal.millis}. " +
                      s"Avro schema: $avroSchema. Base64 bytes: $base64Bytes",
                    e)
              }
            }

          if (debug) {
            val gson = new Gson()
            println(
              s"""
                 |batch ir: ${gson.toJson(batchIr)}
                 |streamingIrs: ${gson.toJson(streamingIrs)}
                 |batchEnd in millis: ${servingInfo.batchEndTsMillis}
                 |queryTime in millis: $queryTimeMs
                 |""".stripMargin)
          }

          aggregator.lambdaAggregateFinalizedTiled(batchIr, streamingIrs, queryTimeMs)
        } else {
          val selectedCodec = servingInfo.groupByOps.dataModel match {
            case DataModel.Events => servingInfo.valueAvroCodec
            case DataModel.Entities => servingInfo.mutationValueAvroCodec
          }

          val streamingRows: Array[Row] = streamingResponses.iterator
            .filter(tVal => tVal.millis >= servingInfo.batchEndTsMillis)
            .map(tVal => selectedCodec.decodeRow(tVal.bytes, tVal.millis, mutations))
            .toArray

          if (debug) {
            val gson = new Gson()
            println(
              s"""
                 |batch ir: ${gson.toJson(batchIr)}
                 |streamingRows: ${gson.toJson(streamingRows)}
                 |batchEnd in millis: ${servingInfo.batchEndTsMillis}
                 |queryTime in millis: $queryTimeMs
                 |""".stripMargin)
          }

          aggregator.lambdaAggregateFinalized(batchIr, streamingRows.iterator, queryTimeMs, mutations)
        }
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

  /**
   * If `batchEndTs` is ahead of `groupByServingInfo.batchEndTsMillis`, update the MetadataStore with the new timestamp.
   *
   * In practice, this means new batch data has landed, and future kvstore requests should fetch streaming data after
   * this new batchEndTsMillis.
   *
   * @param batchEndTs the new batchEndTs from the latest batch data
   * @param groupByServingInfo the current GroupByServingInfo
   */
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

          // Collect cache metrics once per `fetchGroupBys` call.
          maybeBatchIrCache.foreach((cache) => Cache.collectCaffeineCacheMetrics(context, cache, batchIrCacheName))

          var batchKeyBytes: Array[Byte] = null
          var streamingKeyBytes: Array[Byte] = null
          try {
            // The formats of key bytes for batch requests and key bytes for streaming requests may differ based
            // on the KVStore implementation, so we encode each distinctly.
            batchKeyBytes = kvStore.createKeyBytes(request.keys, groupByServingInfo, groupByServingInfo.groupByOps.batchDataset)
            streamingKeyBytes = kvStore.createKeyBytes(request.keys, groupByServingInfo, groupByServingInfo.groupByOps.streamingDataset)
          } catch {
            // TODO: only gets hit in cli path - make this code path just use avro schema to decode keys directly in cli
            // TODO: Remove this code block
            case ex: Exception =>
              val castedKeys = groupByServingInfo.keyChrononSchema.fields.map {
                case StructField(name, typ) => name -> ColumnAggregator.castTo(request.keys.getOrElse(name, null), typ)
              }.toMap
              try {
                batchKeyBytes = kvStore.createKeyBytes(request.keys, groupByServingInfo, groupByServingInfo.groupByOps.batchDataset)
                streamingKeyBytes = kvStore.createKeyBytes(request.keys, groupByServingInfo, groupByServingInfo.groupByOps.streamingDataset)
              } catch {
                case exInner: Exception =>
                  exInner.addSuppressed(ex)
                  throw new RuntimeException("Couldn't encode request keys or casted keys", exInner)
              }
          }
          val batchRequest = GetRequest(batchKeyBytes, groupByServingInfo.groupByOps.batchDataset)
          val streamingRequestOpt = groupByServingInfo.groupByOps.inferredAccuracy match {
            // fetch batch(ir) and streaming(input) and aggregate
            case Accuracy.TEMPORAL =>
              Some(
                GetRequest(streamingKeyBytes,
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
      .flatMap { kvResponses: Seq[GetResponse] =>
        val multiGetMillis = System.currentTimeMillis() - startTimeMs
        val responsesMap: Map[GetRequest, Try[Seq[TimedValue]]] = kvResponses.map { response =>
          response.request -> response.values
        }.toMap
        val totalResponseValueBytes =
          responsesMap.iterator.map(_._2).filter(_.isSuccess).flatMap(_.get.map(v => Option(v.bytes).map(_.length).getOrElse(0))).sum

        val futureResponses: Seq[Future[Response]] = groupByRequestToKvRequest.iterator.map {
          case (request, requestMetaTry) =>
            Future {
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
                    totalResponseValueBytes,
                    request.keys)
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
            }
        }.toList
        Future.sequence(futureResponses)
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
              Right(KeyMissingException(part.fullPrefix, missingKeys.toSeq, request.keys))
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

object BaseFetcher {
  case class BatchIrCacheKey(dataset: String, keys: Map[String, Any], batchEndTsMillis: Long)

  private val batchirCacheSize: Int = Config.getEnvConfig("ai.chronon.fetcher.batch_ir_cache_size", 0)
  private val batchIrCacheName = "batch_ir_cache"

  // Caching is an optional optimization to reduce the amount of CPU time spent decoding batch data.
  lazy val maybeBatchIrCache: Option[CaffeineCache[BatchIrCacheKey, FinalBatchIr]] =
    if (batchirCacheSize > 0)
      Some(Cache[BatchIrCacheKey, FinalBatchIr](cacheName = batchIrCacheName, maximumSize = batchirCacheSize))
    else None
}