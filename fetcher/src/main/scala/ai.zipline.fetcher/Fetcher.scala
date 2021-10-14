package ai.zipline.fetcher

import ai.zipline.aggregator.row.Row
import ai.zipline.aggregator.windowing.{FinalBatchIr, SawtoothOnlineAggregator}
import ai.zipline.api.Constants.ZiplineMetadataKey
import ai.zipline.api.Extensions._
import ai.zipline.api.KVStore.{GetRequest, GetResponse, TimedValue}
import ai.zipline.api.{StructType, _}
import ai.zipline.lib.{FetcherMetrics, Metrics}
import org.apache.avro.Schema
import ai.zipline.fetcher.Fetcher._
import java.util.concurrent.ConcurrentHashMap
import java.util.function
import java.util.function.BiFunction

import scala.collection.JavaConverters._
import scala.collection.parallel.ExecutionContextTaskSupport
import scala.concurrent.Future

// mixin class - with schema
class GroupByServingInfoParsed(groupByServingInfo: GroupByServingInfo)
    extends GroupByServingInfo(groupByServingInfo)
    with Serializable {

  // streaming starts scanning after batchEnd
  val batchEndTsMillis: Long = Constants.Partition.epochMillis(batchEndDate)
  @transient lazy val parser = new Schema.Parser()

  lazy val aggregator: SawtoothOnlineAggregator = {
    val avroInputSchema = parser.parse(selectedAvroSchema)
    val ziplineInputSchema =
      AvroUtils.toZiplineSchema(avroInputSchema).asInstanceOf[StructType].unpack
    new SawtoothOnlineAggregator(batchEndTsMillis, groupByServingInfo.groupBy.aggregations.asScala, ziplineInputSchema)
  }

  // caching groupBy helper to avoid re-computing batchDataSet,streamingDataset & inferred accuracy
  lazy val groupByOps = new GroupByOps(groupByServingInfo.groupBy)

  lazy val irZiplineSchema: StructType =
    StructType.from(s"${groupBy.metaData.cleanName}_IR", aggregator.batchIrSchema)

  def keyCodec: AvroCodec = AvroCodec.of(keyAvroSchema)
  def selectedCodec: AvroCodec = AvroCodec.of(selectedAvroSchema)
  lazy val irAvroSchema: String = AvroUtils.fromZiplineSchema(irZiplineSchema).toString()
  def irCodec: AvroCodec = AvroCodec.of(irAvroSchema)
  def outputCodec: AvroCodec = AvroCodec.of(outputAvroSchema)

  lazy val outputAvroSchema: String = {
    val outputZiplineSchema =
      StructType.from(s"${groupBy.metaData.cleanName}_OUTPUT", aggregator.windowedAggregator.outputSchema)
    AvroUtils.fromZiplineSchema(outputZiplineSchema).toString()
  }
  def inputZiplineSchema: StructType = {
    AvroUtils.toZiplineSchema(parser.parse(inputAvroSchema)).asInstanceOf[StructType]
  }
  def selectedZiplineSchema: StructType = {
    AvroUtils.toZiplineSchema(parser.parse(selectedAvroSchema)).asInstanceOf[StructType]
  }
}

// can continuously grow
class TTLCache[I, O](f: I => O,
                     ttlMillis: Long = 2 * 60 * 60 * 1000, // 2 hours
                     nowFunc: () => Long = { () => System.currentTimeMillis() }) {
  val func: BiFunction[I, (Long, O), (Long, O)] = new function.BiFunction[I, (Long, O), (Long, O)] {
    override def apply(t: I, u: (Long, O)): (Long, O) = {
      val now = nowFunc()
      if (u == null || now - u._1 > ttlMillis) {
        now -> f(t)
      } else {
        u
      }
    }
  }

  val cMap = new ConcurrentHashMap[I, (Long, O)]()
  def apply(i: I): O = cMap.compute(i, func)._2
  def force(i: I): O = cMap.put(i, nowFunc() -> f(i))._2
}

object Fetcher {
  case class Request(name: String, keys: Map[String, AnyRef], atMillis: Option[Long] = None)
  case class Response(request: Request, values: Map[String, AnyRef])
}

class Fetcher(kvStore: KVStore, metaDataSet: String = ZiplineMetadataKey, timeoutMillis: Long = 10000)
    extends MetadataStore(kvStore, metaDataSet, timeoutMillis) {

  private def getGroupByContext(groupByServingInfo: GroupByServingInfo, contextOption: Option[Metrics.Context] = None): Metrics.Context = {
    val context = contextOption.getOrElse(Metrics.Context())
    val groupBy = groupByServingInfo.getGroupBy
    context
      .withGroupBy(groupBy.getMetaData.getName)
      .withProduction(groupBy.getMetaData.isProduction)
      .withTeam(groupBy.getMetaData.getTeam)
  }

  private case class GroupByRequestMeta(groupByServingInfoParsed: GroupByServingInfoParsed,
                                        batchRequest: GetRequest,
                                        streamingRequestOpt: Option[GetRequest],
                                        endTs: Option[Long])

  private def reportRequestBatchSize(requests: Seq[Request], withTag: String => Metrics.Context, context: Metrics.Context): Unit = {
    val batchContext =
      if (requests.forall(_.name == requests.head.name)) withTag(requests.head.name)
      else context
    FetcherMetrics.reportRequestBatchSize(requests.size, batchContext)
  }

  // 1. fetches GroupByServingInfo
  // 2. encodes keys as keyAvroSchema)
  // 3. Based on accuracy, fetches streaming + batch data and aggregates further.
  // 4. Finally converted to outputSchema
  def fetchGroupBys(requests: Seq[Request], contextOption: Option[Metrics.Context] = None): Future[Seq[Response]] = {
    val context = contextOption.getOrElse(Metrics.Context(method = "fetchGroupBys"))
    reportRequestBatchSize(requests, context.withGroupBy, context)
    val startTimeMs = System.currentTimeMillis()
    // split a groupBy level request into its kvStore level requests
    val groupByRequestToKvRequest = requests.iterator.map { request =>
      val groupByName = request.name
      val groupByServingInfo = getGroupByServingInfo(groupByName)
      val keyBytes = groupByServingInfo.keyCodec.encode(request.keys)
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
      request -> GroupByRequestMeta(groupByServingInfo, batchRequest, streamingRequestOpt, request.atMillis)
    }.toSeq
    val allRequests: Seq[GetRequest] = groupByRequestToKvRequest.flatMap {
      case (_, GroupByRequestMeta(_, batchRequest, streamingRequestOpt, _)) =>
        Some(batchRequest) ++ streamingRequestOpt
    }

    val kvResponseFuture: Future[Seq[GetResponse]] = kvStore.multiGet(allRequests)

    // map all the kv store responses back to groupBy level responses
    kvResponseFuture.map { responsesFuture: Seq[GetResponse] =>
      val responsesMap = responsesFuture.iterator.map { response =>
        response.request -> response.values
      }.toMap
      // Heaviest compute is decoding bytes and merging them - so we parallelize
      val requestParFanout = groupByRequestToKvRequest.par
      requestParFanout.tasksupport = new ExecutionContextTaskSupport(executionContext)
      val responses: Seq[Response] = requestParFanout.map {
        case (request, GroupByRequestMeta(groupByServingInfo, batchRequest, streamingRequestOpt, atMillis)) =>
         // pick the batch version with highest timestamp
          val batchOption = responsesMap.get(batchRequest).flatMap(Option(_)).map(_.maxBy(_.millis))
          val batchTime: Option[Long] = batchOption.map(_.millis)

          val servingInfo = if (batchTime.exists(_ > groupByServingInfo.batchEndTsMillis)) {
            println(s"""${request.name}'s value's batch timestamp of $batchTime is
                 |ahead of schema timestamp of ${groupByServingInfo.batchEndTsMillis}.
                 |Forcing an update of schema.""".stripMargin)
            getGroupByServingInfo.force(request.name)
          } else {
            groupByServingInfo
          }
          val groupByContext = getGroupByContext(groupByServingInfo, Some(context))
          // batch request has only one value per key.
          val batchValueOption: Option[TimedValue] = Option(responsesMap(batchRequest)).flatMap(_.headOption)
          batchValueOption.foreach { value =>
            FetcherMetrics.reportDataFreshness(value.millis, groupByContext.asBatch)
            FetcherMetrics.reportResponseBytesSize(value.bytes.length, groupByContext.asBatch)
          }
          FetcherMetrics.reportResponseNumRows(if (batchValueOption.isDefined) 1 else 0, groupByContext.asBatch)
          val batchBytes: Array[Byte] = batchOption
          // bulk upload didn't remove an older batch value - so we manually discard
            .filter(_.millis >= servingInfo.batchEndTsMillis)
            .map(_.bytes)
            .orNull

          val responseMap: Map[String, AnyRef] = if (servingInfo.groupBy.aggregations == null) { // no-agg
            servingInfo.selectedCodec.decodeMap(batchBytes)
          } else if (streamingRequestOpt.isEmpty) { // snapshot accurate
            servingInfo.outputCodec.decodeMap(batchBytes)
          } else { // temporal accurate
            val aggregator: SawtoothOnlineAggregator = servingInfo.aggregator
            val streamingResponses: Seq[TimedValue] =
              responsesMap.get(streamingRequestOpt.get).flatMap(Option(_)).getOrElse(Seq.empty)
            val selectedCodec = servingInfo.selectedCodec
            val streamingRows: Iterator[Row] =
              streamingResponses.iterator
                .filter(tVal => tVal.millis >= servingInfo.batchEndTsMillis)
                .map(tVal => selectedCodec.decodeRow(tVal.bytes, tVal.millis))
            if (streamingResponses.length > 0) {
              val streamingContext = groupByContext.asStreaming
              // report streaming metrics.
              FetcherMetrics.reportDataFreshness(streamingResponses.map(_.millis).max - startTimeMs, streamingContext)
              FetcherMetrics.reportResponseBytesSize(streamingResponses.map(_.bytes.length).sum, streamingContext)
              FetcherMetrics.reportResponseNumRows(streamingRows.length, streamingContext)
            }
            val batchIr = toBatchIr(batchBytes, servingInfo)
            val queryTs = atMillis.getOrElse(System.currentTimeMillis())
            val output = aggregator.lambdaAggregateFinalized(batchIr, streamingRows, queryTs)
            servingInfo.outputCodec.fieldNames.zip(output.map(_.asInstanceOf[AnyRef])).toMap
          }
          FetcherMetrics.reportLatency(System.currentTimeMillis() - startTimeMs, groupByContext)
          Response(request, responseMap)
      }.toList
      // report latency of each group by as the maximum of the latency of the group bys in the request batch.
      responses.foreach { resp =>
        FetcherMetrics.reportFinalLatency(System.currentTimeMillis() - startTimeMs, context.withGroupBy(resp.request.name))
      }
      responses
    }.recover {
      case e: Exception =>
        reportFailure(requests, context.withGroupBy, e)
        throw e
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

  def tuplesToMap[K, V](tuples: Seq[(K, V)]): Map[K, Seq[V]] = tuples.groupBy(_._1).mapValues(_.map(_._2))

  private case class PrefixedRequest(prefix: String, request: Request)

  def fetchJoin(requests: Seq[Request]): Future[Seq[Response]] = {
    val context = Metrics.Context(method = "fetchJoin")
    reportRequestBatchSize(requests, context.withJoin, context)
    val startTimeMs = System.currentTimeMillis()
    // convert join requests to groupBy requests
    val joinDecomposed: Seq[(Request, (Seq[PrefixedRequest], Metrics.Context))] =
      requests.iterator.map { request =>
        val join = getJoinConf(request.name)
        val prefixedRequests = join.joinPartOps.map { part =>
          val groupByName = part.groupBy.getMetaData.getName
          val rightKeys = part.leftToRight.map { case (leftKey, rightKey) => rightKey -> request.keys(leftKey) }
          PrefixedRequest(part.fullPrefix, Request(groupByName, rightKeys, request.atMillis))
        }
        val joinContext = context.withJoin(request.name).withProduction(join.isProduction).withTeam(join.team)
        request -> (prefixedRequests, joinContext)
      }.toSeq

    // dedup duplicate requests
    val uniqueRequests = joinDecomposed.iterator.flatMap(_._2._1).map(_.request).toSet
    val groupByResponsesFuture = fetchGroupBys(uniqueRequests.toSeq)

    // re-attach groupBy responses to join
    groupByResponsesFuture.map { groupByResponses =>
      val responseMap = groupByResponses.iterator.map { response => response.request -> response.values }.toMap
      val responses = joinDecomposed.iterator.map {
        case (joinRequest, (groupByRequestsWithPrefix, joinContext)) =>
          val joinValues = groupByRequestsWithPrefix.iterator.flatMap {
            case PrefixedRequest(prefix, groupByRequest) =>
              responseMap
                .get(groupByRequest)
                .flatMap(Option(_))
                .map {
                  _.map { case (aggName, aggValue) => prefix + "_" + aggName -> aggValue }
                }
                .getOrElse(Seq.empty)
          }.toMap
          FetcherMetrics.reportLatency(System.currentTimeMillis() - startTimeMs, joinContext)
          Response(joinRequest, joinValues)
      }.toSeq
      // report latency of each join as the maximum of the latency of the joins in the request batch.
      responses.foreach { resp =>
        FetcherMetrics.reportFinalLatency(System.currentTimeMillis() - startTimeMs, context.withJoin(resp.request.name))
      }
      responses
    }.recover {
      case e: Exception =>
        reportFailure(requests, context.withJoin, e)
      throw e
    }
  }

  private def reportFailure(requests: Seq[Request], withTag: String => Metrics.Context, e: Exception) = {
    requests.foreach { req =>
      val context = withTag(req.name)
      FetcherMetrics.reportFailure(e, context)
    }
  }
}
