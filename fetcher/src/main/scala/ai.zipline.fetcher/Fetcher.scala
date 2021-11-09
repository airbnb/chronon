package ai.zipline.fetcher

import ai.zipline.aggregator.windowing.{FinalBatchIr, SawtoothOnlineAggregator}
import ai.zipline.api.Constants.ZiplineMetadataKey
import ai.zipline.api.KVStore.{GetRequest, GetResponse, TimedValue}
import ai.zipline.api.{Row, _}
import ai.zipline.fetcher.Fetcher._
import com.google.gson.GsonBuilder
import org.rogach.scallop.{ScallopConf, ScallopOption}

import scala.collection.JavaConverters._
import scala.collection.parallel.ExecutionContextTaskSupport

import scala.concurrent.duration.DurationInt
import scala.concurrent.{Await, Future}

class Fetcher(kvStore: KVStore, metaDataSet: String = ZiplineMetadataKey, timeoutMillis: Long = 10000)
    extends MetadataStore(kvStore, metaDataSet, timeoutMillis) {

  private def getGroupByContext(groupByServingInfo: GroupByServingInfo,
                                contextOption: Option[Metrics.Context] = None): Metrics.Context = {
    val context = contextOption.getOrElse(Metrics.Context())
    val groupBy = groupByServingInfo.getGroupBy
    context
      .withGroupBy(groupBy.getMetaData.getName)
      .withProduction(groupBy.getMetaData.isProduction)
      .withTeam(groupBy.getMetaData.getTeam)
  }

  private case class GroupByRequestMeta(
      groupByServingInfoParsed: GroupByServingInfoParsed,
      batchRequest: GetRequest,
      streamingRequestOpt: Option[GetRequest],
      endTs: Option[Long]
  )

  private def getRequestContext(requests: Seq[Request], withTag: String => Metrics.Context, context: Metrics.Context) = {
    if (requests.forall(_.name == requests.head.name)) withTag(requests.head.name)
    else context
  }

  private def reportRequestBatchSize(requests: Seq[Request], withTag: String => Metrics.Context, context: Metrics.Context): Unit = {
    FetcherMetrics.reportRequestBatchSize(requests.size, getRequestContext(requests, withTag, context))
  }


  // 1. fetches GroupByServingInfo
  // 2. encodes keys as keyAvroSchema)
  // 3. Based on accuracy, fetches streaming + batch data and aggregates further.
  // 4. Finally converted to outputSchema
  def fetchGroupBys(requests: Seq[Request], contextOption: Option[Metrics.Context] = None): Future[Seq[Response]] = {
    val context = contextOption.getOrElse(Metrics.Context(method = "fetchGroupBys"))
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

    val kvRequestStartTimeMs = System.currentTimeMillis()
    val kvResponseFuture: Future[Seq[GetResponse]] = kvStore.multiGet(allRequests)
    // map all the kv store responses back to groupBy level responses
    kvResponseFuture
      .map { responsesFuture: Seq[GetResponse] =>
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
              println(s"""${request.name}'s value's batch timestamp of ${batchTime.get} is
                 |ahead of schema timestamp of ${groupByServingInfo.batchEndTsMillis}.
                 |Forcing an update of schema.""".stripMargin)
              getGroupByServingInfo.force(request.name)
            } else {
              groupByServingInfo
            }
            val groupByContext = FetcherMetrics.getGroupByContext(groupByServingInfo, Some(context))
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
              if (streamingResponses.nonEmpty) {
                val streamingContext = groupByContext.asStreaming
                // report streaming metrics.
                FetcherMetrics.reportDataFreshness(streamingResponses.iterator.map(_.millis).max - startTimeMs,
                                                   streamingContext)
                FetcherMetrics.reportResponseBytesSize(streamingResponses.iterator.map(_.bytes.length).sum,
                                                       streamingContext)
                FetcherMetrics.reportResponseNumRows(streamingResponses.length, streamingContext)
              }
              val batchIr = toBatchIr(batchBytes, servingInfo)
              val queryTs = atMillis.getOrElse(System.currentTimeMillis())
              val output = aggregator.lambdaAggregateFinalized(batchIr, streamingRows, queryTs)
              servingInfo.outputCodec.fieldNames.zip(output.map(_.asInstanceOf[AnyRef])).toMap
            }
            val batchIr = toBatchIr(batchBytes, servingInfo)
            val queryTs = atMillis.getOrElse(System.currentTimeMillis())
            val output = aggregator.lambdaAggregateFinalized(batchIr, streamingRows, queryTs)
            servingInfo.outputCodec.fieldNames.zip(output.map(_.asInstanceOf[AnyRef])).toMap
          }
          FetcherMetrics.reportLatency(System.currentTimeMillis() - startTimeMs, groupByContext)
          Response(request, responseMap)
      }.toList
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
    groupByResponsesFuture
      .map { groupByResponses =>
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

  private def reportFailure(requests: Seq[Request], withTag: String => Metrics.Context, e: Exception) = {
    requests.foreach { req =>
      val context = withTag(req.name)
      FetcherMetrics.reportFailure(e, context)
    }
  }
}

object Fetcher {
  case class Request(name: String, keys: Map[String, AnyRef], atMillis: Option[Long] = None)
  case class Response(request: Request, values: Map[String, AnyRef])

  class Args(arguments: Seq[String]) extends ScallopConf(arguments) {
    val keyJson: ScallopOption[String] = opt[String](required = true, descr = "json of the keys to fetch")
    val name: ScallopOption[String] = opt[String](required = true, descr = "name of the join/group-by to fetch")
    val `type`: ScallopOption[String] = choice(Seq("join", "group-by"), descr = "the type of conf to fetch")
    verify()
  }

  def run(args: Args, onlineImpl: OnlineImpl): Unit = {
    val gson = (new GsonBuilder()).setPrettyPrinting().create()
    val keyMap = gson.fromJson(args.keyJson(), classOf[java.util.Map[String, AnyRef]]).asScala.toMap

    val fetcher = new Fetcher(onlineImpl.genKvStore)
    val startNs = System.nanoTime
    val requests = Seq(Fetcher.Request(args.name(), keyMap))
    val resultFuture = if (args.`type`() == "join") {
      fetcher.fetchJoin(requests)
    } else {
      fetcher.fetchGroupBys(requests)
    }
    val result = Await.result(resultFuture, 1.minute)
    val awaitTimeMs = (System.nanoTime - startNs) / 1e6d

    // treeMap to produce a sorted result
    val tMap = new java.util.TreeMap[String, AnyRef]()
    result.head.values.foreach { case (k, v) => tMap.put(k, v) }
    println(gson.toJson(tMap))
    println(s"Fetched in: $awaitTimeMs ms")

    sys.exit(0)
  }

  def main(args: Array[String]): Unit = {
    val gson = (new GsonBuilder()).setPrettyPrinting().create()
    val keyMap = gson
      .fromJson(
        """{
          |"key1": "value1", "key2": 3748, "key3": 0.42389, 
          |"key4": [0, 1, 2, 3], 
          |"key5": {"sub1": "val1", "sub2": "val2"}}
          |""".stripMargin,
        classOf[java.util.Map[String, AnyRef]]
      )
      .asScala
      .toMap

    // treeMap to produce a sorted result
    val tMap = new java.util.TreeMap[String, AnyRef]()
    keyMap.foreach { case (k, v) => tMap.put(k, v) }
    println(gson.toJson(tMap))
  }
}
