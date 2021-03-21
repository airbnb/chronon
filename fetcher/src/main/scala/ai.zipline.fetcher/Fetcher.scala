package ai.zipline.fetcher

import ai.zipline.aggregator.base.StructType
import ai.zipline.aggregator.row.RowAggregator
import ai.zipline.api.Extensions._
import ai.zipline.api.{Accuracy, Constants, GroupBy, GroupByServingInfo, Join, ThriftJsonCodec}
import ai.zipline.fetcher.KeyValueStore.{GetRequest, GetResponse, PutRequest}
import org.apache.avro.Schema

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.concurrent.duration.{Duration, MILLISECONDS}
import scala.concurrent.{Await, ExecutionContext, Future}

// the main system level api for online storage integration
// used for fetcher + streaming + bulk uploads
trait KeyValueStore {
  def multiGet(requests: Seq[GetRequest]): Future[Seq[GetResponse]]
  def put(keyValueDataset: PutRequest): Unit
  def bulkPut(sourceOfflineTable: String, destinationOnlineDataSet: String, tsMillis: Long): Unit
}

object KeyValueStore {
  case class GetRequest(keyBytes: Array[Byte], dataset: String, afterTsMillis: Option[Long] = None)
  case class GetResponse(request: GetRequest, valueBytes: Array[Byte])
  case class PutRequest(keyBytes: Array[Byte], valueBytes: Array[Byte], dataset: String, tsMillis: Option[Long] = None)
}

class MetadataStore(kvStore: KeyValueStore, metaDataSet: String = "ZIPLINE_METADATA", timeoutMillis: Long = 2000) {
  protected val timeout = Duration(timeoutMillis.toDouble, MILLISECONDS)

  def getMetadata(key: String): Array[Byte] = {
    val fetchRequest = KeyValueStore.GetRequest(key.getBytes, metaDataSet)
    val responseFuture = kvStore.multiGet(Seq(fetchRequest))
    val response = Await.result(responseFuture, timeout)
    response.head.valueBytes
  }

  // a simple thread safe cache for metadata & schemas
  private def memoize[I, O](f: I => O): I => O =
    new mutable.HashMap[I, O]() {
      override def apply(key: I): O = this.synchronized(getOrElseUpdate(key, f(key)))
    }

  lazy val getJoinConf: String => Join = memoize { name =>
    ThriftJsonCodec
      .fromJsonStr[Join](new String(getMetadata(s"join/$name")), check = true, classOf[Join])
  }

  def putJoinConf(join: Join): Unit = {
    kvStore.put(
      PutRequest(s"join/${join.metaData.name}".getBytes, ThriftJsonCodec.toJsonStr(join).getBytes, metaDataSet))
  }

  lazy val getGroupByServingInfo: String => GroupByServingInfoParsed = memoize { name =>
    val groupByServingInfo = ThriftJsonCodec
      .fromJsonStr[GroupByServingInfo](new String(getMetadata(s"group_by/$name/serving_info")),
                                       check = true,
                                       classOf[GroupByServingInfo])
    new GroupByServingInfoParsed(groupByServingInfo)
  }

  // mixin class - with schema
  class GroupByServingInfoParsed(groupByServingInfo: GroupByServingInfo)
      extends GroupByServingInfo(groupByServingInfo) {
    private val avroSchemaParser = new Schema.Parser()
    private def codec(schemaStr: String): AvroCodec = AvroCodec.of(avroSchemaParser.parse(schemaStr))
    val keyCodec: AvroCodec = codec(keyAvroSchema)
    val irCodec: AvroCodec = codec(irAvroSchema)
    val inputCodec: AvroCodec = codec(inputAvroSchema)
    // streaming needs to start scanning after this
    val batchEndTsMillis: Long = Constants.Partition.epochMillis(batchDateStamp)
    private val avroInputSchema = avroSchemaParser.parse(inputAvroSchema)
    private val ziplineInputSchema = AvroCodec.toZiplineSchema(avroInputSchema).asInstanceOf[StructType].unpack
    val aggregatorOpt = Option(groupByServingInfo.groupBy.aggregations).map { aggs =>
      new RowAggregator(ziplineInputSchema, aggs.asScala.flatMap(_.unpack))
    }

    def aggregateTemporal(batchResponse: Array[Byte], streamingResponses: Seq[Array[Byte]]): Array[Any] = {}
  }

  def putGroupByServingInfo(groupByServingInfo: GroupByServingInfo): Unit = {
    kvStore.put(
      PutRequest(s"group_by/${groupByServingInfo.groupBy.metaData.name}/serving_info".getBytes,
                 ThriftJsonCodec.toJsonStr(groupByServingInfo).getBytes,
                 metaDataSet))
  }
}

class Fetcher(kvStore: KeyValueStore, metaDataSet: String = "ZIPLINE_METADATA", timeoutMillis: Long = 2000)(implicit
    ex: ExecutionContext)
    extends MetadataStore(kvStore, metaDataSet, timeoutMillis) {

  // the request and response classes are similar for both groupBy and Join
  case class Request(name: String, keys: Map[String, AnyRef])
  case class Response(request: Request, values: Map[String, AnyRef])

  // 1. fetches GroupByServingInfo
  // 2. encodes keys as keyAvroSchema)
  // 3. Based on accuracy
  //   Temporal => a. fetches batch - (decoded as deltaAvroSchemas),
  //               b. fetches streaming - (decoded as inputAvroSchema)
  //                    TODO: scan query starting at batchUploadTime, ending at currentTime
  //               c. aggregates them into a outputAvroSchema
  //   False => fetches batch (outputSchema decoded)
  // 4. Finally converted to outputSchema
  def fetchGroupBys(requests: Seq[Request]): Future[Seq[Response]] = {
    val groupByRequestToKvRequest = requests.map { request =>
      val groupByName = request.name
      val groupByServingInfo = getGroupByServingInfo(groupByName)
      val keyBytes = groupByServingInfo.keyCodec.encode(request.keys)
      val batchRequest = GetRequest(keyBytes, groupByServingInfo.groupBy.batchDataset)
      val streamingRequestOpt = groupByServingInfo.groupBy.inferredAccuracy match {
        // fetch batch(ir) and streaming(input) and aggregate
        case Accuracy.TEMPORAL => Some(GetRequest(keyBytes, groupByServingInfo.groupBy.streamingDataset))
        // no further aggregation is required - the value in kvstore is good as is
        case Accuracy.SNAPSHOT => None
      }
      request -> (groupByServingInfo, batchRequest, streamingRequestOpt)
    }.toMap
    val allRequests: Seq[GetRequest] = groupByRequestToKvRequest.values.flatMap {
      case (_, batchReqeust, streamingRequestOpt) =>
        Some(batchReqeust) ++ streamingRequestOpt
    }.toSeq
    val responses = kvStore.multiGet(allRequests).map { responsesFuture =>
      val responsesMap = responsesFuture.map { response => response.request -> response.valueBytes }.toMap
      groupByRequestToKvRequest.mapValues {
        case (groupByServingInfo, batchRequest, streamingRequestOpt) =>
          val batchResponse = responsesMap(batchRequest)
          val streamingResponseOpt = streamingRequestOpt.map(responsesMap)
          streamingRequestOpt match {
            case Some(streamingRequest) =>
              groupByServingInfo.irCodec.decodeRow(batchResponse)
          }
      }
    }

  }

  def tuplesToMap[K, V](tuples: Seq[(K, V)]): Map[K, Seq[V]] = tuples.groupBy(_._1).mapValues(_.map(_._2))

  def fetchJoin(requests: Seq[Request]): Future[Seq[Response]] = {
    // convert join requests to groupBy requests
    val joinToGroupByRequestsMap: Map[Request, Seq[(String, Request)]] = tuplesToMap(
      requests
        .flatMap { request =>
          val join = getJoinConf(request.name)
          join.joinParts.asScala.map { part =>
            val groupByName = part.groupBy.getMetaData.getName
            val leftToRight = part.leftToRight
            val rightKeys = request.keys.map { case (leftKey, value) => leftToRight(leftKey) -> value }
            val prefix = Option(part.prefix).map(_ + "_").getOrElse("")
            request -> (prefix -> Request(groupByName, rightKeys))
          }
        })

    val uniqueRequests = joinToGroupByRequestsMap.values.flatten.map(_._2).toSet
    // fetch all unique groupBy requests
    val groupByResponsesFuture = fetchGroupBys(uniqueRequests.toSeq)
    // re-attach groupBy responses to
    groupByResponsesFuture.map { groupByResponses =>
      val responseMap = groupByResponses.map { response => response.request -> response.values }.toMap
      joinToGroupByRequestsMap.map {
        case (joinRequest, prefixedGroupByRequests) =>
          val joinValues = prefixedGroupByRequests.flatMap {
            case (prefix, groupByRequest) =>
              // this map lookup may fail. TODO: handle + log failure counters per (join, groupBy)
              responseMap(groupByRequest).map { case (aggName, aggValue) => prefix + aggName -> aggValue }
          }.toMap
          Response(joinRequest, joinValues)
      }.toSeq
    }
  }

}
