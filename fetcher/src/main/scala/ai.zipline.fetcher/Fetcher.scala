package ai.zipline.fetcher

import ai.zipline.aggregator.base.StructType
import ai.zipline.aggregator.windowing.{FinalBatchIr, SawtoothOnlineAggregator}
import ai.zipline.api.Extensions._
import ai.zipline.api._
import ai.zipline.fetcher.KVStore.{GetRequest, GetResponse, PutRequest}
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.concurrent.duration.{Duration, MILLISECONDS}
import scala.concurrent.{Await, ExecutionContext, Future}

// the main system level api for online storage integration
// used for fetcher + streaming + bulk uploads
trait KVStore {
  def create(dataset: String): Unit
  def multiGet(requests: Seq[GetRequest]): Future[Seq[GetResponse]]
  def multiPut(keyValueDatasets: Seq[PutRequest]): Unit

  def bulkPut(sourceOfflineTable: String, destinationOnlineDataSet: String, tsMillis: Long): Unit

  // helper methods
  def get(request: GetRequest)(implicit executionContext: ExecutionContext): Future[GetResponse] =
    multiGet(Seq(request)).map(_.head)
  def put(putRequest: PutRequest): Unit = multiPut(Seq(putRequest))
}

object KVStore {
  // a scan request essentially for the keyBytes
  // afterTsMillis - is used to limit the scan to more recent data
  case class GetRequest(keyBytes: Array[Byte], dataset: String, afterTsMillis: Option[Long] = None)
  case class GetResponse(request: GetRequest, values: Seq[Array[Byte]])
  case class PutRequest(keyBytes: Array[Byte], valueBytes: Array[Byte], dataset: String, tsMillis: Option[Long] = None)
}

class MetadataStore(kvStore: KVStore, metaDataSet: String = "ZIPLINE_METADATA", timeoutMillis: Long = 2000)(implicit
    ex: ExecutionContext) {
  protected val timeout = Duration(timeoutMillis.toDouble, MILLISECONDS)

  def getMetadata(key: String, dataset: String = metaDataSet): Array[Byte] = {
    val fetchRequest = KVStore.GetRequest(key.getBytes, dataset)
    val responseFuture = kvStore.get(fetchRequest)
    val response = Await.result(responseFuture, timeout)
    response.values.head
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

  def putGroupByServingInfo(groupByServingInfo: GroupByServingInfo): Unit = {
    kvStore.put(
      PutRequest(s"group_by/${groupByServingInfo.groupBy.metaData.name}/serving_info".getBytes,
                 ThriftJsonCodec.toJsonStr(groupByServingInfo).getBytes,
                 metaDataSet))
  }

  // mixin class - with schema
  class GroupByServingInfoParsed(groupByServingInfo: GroupByServingInfo)
      extends GroupByServingInfo(groupByServingInfo) {
    private val avroSchemaParser = new Schema.Parser()
    private def codec(schemaStr: String): AvroCodec = AvroCodec.of(schemaStr)
    val keyCodec: AvroCodec = AvroCodec.of(keyAvroSchema)
    val inputCodec: AvroCodec = AvroCodec.of(inputAvroSchema)
    // streaming needs to start scanning after this
    val batchEndTsMillis: Long = Constants.Partition.epochMillis(batchDateStamp)
    private val avroInputSchema = avroSchemaParser.parse(inputAvroSchema)
    private val ziplineInputSchema =
      AvroUtils.toZiplineSchema(avroInputSchema).asInstanceOf[StructType].unpack
    val aggregationsOpt = Option(groupByServingInfo.groupBy.aggregations)
    val aggregatorOpt = aggregationsOpt.map { aggs =>
      new SawtoothOnlineAggregator(batchEndTsMillis, aggs.asScala, ziplineInputSchema)
    }
    val irCodecOpt: Option[AvroCodec] = aggregatorOpt
      .map { agg =>
        val irZiplineSchema = StructType.from(s"${groupBy.metaData.cleanName}_IR", agg.batchIrSchema)
        val irAvroSchema = AvroUtils.fromZiplineSchema(irZiplineSchema).toString()
        AvroCodec.of(irAvroSchema)
      }

    val outputCodecOpt: Option[AvroCodec] = aggregatorOpt
      .map { agg =>
        val outputZiplineSchema =
          StructType.from(s"${groupBy.metaData.cleanName}_OUTPUT", agg.windowedAggregator.outputSchema)
        val outputAvroSchema = AvroUtils.fromZiplineSchema(outputZiplineSchema).toString()
        AvroCodec.of(outputAvroSchema)
      }
  }

}

class Fetcher(kvStore: KVStore, metaDataSet: String = "ZIPLINE_METADATA", timeoutMillis: Long = 2000)(implicit
    ex: ExecutionContext)
    extends MetadataStore(kvStore, metaDataSet, timeoutMillis) {

  // the request and response classes are similar for both groupBy and Join
  case class Request(name: String, keys: Map[String, AnyRef])
  case class Response(request: Request, values: Map[String, AnyRef])

  // 1. fetches GroupByServingInfo
  // 2. encodes keys as keyAvroSchema)
  // 3. Based on accuracy, fetches streaming + batch data and aggregates further.
  // 4. Finally converted to outputSchema
  def fetchGroupBys(requests: Seq[Request]): Future[Seq[Response]] = {
    // split a groupBy level request into its kvStore level requests
    val groupByRequestToKvRequest = requests.map { request =>
      val groupByName = request.name
      val groupByServingInfo = getGroupByServingInfo(groupByName)
      val keyBytes = groupByServingInfo.keyCodec.encode(request.keys)
      val batchRequest = GetRequest(keyBytes, groupByServingInfo.groupBy.batchDataset)
      val streamingRequestOpt = groupByServingInfo.groupBy.inferredAccuracy match {
        // fetch batch(ir) and streaming(input) and aggregate
        case Accuracy.TEMPORAL => Some(GetRequest(keyBytes, groupByServingInfo.groupBy.streamingDataset))
        // no further aggregation is required - the value in KVstore is good as is
        case Accuracy.SNAPSHOT => None
      }
      request -> (groupByServingInfo, batchRequest, streamingRequestOpt)
    }.toMap
    val allRequests: Seq[GetRequest] = groupByRequestToKvRequest.values.flatMap {
      case (_, batchRequest, streamingRequestOpt) =>
        Some(batchRequest) ++ streamingRequestOpt
    }.toSeq

    val kvResponseFuture: Future[Seq[GetResponse]] = kvStore.multiGet(allRequests)

    // map all the kv store responses back to groupBy level responses
    kvResponseFuture.map { responsesFuture: Seq[GetResponse] =>
      val responsesMap = responsesFuture.map { response => response.request -> response.values }.toMap
      val responses: Seq[Response] = groupByRequestToKvRequest
        .mapValues {
          case (groupByServingInfo, batchRequest, streamingRequestOpt) =>
            lazy val batchResponseBytes = responsesMap(batchRequest).headOption.orNull
            val outputCodec = groupByServingInfo.outputCodecOpt.get
            val responseMap: Map[String, AnyRef] = if (groupByServingInfo.groupBy.aggregations == null) { // no-agg
              groupByServingInfo.inputCodec.decodeMap(batchResponseBytes)
            } else if (streamingRequestOpt.isEmpty) { // snapshot accurate
              groupByServingInfo.outputCodecOpt.get.decodeMap(batchResponseBytes)
            } else { // temporal accurate
              val aggregator = groupByServingInfo.aggregatorOpt.get
              val irCodec = groupByServingInfo.irCodecOpt.get
              val streamingResponses = streamingRequestOpt.map(responsesMap).get
              val streamingRows = streamingResponses.iterator.map(groupByServingInfo.inputCodec.decodeRow)
              val batchRecord = irCodec.decode(batchResponseBytes)
              val collapsed = recordToArray(batchRecord.get(0), aggregator.windowedAggregator.length)
              val tailHops = batchRecord.get(1).asInstanceOf[Array[AnyRef]].map { hops =>
                hops
                  .asInstanceOf[Array[AnyRef]]
                  .map(hop => recordToArray(hop, aggregator.baseAggregator.length))
              }
              val batchIr = FinalBatchIr(collapsed, tailHops)
              val output = groupByServingInfo.aggregatorOpt.get
                .lambdaAggregateFinalized(batchIr, streamingRows, System.currentTimeMillis())
              outputCodec.fieldNames.zip(output.map(_.asInstanceOf[AnyRef])).toMap
            }
            responseMap
        }
        .map { case (request, responseMap) => Response(request, responseMap) }
        .toSeq
      responses
    }
  }

  private def recordToArray(rec: AnyRef, len: Int): Array[Any] = {
    val grec = rec.asInstanceOf[GenericRecord]
    (0 until len).map(grec.get).toArray
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
    // re-attach groupBy responses to join
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
