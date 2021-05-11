package ai.zipline.fetcher

import ai.zipline.aggregator.base.StructType
import ai.zipline.aggregator.row.Row
import ai.zipline.aggregator.windowing.{FinalBatchIr, SawtoothOnlineAggregator, TsUtils}
import ai.zipline.api.Extensions._
import ai.zipline.api._
import ai.zipline.fetcher.KVStore.{GetRequest, GetResponse, PutRequest, TimedValue}
import com.google.gson.Gson
import org.apache.avro.Schema
import org.apache.avro.generic.{GenericData, GenericRecord}

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

  def bulkPut(sourceOfflineTable: String, destinationOnlineDataSet: String, partition: String): Unit

  // helper methods
  def get(request: GetRequest)(implicit executionContext: ExecutionContext): Future[GetResponse] =
    multiGet(Seq(request)).map(_.head)
  def put(putRequest: PutRequest): Unit = multiPut(Seq(putRequest))

  def getString(key: String, dataset: String, timeoutMillis: Long)(implicit
      executionContext: ExecutionContext): String = {
    val fetchRequest = KVStore.GetRequest(key.getBytes(Constants.UTF8), dataset)
    val responseFuture = get(fetchRequest)
    val response = Await.result(responseFuture, Duration(timeoutMillis, MILLISECONDS))
    new String(response.values.head.bytes, Constants.UTF8)
  }
}

object KVStore {
  // a scan request essentially for the keyBytes
  // afterTsMillis - is used to limit the scan to more recent data
  case class GetRequest(keyBytes: Array[Byte], dataset: String, afterTsMillis: Option[Long] = None)
  case class TimedValue(bytes: Array[Byte], millis: Long)
  case class GetResponse(request: GetRequest, values: Seq[TimedValue])
  case class PutRequest(keyBytes: Array[Byte], valueBytes: Array[Byte], dataset: String, tsMillis: Option[Long] = None)
}

// mixin class - with schema
class GroupByServingInfoParsed(groupByServingInfo: GroupByServingInfo)
    extends GroupByServingInfo(groupByServingInfo)
    with Serializable {

  // streaming starts scanning after batchEnd
  val batchEndTsMillis: Long = Constants.Partition.epochMillis(batchEndDate)

  lazy val aggregator = {
    val avroSchemaParser = new Schema.Parser()
    val avroInputSchema = avroSchemaParser.parse(selectedAvroSchema)
    val ziplineInputSchema =
      AvroUtils.toZiplineSchema(avroInputSchema).asInstanceOf[StructType].unpack
    new SawtoothOnlineAggregator(batchEndTsMillis, groupByServingInfo.groupBy.aggregations.asScala, ziplineInputSchema)
  }

  lazy val irZiplineSchema: StructType =
    StructType.from(s"${groupBy.metaData.cleanName}_IR", aggregator.batchIrSchema)

  lazy val keyCodec: AvroCodec = AvroCodec.of(keyAvroSchema)
  lazy val selectedCodec: AvroCodec = AvroCodec.of(selectedAvroSchema)
  lazy val irCodec: AvroCodec = {
    val irAvroSchema: String = AvroUtils.fromZiplineSchema(irZiplineSchema).toString()
    AvroCodec.of(irAvroSchema)
  }
  lazy val outputCodec: AvroCodec = {
    val outputZiplineSchema =
      StructType.from(s"${groupBy.metaData.cleanName}_OUTPUT", aggregator.windowedAggregator.outputSchema)
    val outputAvroSchema = AvroUtils.fromZiplineSchema(outputZiplineSchema).toString()
    AvroCodec.of(outputAvroSchema)
  }
}

class MetadataStore(kvStore: KVStore, val dataset: String = "ZIPLINE_METADATA", timeoutMillis: Long = 2000)(implicit
    ex: ExecutionContext) {
  // a simple thread safe cache for metadata & schemas
  private def memoize[I, O](f: I => O): I => O =
    new mutable.HashMap[I, O]() {
      override def apply(key: I): O = this.synchronized(getOrElseUpdate(key, f(key)))
    }

  lazy val getJoinConf: String => Join = memoize { name =>
    ThriftJsonCodec
      .fromJsonStr[Join](kvStore.getString(s"joins/$name", dataset, timeoutMillis), check = true, classOf[Join])
  }

  def putJoinConf(join: Join): Unit = {
    kvStore.put(
      PutRequest(s"joins/${join.metaData.name}".getBytes(Constants.UTF8),
                 ThriftJsonCodec.toJsonStr(join).getBytes(Constants.UTF8),
                 dataset))
  }

  lazy val getGroupByServingInfo: String => GroupByServingInfoParsed = memoize { name =>
    val batchDataset = s"${name.sanitize.toUpperCase()}_BATCH"
    val metaData =
      kvStore.getString(Constants.GroupByServingInfoKey, s"${name.sanitize.toUpperCase()}_BATCH", timeoutMillis)
    println(s"Fetched ${Constants.GroupByServingInfoKey} from : $batchDataset\n$metaData")
    val groupByServingInfo = ThriftJsonCodec
      .fromJsonStr[GroupByServingInfo](metaData, check = true, classOf[GroupByServingInfo])
    new GroupByServingInfoParsed(groupByServingInfo)
  }
}

object Fetcher {
  case class Request(name: String, keys: Map[String, AnyRef], atMillis: Option[Long] = None)
  case class Response(request: Request, values: Map[String, AnyRef])
}
import Fetcher._

class Fetcher(kvStore: KVStore, metaDataSet: String = "ZIPLINE_METADATA", timeoutMillis: Long = 2000)(implicit
    ex: ExecutionContext)
    extends MetadataStore(kvStore, metaDataSet, timeoutMillis) {

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
        case Accuracy.TEMPORAL =>
          Some(
            GetRequest(keyBytes,
                       groupByServingInfo.groupBy.streamingDataset,
                       Some(groupByServingInfo.batchEndTsMillis)))
        // no further aggregation is required - the value in KVstore is good as is
        case Accuracy.SNAPSHOT => None
      }
      request -> (groupByServingInfo, batchRequest, streamingRequestOpt, request.atMillis)
    }.toMap
    val allRequests: Seq[GetRequest] = groupByRequestToKvRequest.values.flatMap {
      case (_, batchRequest, streamingRequestOpt, _) =>
        Some(batchRequest) ++ streamingRequestOpt
    }.toSeq

    val kvResponseFuture: Future[Seq[GetResponse]] = kvStore.multiGet(allRequests)

    // map all the kv store responses back to groupBy level responses
    kvResponseFuture.map { responsesFuture: Seq[GetResponse] =>
      val responsesMap = responsesFuture.map { response =>
        response.request -> response.values
      }.toMap
      val responses: Seq[Response] = groupByRequestToKvRequest.map {
        case (request, (groupByServingInfo, batchRequest, streamingRequestOpt, atMillis)) =>
          val batchResponseBytes: Array[Byte] =
            Option(responsesMap(batchRequest)).flatMap(_.headOption).map(_.bytes).orNull
          val responseMap: Map[String, AnyRef] = if (groupByServingInfo.groupBy.aggregations == null) { // no-agg
            groupByServingInfo.selectedCodec.decodeMap(batchResponseBytes)
          } else if (streamingRequestOpt.isEmpty) { // snapshot accurate
            groupByServingInfo.outputCodec.decodeMap(batchResponseBytes)
          } else { // temporal accurate
            val aggregator: SawtoothOnlineAggregator = groupByServingInfo.aggregator
            val streamingResponses: Seq[TimedValue] =
              responsesMap.get(streamingRequestOpt.get).flatMap(Option(_)).getOrElse(Seq.empty)
            val streamingRows: Seq[Row] =
              streamingResponses.map(tVal => groupByServingInfo.selectedCodec.decodeRow(tVal.bytes, tVal.millis))
            val batchIr = toBatchIr(batchResponseBytes, groupByServingInfo)
            val queryTs = atMillis.getOrElse(System.currentTimeMillis())
            val output = aggregator.lambdaAggregateFinalized(batchIr, streamingRows.iterator, queryTs)
            groupByServingInfo.outputCodec.fieldNames.zip(output.map(_.asInstanceOf[AnyRef])).toMap
          }
          Response(request, responseMap)
      }.toSeq
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
            val rightKeys = request.keys.filterKeys(leftToRight.contains).map {
              case (leftKey, value) => leftToRight(leftKey) -> value
            }
            val prefix = (Option(part.prefix) ++ Some(part.groupBy.getMetaData.cleanName)).mkString("_")
            request -> (prefix -> Request(groupByName, rightKeys, request.atMillis))
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
              responseMap
                .get(groupByRequest)
                .flatMap(Option(_))
                .map {
                  _.map { case (aggName, aggValue) => prefix + "_" + aggName -> aggValue }
                }
                .getOrElse(Seq.empty)
          }.toMap
          Response(joinRequest, joinValues)
      }.toSeq
    }
  }

}
