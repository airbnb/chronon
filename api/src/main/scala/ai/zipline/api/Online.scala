package ai.zipline.api

import ai.zipline.api.KVStore.{GetRequest, GetResponse, PutRequest}
import java.util.concurrent.Executors
import scala.concurrent.duration.{Duration, MILLISECONDS}
import scala.concurrent.{Await, ExecutionContext, Future}

object KVStore {
  // a scan request essentially for the keyBytes
  // afterTsMillis - is used to limit the scan to more recent data
  case class GetRequest(keyBytes: Array[Byte], dataset: String, afterTsMillis: Option[Long] = None)
  case class TimedValue(bytes: Array[Byte], millis: Long)
  case class GetResponse(request: GetRequest, values: Seq[TimedValue]) {
    def latest: Option[TimedValue] = if (values.isEmpty) None else Some(values.maxBy(_.millis))
  }
  case class PutRequest(keyBytes: Array[Byte], valueBytes: Array[Byte], dataset: String, tsMillis: Option[Long] = None)
}

// the main system level api for key value storage
// used for streaming writes, batch bulk uploads & fetching
trait KVStore {
  private val threadPool = Executors.newWorkStealingPool()
  implicit val executionContext: ExecutionContext = ExecutionContext.fromExecutor(threadPool)

  def create(dataset: String): Unit
  def multiGet(requests: Seq[GetRequest]): Future[Seq[GetResponse]]
  def multiPut(keyValueDatasets: Seq[PutRequest]): Future[Seq[Boolean]]

  def bulkPut(sourceOfflineTable: String, destinationOnlineDataSet: String, partition: String): Unit

  // helper methods to do single put and single get
  def get(request: GetRequest): Future[GetResponse] = multiGet(Seq(request)).map(_.head)
  def put(putRequest: PutRequest): Future[Boolean] = multiPut(Seq(putRequest)).map(_.head)

  def close(): Unit = {
    threadPool.shutdown()
  }

  // helper method to blocking read a string - used for fetching metadata & not in hotpath.
  def getString(key: String, dataset: String, timeoutMillis: Long): String = {
    val fetchRequest = KVStore.GetRequest(key.getBytes(Constants.UTF8), dataset)
    val responseFuture = get(fetchRequest)
    val response = Await.result(responseFuture, Duration(timeoutMillis, MILLISECONDS))
    assert (response.latest.isDefined, s"we should have a string response")
    new String(response.latest.get.bytes, Constants.UTF8)
  }
}

/**
  * ==== MUTATION vs. EVENT ====
  * Mutation is the general case of an Event
  * Imagine a user impression/view stream - impressions/views are immutable events
  * Imagine a stream of changes to a credit card transaction stream.
  *    - transactions can be "corrected"/updated & deleted, besides being "inserted"
  *    - This is one of the core difference between entity and event sources. Events are insert-only.
  *    - (The other difference is Entites are stored in the warehouse typically as snapshots of the table as of midnight)
  * In case of an update - one must produce both before and after values
  * In case of a delete - only before is populated & after is left as null
  * In case of a insert - only after is populated & before is left as null

  * ==== TIME ASSUMPTIONS ====
  * The schema needs to contain a `ts`(milliseconds as a java Long)
  * For the entities case, `mutation_ts` when absent will use `ts` as a replacement

  * ==== TYPE CONVERSIONS ====
  * Java types corresponding to the schema types. Stream [[Decoder]] should produce mutations that comply.
  * NOTE: everything is nullable (hence boxed)
  * IntType        java.lang.Integer
  * LongType       java.lang.Long
  * DoubleType     java.lang.Double
  * FloatType      java.lang.Float
  * ShortType      java.lang.Short
  * BooleanType    java.lang.Boolean
  * ByteType       java.lang.Byte
  * StringType     java.lang.String
  * BinaryType     Array[Byte]
  * ListType       java.util.List[Byte]
  * MapType        java.util.Map[Byte]
  * StructType     Array[Any]
  */
case class Mutation(schema: StructType = null, before: Array[AnyRef] = null, after: Array[AnyRef] = null)

trait Decoder extends Serializable {
  def decode(bytes: Array[Byte]): Mutation
}


abstract class OnlineImpl(userConf: Map[String, String]) extends Serializable {
  // helper method to access property
  // -Dkey1=value -Dkey2=value2  from scallop gets converted to userConf.
  def getProperty(key: String): String = userConf(key)

  def genStreamDecoder(inputSchema: StructType): Decoder

  // users can set transform input avro schema from batch source to streaming compatible schema
  def batchInputAvroSchemaToStreaming(batchSchema: StructType): StructType

  def genKvStore: KVStore
}
