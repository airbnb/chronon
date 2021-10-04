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
  implicit val executionContext: ExecutionContext = ExecutionContext.fromExecutor(Executors.newWorkStealingPool())

  def create(dataset: String): Unit
  def multiGet(requests: Seq[GetRequest]): Future[Seq[GetResponse]]
  def multiPut(keyValueDatasets: Seq[PutRequest]): Future[Seq[Boolean]]

  def bulkPut(sourceOfflineTable: String, destinationOnlineDataSet: String, partition: String): Unit

  // helper methods to do single put and single get
  def get(request: GetRequest): Future[GetResponse] = multiGet(Seq(request)).map(_.headOption.orNull)
  def put(putRequest: PutRequest): Future[Boolean] = multiPut(Seq(putRequest)).map(_.headOption.getOrElse(false))
  // helper method to blocking read a string - used for fetching metadata & not in hotpath.
  def getString(key: String, dataset: String, timeoutMillis: Long): String = {
    val fetchRequest = KVStore.GetRequest(key.getBytes(Constants.UTF8), dataset)
    val responseFuture = get(fetchRequest)
    val response = Await.result(responseFuture, Duration(timeoutMillis, MILLISECONDS))
    assert(response.latest.isDefined, s"Failed to fetch key $key from dataset $dataset")
    new String(response.latest.get.bytes, Constants.UTF8)
  }
}
