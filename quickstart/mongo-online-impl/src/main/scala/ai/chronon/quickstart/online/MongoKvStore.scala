import ai.chronon.online.KVStore
import ai.chronon.online.KVStore._
import org.mongodb.scala._
import org.mongodb.scala.model.Filters._

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}


/**
 * A KVStore implementation backed by MongoDB.
 * Databases : [dataset]_realtime, [dataset]_batch.
 *
 */
class MongoKvStore(mongoClient: MongoClient, databaseName: String, dataset: String)
                    (implicit ec: ExecutionContext) extends KVStore {

  private val collection: MongoCollection[Document] =
    mongoClient.getDatabase(databaseName).getCollection(dataset)

  override def create(dataset: String): Unit = mongoClient.getDatabase(databaseName).createCollection(dataset)

  override def multiGet(requests: Seq[GetRequest]): Future[Seq[GetResponse]] = {
    val futures = requests.map { request =>
      val filter = and(equal("key", request.keyBytes), equal("dataset", request.dataset))
      collection.find(filter).limit(1).toFuture().map { documents =>
        if (documents.isEmpty) {
          GetResponse(request, Failure(new NoSuchElementException("Key not found")))
        } else {
          GetResponse(request, Try(
            documents.map(document =>
              TimedValue(
                document.get("value").get.asBinary().getData,
                document.getOrElse("timestamp", System.currentTimeMillis()).asInstanceOf[Long])
            )))
        }
      }
    }
    Future.sequence(futures)
  }

  // Move to insertMany grouped by dataset.
  override def multiPut(putRequests: Seq[PutRequest]): Future[Seq[Boolean]] = {
    val futures = putRequests.map { putRequest =>
      val document = Document(
        "key" -> putRequest.keyBytes, "value" -> putRequest.valueBytes, "timestamp" -> putRequest.dataset, "dataset" -> putRequest.dataset)
      collection.insertOne(document).toFuture().map(_ => true).recover { case _ => false }
    }
    Future.sequence(futures)
  }

  override def bulkPut(sourceOfflineTable: String, destinationOnlineDataSet: String, partition: String): Unit = {
    // Logic to perform bulk put operation (if applicable)
  }
}
