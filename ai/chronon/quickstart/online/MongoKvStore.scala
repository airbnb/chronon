package ai.chronon.quickstart.online

import ai.chronon.online.KVStore
import ai.chronon.online.KVStore._
import org.mongodb.scala._
import org.mongodb.scala.model.Filters._
import scala.concurrent.Future
import scala.util.{Failure, Success, Try}
import java.util.Base64


/**
 * A KVStore implementation backed by MongoDB.
 * Databases : [dataset]_realtime, [dataset]_batch.
 *
 */
class MongoKvStore(mongoClient: MongoClient, databaseName: String) extends KVStore {

  override def create(dataset: String): Unit = mongoClient.getDatabase(databaseName).createCollection(dataset)

  override def multiGet(requests: Seq[GetRequest]): Future[Seq[GetResponse]] = {
    val futures = requests.map { request =>
      val collection = mongoClient.getDatabase(databaseName).getCollection(request.dataset)
      val filter = equal(Constants.mongoKey, request.keyBytes)
      collection.find(filter).limit(1).toFuture().map { documents =>
        if (documents.isEmpty) {
          GetResponse(request, Failure(new NoSuchElementException("Key not found")))
        } else {
          GetResponse(request, Try(
            documents.map(document =>
              TimedValue(
                document.get(Constants.mongoValue).get.asBinary().getData,
                System.currentTimeMillis())
            )))
        }
      }
    }
    Future.sequence(futures)
  }

  // Move to insertMany grouped by dataset.
  override def multiPut(putRequests: Seq[PutRequest]): Future[Seq[Boolean]] = {
    val futures = putRequests.map { putRequest =>
      val collection = mongoClient.getDatabase(databaseName).getCollection(putRequest.dataset)
      val document = Document(
        Constants.mongoKey -> putRequest.keyBytes,
        Constants.mongoValue -> putRequest.valueBytes,
        Constants.mongoTs-> putRequest.tsMillis)
      collection.insertOne(document).toFuture().map(_ => true).recover { case _ => false }
    }
    Future.sequence(futures)
  }

  override def bulkPut(sourceOfflineTable: String, destinationOnlineDataSet: String, partition: String): Unit = ???
}
