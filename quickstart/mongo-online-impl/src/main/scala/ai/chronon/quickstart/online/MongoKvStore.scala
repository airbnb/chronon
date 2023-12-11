package ai.chronon.quickstart.online

import ai.chronon.online.KVStore
import ai.chronon.online.KVStore._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.mongodb.scala._
import org.mongodb.scala.model.Filters._

import scala.concurrent.Future
import scala.util.{Failure, Success, Try}


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
      val filter = and(equal("keyBytes", request.keyBytes))
      collection.find(filter).limit(1).toFuture().map { documents =>
        if (documents.isEmpty) {
          GetResponse(request, Failure(new NoSuchElementException("Key not found")))
        } else {
          GetResponse(request, Try(
            documents.map(document =>
              TimedValue(
                document.get("valueBytes").get.asBinary().getData,
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
      val collection = mongoClient.getDatabase(databaseName).getCollection(putRequest.dataset)
      val document = Document(
        "keyBytes" -> putRequest.keyBytes, "valueBytes" -> putRequest.valueBytes, "timestamp" -> putRequest.dataset)
      collection.insertOne(document).toFuture().map(_ => true).recover { case _ => false }
    }
    Future.sequence(futures)
  }

  override def bulkPut(sourceOfflineTable: String, destinationOnlineDataSet: String, partition: String): Unit = {
    val spark: SparkSession = SparkSession.builder().appName("MongoDBBulkPut").getOrCreate()

    try {
      // Read data from the specified table using Spark
      val df: DataFrame = spark.table(sourceOfflineTable)

      // Transform DataFrame to a format suitable for MongoDB insertion
      val documents: Seq[Document] = df.collect().map { row =>
        // Map DataFrame columns to MongoDB document fields
        val keyBytes: Array[Byte] = row.getAs[Array[Byte]]("keyBytes")
        val valueBytes: Array[Byte] = row.getAs[Array[Byte]]("valueBytes")
        val timestamp: Long = row.getAs[Long]("timestamp")
        Document(
          "keyBytes" -> keyBytes,
          "valueBytes" -> valueBytes,
          "timestamp" -> timestamp
        )
      }

      // Insert the transformed data into MongoDB collection
      val mongoCollection = mongoClient.getDatabase(databaseName).getCollection(destinationOnlineDataSet)
      mongoCollection.insertMany(documents).toFuture().map(_ => println("Bulk insert successful"))
    } finally {
      spark.stop()
    }
  }
}
