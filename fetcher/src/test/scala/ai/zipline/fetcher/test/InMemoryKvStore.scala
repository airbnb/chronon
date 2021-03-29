package ai.zipline.fetcher.test

import ai.zipline.fetcher.KVStore
import ai.zipline.fetcher.KVStore.PutRequest

import scala.collection.{mutable, parallel}
import scala.concurrent.{ExecutionContext, Future}

class InMemoryKvStore(implicit ec: ExecutionContext) extends KVStore {
  //type aliases for readability
  type Key = Array[Byte]
  type Data = Array[Byte]
  type DataSet = String
  type Version = Long
  type Table = parallel.mutable.ParHashMap[Key, mutable.Buffer[(Version, Data)]]
  private val database = parallel.mutable.ParHashMap.empty[DataSet, Table]

  override def multiGet(requests: Seq[KVStore.GetRequest]): Future[Seq[KVStore.GetResponse]] = {
    Future {
      requests.map { req =>
        val valueBytes = database(req.dataset) // table
          .get(req.keyBytes) // values of key
          .map { values =>
            values
              .filter { case (version, _) => req.afterTsMillis.forall(_ >= version) } // filter version
              .map(_._2) // select data
          }
          .orNull
        KVStore.GetResponse(req, valueBytes)
      }
    }
  }

  override def multiPut(putRequests: Seq[KVStore.PutRequest]): Unit = {
    putRequests.foreach {
      case PutRequest(keyBytes, valueBytes, dataset, millis) =>
        val table = database(dataset)
        synchronized {
          if (!table.contains(keyBytes)) {
            table.put(keyBytes, mutable.Buffer.empty[(Version, Data)])
          }
          table(keyBytes).append((millis.getOrElse(System.currentTimeMillis()), valueBytes))
        }
    }
  }

  // not implemented - because we simulate bulkPuts using multiPut in testing
  override def bulkPut(sourceOfflineTable: String, destinationOnlineDataSet: String, tsMillis: Long): Unit = ???

  override def create(dataset: String): Unit = {
    database.put(dataset, parallel.mutable.ParHashMap.empty[Key, mutable.Buffer[(Version, Data)]])
  }
}
