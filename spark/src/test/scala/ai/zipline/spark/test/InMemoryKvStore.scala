package ai.zipline.spark.test

import ai.zipline.api.Constants
import ai.zipline.fetcher.KVStore
import ai.zipline.fetcher.KVStore.PutRequest
import ai.zipline.spark.{SparkSessionBuilder, TableUtils}
import com.google.gson.Gson

import java.util.Base64
import scala.collection.{mutable, parallel}
import scala.concurrent.{ExecutionContext, Future}

class InMemoryKvStore(implicit ec: ExecutionContext, tableUtils: TableUtils) extends KVStore {
  //type aliases for readability
  type Key = String
  type Data = Array[Byte]
  type DataSet = String
  type Version = Long
  type Table = parallel.mutable.ParHashMap[Key, mutable.Buffer[(Version, Data)]]
  private val database = parallel.mutable.ParHashMap.empty[DataSet, Table]

  val encoder = Base64.getEncoder()
  def encode(bytes: Array[Byte]) = encoder.encodeToString(bytes)
  def toStr(bytes: Array[Byte]): String = new String(bytes, Constants.UTF8)
  override def multiGet(requests: Seq[KVStore.GetRequest]): Future[Seq[KVStore.GetResponse]] = {
    Future {
      requests.map { req =>
        val gson = new Gson()
        println(s"keyBytes: ${gson.toJson(req.keyBytes)}")
        println(s"datasets: [${database.keys.mkString(", ")}]")
        val table = database(req.dataset)
        assert(database.keys.exists(req.dataset == _), s"dataset ${req.dataset} doesn't exist")
        // assert(table.keys.exists(req.keyBytes == _), s"key ${toStr(req.keyBytes)} doesn't exist")

        val valueBytes = database(req.dataset) // table
          .get(encode(req.keyBytes)) // values of key
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
    println(
      s"Writing ${putRequests.size} requests to [${putRequests.iterator.map(_.dataset).toSet.mkString(",")}] datasets.")
    putRequests.foreach {
      case PutRequest(keyBytes, valueBytes, dataset, millis) =>
        val table = database(dataset)
        val key = encode(keyBytes)
        synchronized {
          if (!table.contains(key)) {
            table.put(key, mutable.Buffer.empty[(Version, Data)])
          }
          table(key).append((millis.getOrElse(System.currentTimeMillis()), valueBytes))
        }
    }
  }

  // the table is assumed to be encoded with two columns - `key` and `value` as Array[Bytes]
  // one of the keys should be "group_by_serving_info" as bytes with value as TSimpleJsonEncoded String
  override def bulkPut(sourceOfflineTable: String, destinationOnlineDataSet: String, partition: String): Unit = {
    val partitionFilter =
      Option(partition).map { part => s"WHERE ${Constants.PartitionColumn} = '$part'" }.getOrElse("")
    val df = tableUtils.sql(s"""SELECT key, value 
         |FROM $sourceOfflineTable
         |$partitionFilter""".stripMargin)
    val requests = df.rdd
      .collect()
      .map { row =>
        val key = row.get(0).asInstanceOf[Array[Byte]]
        val value = row.get(1).asInstanceOf[Array[Byte]]
        KVStore.PutRequest(key, value, destinationOnlineDataSet)
      }

    create(destinationOnlineDataSet)
    multiPut(requests)
  }

  override def create(dataset: String): Unit = {
    database.put(dataset, parallel.mutable.ParHashMap.empty[Key, mutable.Buffer[(Version, Data)]])
  }
}
