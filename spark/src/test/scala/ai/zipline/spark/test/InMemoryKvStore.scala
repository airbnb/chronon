package ai.zipline.spark.test

import ai.zipline.api.{Constants, KVStore}
import KVStore.{PutRequest, TimedValue}
import ai.zipline.spark.TableUtils
import org.apache.spark.sql.Row
import org.jboss.netty.util.internal.ConcurrentHashMap

import java.util.Base64
import java.util.function.BiFunction
import scala.collection.{mutable, parallel}
import scala.concurrent.Future

class InMemoryKvStore(implicit tableUtils: TableUtils) extends KVStore {
  //type aliases for readability
  type Key = String
  type Data = Array[Byte]
  type DataSet = String
  type Version = Long
  type VersionedData = mutable.Buffer[(Version, Data)]
  type Table = ConcurrentHashMap[Key, VersionedData]
  private val database = new ConcurrentHashMap[DataSet, Table]

  private val encoder = Base64.getEncoder
  def encode(bytes: Array[Byte]): String = encoder.encodeToString(bytes)
  def toStr(bytes: Array[Byte]): String = new String(bytes, Constants.UTF8)
  override def multiGet(requests: Seq[KVStore.GetRequest]): Future[Seq[KVStore.GetResponse]] = {
    Future {
      // emulate IO latency
      Thread.sleep(4)
      requests.map { req =>
        val values = Option(
          database
            .get(req.dataset) // table
            .get(encode(req.keyBytes))
        ) // values of key
        .map { values =>
          values
            .filter { case (version, _) => req.afterTsMillis.forall(version >= _) } // filter version
            .map { case (version, bytes) => TimedValue(bytes, version) }
        }.orNull
        KVStore.GetResponse(req, values)
      }
    }
  }

  private def putFunc(newData: (Version, Data)) =
    new BiFunction[Key, VersionedData, VersionedData] {
      override def apply(t: Key, u: VersionedData): VersionedData = {
        val result = if (u == null) {
          mutable.Buffer.empty[(Version, Data)]
        } else u
        result.append(newData)
        result
      }
    }

  override def multiPut(putRequests: Seq[KVStore.PutRequest]): Future[Seq[Boolean]] = {
    Future {
      putRequests.map {
        case PutRequest(keyBytes, valueBytes, dataset, millis) =>
          val table = database.get(dataset)
          val key = encode(keyBytes)
          table.compute(key, putFunc(millis.getOrElse(System.currentTimeMillis()) -> valueBytes))
          true
      }
    }
  }

  // the table is assumed to be encoded with two columns - `key` and `value` as Array[Bytes]
  // one of the keys should be "group_by_serving_info" as bytes with value as TSimpleJsonEncoded String
  override def bulkPut(sourceOfflineTable: String, destinationOnlineDataSet: String, partition: String): Unit = {
    val partitionFilter =
      Option(partition).map { part => s"WHERE ${Constants.PartitionColumn} = '$part'" }.getOrElse("")
    tableUtils.sql(s"SELECT * FROM $sourceOfflineTable").show()
    val df = tableUtils.sql(s"""SELECT key_bytes, value_bytes, unix_timestamp(ds, 'yyyy-MM-dd') * 1000 as ts 
         |FROM $sourceOfflineTable
         |$partitionFilter""".stripMargin)
    val requests = df.rdd
      .collect()
      .map { row: Row =>
        val key = row.get(0).asInstanceOf[Array[Byte]]
        val value = row.get(1).asInstanceOf[Array[Byte]]
        val timestamp = row.get(2).asInstanceOf[Long]
        KVStore.PutRequest(key, value, destinationOnlineDataSet, Option(timestamp))
      }

    create(destinationOnlineDataSet)
    multiPut(requests)
  }

  override def create(dataset: String): Unit = {
    database.put(dataset, new ConcurrentHashMap[Key, mutable.Buffer[(Version, Data)]])
  }
}
