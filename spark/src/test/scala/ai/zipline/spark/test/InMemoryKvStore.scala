package ai.zipline.spark.test

import ai.zipline.api.Constants
import ai.zipline.online.KVStore
import ai.zipline.online.KVStore.{PutRequest, TimedValue}
import ai.zipline.spark.TableUtils
import org.apache.spark.sql.Row
import org.jboss.netty.util.internal.ConcurrentHashMap

import java.util.{Base64, function}
import scala.collection.mutable
import scala.concurrent.Future
import scala.util.Try

class InMemoryKvStore(tableUtils: () => TableUtils) extends KVStore {
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
        val values = Try {
          database
            .get(req.dataset) // table
            .get(encode(req.keyBytes)) // values of key
            .filter {
              case (version, _) => req.afterTsMillis.forall(version >= _)
            } // filter version
            .map { case (version, bytes) => TimedValue(bytes, version) }
        }
        KVStore.GetResponse(req, values)
      }
    }
  }

  private def putFunc(newData: (Version, Data)) =
    new function.BiFunction[Key, VersionedData, VersionedData] {
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
    val tableUtilInst = tableUtils()
    val partitionFilter =
      Option(partition).map { part => s"WHERE ${Constants.PartitionColumn} = '$part'" }.getOrElse("")
    tableUtilInst.sql(s"SELECT * FROM $sourceOfflineTable").show(false)
    val df =
      tableUtilInst.sql(
        s"""SELECT key_bytes, value_bytes, (unix_timestamp(ds, 'yyyy-MM-dd') * 1000 + ${Constants.Partition.spanMillis}) as ts
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

object InMemoryKvStore {
  val stores: ConcurrentHashMap[String, InMemoryKvStore] = new ConcurrentHashMap[String, InMemoryKvStore]

  // We would like to create one instance of InMemoryKVStore per executors, but share SparkContext
  // across them. Since SparkContext is not serializable,  we wrap TableUtils that has SparkContext
  // in a closure and pass it around.
  def build(testName: String, tableUtils: () => TableUtils): InMemoryKvStore = {
    stores.computeIfAbsent(
      testName,
      new function.Function[String, InMemoryKvStore] {
        override def apply(name: String): InMemoryKvStore = {
          println(s"Missing in-memory store for name: $name. Creating one")
          new InMemoryKvStore(tableUtils)
        }
      }
    )
  }
}
