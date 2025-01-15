/*
 *    Copyright (C) 2023 The Chronon Authors.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package ai.chronon.spark.test

import org.slf4j.LoggerFactory
import ai.chronon.api.Constants
import ai.chronon.online.KVStore
import ai.chronon.online.KVStore.{PutRequest, TimedValue}
import ai.chronon.spark.TableUtils
import org.apache.spark.sql.Row

import java.util.concurrent.ConcurrentHashMap
import java.util.{Base64, function}
import scala.collection.mutable
import scala.concurrent.Future
import scala.util.Try

class InMemoryKvStore(tableUtils: () => TableUtils) extends KVStore with Serializable {
  //type aliases for readability
  type Key = String
  type Data = Array[Byte]
  type DataSet = String
  type Version = Long
  type VersionedData = mutable.Buffer[(Version, Data)]
  type Table = ConcurrentHashMap[Key, VersionedData]
  protected[spark] val database = new ConcurrentHashMap[DataSet, Table]

  @transient lazy val encoder = Base64.getEncoder
  def encode(bytes: Array[Byte]): String = encoder.encodeToString(bytes)
  def toStr(bytes: Array[Byte]): String = new String(bytes, Constants.UTF8)
  override def multiGet(requests: collection.Seq[KVStore.GetRequest]): Future[collection.Seq[KVStore.GetResponse]] = {
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

  override def multiPut(putRequests: collection.Seq[KVStore.PutRequest]): Future[collection.Seq[Boolean]] = {
    val result = putRequests.map {
      case PutRequest(keyBytes, valueBytes, dataset, millis) =>
        val table = database.get(dataset)
        val key = encode(keyBytes)
        table.compute(key, putFunc(millis.getOrElse(System.currentTimeMillis()) -> valueBytes))
        true
    }

    Future {
      result
    }
  }

  // For the case of group by batch uploads.
  // the table is assumed to be encoded with two columns - `key` and `value` as Array[Bytes]
  // one of the keys should be "group_by_serving_info" as bytes with value as TSimpleJsonEncoded String
  override def bulkPut(sourceOfflineTable: String, destinationOnlineDataSet: String, partition: String): Unit = {
    val tableUtilInst = tableUtils()
    val partitionFilter =
      Option(partition).map { part => s"WHERE ${tableUtilInst.partitionColumn} = '$part'" }.getOrElse("")
    val offlineDf = tableUtilInst.sql(s"SELECT * FROM $sourceOfflineTable")
    val tsColumn =
      if (offlineDf.columns.contains(Constants.TimeColumn)) Constants.TimeColumn
      else s"(unix_timestamp(ds, 'yyyy-MM-dd') * 1000 + ${tableUtilInst.partitionSpec.spanMillis})"
    val df =
      tableUtilInst.sql(s"""SELECT key_bytes, value_bytes, $tsColumn as ts
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

  def show(): Unit = {
    val it = database.entrySet().iterator()
    while (it.hasNext) {
      val entry = it.next()
      val tableName = entry.getKey
      val table = entry.getValue
      val innerIt = table.entrySet().iterator()
      while (innerIt.hasNext) {
        val tableEntry = innerIt.next()
        val key = tableEntry.getKey
        val value = tableEntry.getValue
        value.foreach {
          case (version, data) =>
            logger.info(s"table: $tableName, key: $key, value: $data, version: $version")
        }
      }
    }
  }
}

object InMemoryKvStore {
  @transient lazy val logger = LoggerFactory.getLogger(getClass)
  val stores: ConcurrentHashMap[String, InMemoryKvStore] = new ConcurrentHashMap[String, InMemoryKvStore]

  // We would like to create one instance of InMemoryKVStore per executors, but share SparkContext
  // across them. Since SparkContext is not serializable,  we wrap TableUtils that has SparkContext
  // in a closure and pass it around.
  def build(testName: String, tableUtils: () => TableUtils): InMemoryKvStore = {
    stores.computeIfAbsent(
      testName,
      new function.Function[String, InMemoryKvStore] {
        override def apply(name: String): InMemoryKvStore = {
          logger.info(s"Missing in-memory store for name: $name. Creating one")
          new InMemoryKvStore(tableUtils)
        }
      }
    )
  }
}
