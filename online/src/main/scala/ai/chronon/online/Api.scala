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

package ai.chronon.online

import org.slf4j.LoggerFactory
import ai.chronon.api.{Constants, StructType}
import ai.chronon.online.KVStore.{GetRequest, GetResponse, PutRequest}
import org.apache.spark.sql.SparkSession
import java.util.Base64
import java.nio.charset.StandardCharsets

import java.util.function.Consumer
import scala.collection.Seq
import scala.concurrent.duration.{Duration, MILLISECONDS}
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

object KVStore {
  // a scan request essentially for the keyBytes
  // afterTsMillis - is used to limit the scan to more recent data
  case class GetRequest(keyBytes: Array[Byte], dataset: String, afterTsMillis: Option[Long] = None)
  case class TimedValue(bytes: Array[Byte], millis: Long)
  case class GetResponse(request: GetRequest, values: Try[Seq[TimedValue]]) {
    def latest: Try[TimedValue] = values.map(_.maxBy(_.millis))
  }
  case class PutRequest(keyBytes: Array[Byte], valueBytes: Array[Byte], dataset: String, tsMillis: Option[Long] = None)
}

// the main system level api for key value storage
// used for streaming writes, batch bulk uploads & fetching
trait KVStore {
  @transient lazy val logger = LoggerFactory.getLogger(getClass)
  implicit val executionContext: ExecutionContext = FlexibleExecutionContext.buildExecutionContext

  def create(dataset: String): Unit

  def multiGet(requests: Seq[GetRequest]): Future[Seq[GetResponse]]

  def multiPut(keyValueDatasets: Seq[PutRequest]): Future[Seq[Boolean]]

  def bulkPut(sourceOfflineTable: String, destinationOnlineDataSet: String, partition: String): Unit

  def put(putRequest: PutRequest): Future[Boolean] = multiPut(Seq(putRequest)).map(_.head)

  // helper method to blocking read a string - used for fetching metadata & not in hotpath.
  def getString(key: String, dataset: String, timeoutMillis: Long): Try[String] = {
    val response = getResponse(key, dataset, timeoutMillis)
    if (response.values.isFailure) {
      Failure(new RuntimeException(s"Request for key ${key} in dataset ${dataset} failed", response.values.failed.get))
    } else {
      Success(new String(response.latest.get.bytes, Constants.UTF8))
    }
  }

  def getStringArray(key: String, dataset: String, timeoutMillis: Long): Try[Seq[String]] = {
    val response = getResponse(key, dataset, timeoutMillis)
    if (response.values.isFailure) {
      Failure(new RuntimeException(s"Request for key ${key} in dataset ${dataset} failed", response.values.failed.get))
    } else {
      Success(StringArrayConverter.bytesToStrings(response.latest.get.bytes))
    }
  }

  private def getResponse(key: String, dataset: String, timeoutMillis: Long): GetResponse = {
    val fetchRequest = KVStore.GetRequest(key.getBytes(Constants.UTF8), dataset)
    val responseFutureOpt = get(fetchRequest)
    Await.result(responseFutureOpt, Duration(timeoutMillis, MILLISECONDS))
  }
  def get(request: GetRequest): Future[GetResponse] = {
    multiGet(Seq(request))
      .map(_.head)
      .recover {
        case e: java.util.NoSuchElementException =>
          logger.error(
            s"Failed request against ${request.dataset} check the related task to the upload of the dataset (GroupByUpload or MetadataUpload)")
          throw e
      }
  }

  // Method for taking the set of keys and constructing the byte array sent to the KVStore
  def createKeyBytes(keys: Map[String, AnyRef],
                     groupByServingInfo: GroupByServingInfoParsed,
                     dataset: String): Array[Byte] = {
    groupByServingInfo.keyCodec.encode(keys)
  }
}

object StringArrayConverter {
  @transient lazy val logger = LoggerFactory.getLogger(getClass)
  // Method to convert an array of strings to a byte array using Base64 encoding for each element
  def stringsToBytes(strings: Seq[String]): Array[Byte] = {
    val base64EncodedStrings = strings.map(s => Base64.getEncoder.encodeToString(s.getBytes(StandardCharsets.UTF_8)))
    base64EncodedStrings.mkString(",").getBytes(StandardCharsets.UTF_8)
  }

  // Method to convert a byte array back to an array of strings by decoding Base64
  def bytesToStrings(bytes: Array[Byte]): Seq[String] = {
    val encodedString = new String(bytes, StandardCharsets.UTF_8)
    encodedString.split(",").map(s => new String(Base64.getDecoder.decode(s), StandardCharsets.UTF_8))
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
  * Java types corresponding to the schema types. [[StreamDecoder]] should produce mutations that comply.
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
case class Mutation(schema: StructType = null, before: Array[Any] = null, after: Array[Any] = null)

case class LoggableResponse(keyBytes: Array[Byte],
                            valueBytes: Array[Byte],
                            joinName: String,
                            tsMillis: Long,
                            schemaHash: String)

case class LoggableResponseBase64(keyBase64: String,
                                  valueBase64: String,
                                  name: String,
                                  tsMillis: Long,
                                  schemaHash: String)

abstract class StreamDecoder extends Serializable {
  def decode(bytes: Array[Byte]): Mutation
  def schema: StructType
}

trait StreamBuilder {
  def from(topicInfo: TopicInfo)(implicit session: SparkSession, props: Map[String, String]): DataStream
}

object ExternalSourceHandler {
  private[ExternalSourceHandler] val executor = FlexibleExecutionContext.buildExecutionContext
}

// user facing class that needs to be implemented for external sources defined in a join
// Chronon issues the request in parallel to groupBy fetches.
// There is a Java Friendly Handler that extends this and handles conversions
// see: [[ai.chronon.online.JavaExternalSourceHandler]]
abstract class ExternalSourceHandler extends Serializable {
  implicit lazy val executionContext: ExecutionContext = ExternalSourceHandler.executor
  def fetch(requests: Seq[Fetcher.Request]): Future[Seq[Fetcher.Response]]
}

// the implementer of this class should take a single argument, a scala map of string to string
// chronon framework will construct this object with user conf supplied via CLI
abstract class Api(userConf: Map[String, String]) extends Serializable {
  lazy val fetcher: Fetcher = {
    if (fetcherObj == null)
      fetcherObj = buildFetcher()
    fetcherObj
  }
  private var fetcherObj: Fetcher = null

  def streamDecoder(groupByServingInfoParsed: GroupByServingInfoParsed): StreamDecoder

  def genKvStore: KVStore

  def externalRegistry: ExternalSourceRegistry

  private var timeoutMillis: Long = 10000

  private var flagStore: FlagStore = null

  def setFlagStore(customFlagStore: FlagStore): Unit = { flagStore = customFlagStore }

  def setTimeout(millis: Long): Unit = { timeoutMillis = millis }

  // kafka has built-in support - but one can add support to other types using this method.
  def generateStreamBuilder(streamType: String): StreamBuilder = null

  /** logged responses should be made available to an offline log table in Hive
    *  with columns
    *     key_bytes, value_bytes, ts_millis, join_name, schema_hash and ds (date string)
    *  partitioned by `join_name` and `ds`
    *  Note the camel case to snake case conversion: Hive doesn't like camel case.
    *  The key bytes and value bytes will be transformed by chronon to human readable columns for each join.
    *    <team_namespace>.<join_name>_logged
    *  To measure consistency - a Side-by-Side comparison table will be created at
    *    <team_namespace>.<join_name>_comparison
    *  Consistency summary will be available in
    *    <logTable>_consistency_summary
    */
  def logResponse(resp: LoggableResponse): Unit

  // helper functions
  final def buildFetcher(debug: Boolean = false,
                         callerName: String = null,
                         disableErrorThrows: Boolean = false): Fetcher =
    new Fetcher(genKvStore,
                Constants.ChrononMetadataKey,
                logFunc = responseConsumer,
                debug = debug,
                externalSourceRegistry = externalRegistry,
                timeoutMillis = timeoutMillis,
                callerName = callerName,
                flagStore = flagStore,
                disableErrorThrows = disableErrorThrows)

  final def buildJavaFetcher(callerName: String = null, disableErrorThrows: Boolean = false): JavaFetcher = {
    new JavaFetcher(genKvStore,
                    Constants.ChrononMetadataKey,
                    timeoutMillis,
                    responseConsumer,
                    externalRegistry,
                    callerName,
                    flagStore,
                    disableErrorThrows)
  }

  final def buildJavaFetcher(): JavaFetcher = buildJavaFetcher(null)

  private def responseConsumer: Consumer[LoggableResponse] =
    new Consumer[LoggableResponse] {
      override def accept(t: LoggableResponse): Unit = logResponse(t)
    }
}
