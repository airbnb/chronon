package ai.chronon.spark.test

import ai.chronon.api.{Constants, StructType}
import ai.chronon.online.Fetcher.Response
import ai.chronon.online._
import ai.chronon.spark.Extensions._
import ai.chronon.spark.TableUtils
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.avro.io.{BinaryDecoder, DecoderFactory}
import org.apache.avro.specific.SpecificDatumReader
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.io.{ByteArrayInputStream, InputStream}
import java.util
import java.util.Base64
import java.util.concurrent.{CompletableFuture, ConcurrentLinkedQueue}
import scala.collection.Seq
import scala.concurrent.Future
import scala.util.ScalaJavaConversions.{IteratorOps, JListOps, JMapOps}
import scala.util.Success

class MockDecoder(inputSchema: StructType, streamSchema: StructType) extends StreamDecoder {

  private def byteArrayToAvro(avro: Array[Byte], schema: Schema): GenericRecord = {
    val reader = new SpecificDatumReader[GenericRecord](schema)
    val input: InputStream = new ByteArrayInputStream(avro)
    val decoder: BinaryDecoder = DecoderFactory.get().binaryDecoder(input, null)
    reader.read(null, decoder)
  }

  override def decode(bytes: Array[Byte]): Mutation = {
    val avroSchema = AvroConversions.fromChrononSchema(streamSchema)
    val avroRecord = byteArrayToAvro(bytes, avroSchema)

    val row: Array[Any] = schema.fields.map { f =>
      AvroConversions.toChrononRow(avroRecord.get(f.name), f.fieldType).asInstanceOf[AnyRef]
    }
    val reversalIndex = schema.indexWhere(_.name == Constants.ReversalColumn)
    if (reversalIndex >= 0 && row(reversalIndex).asInstanceOf[Boolean]) {
      Mutation(schema, row, null)
    } else {
      Mutation(schema, null, row)
    }
  }

  override def schema: StructType = inputSchema
}

class MockStreamBuilder extends StreamBuilder {
  override def from(topicInfo: TopicInfo)(implicit session: SparkSession, props: Map[String, String]): DataStream = {
    val tableUtils = TableUtils(session)
    val ds = topicInfo.params("ds")
    val df = tableUtils.sql(s"select * from ${topicInfo.name} where ds >= '$ds'")
    // table name should be same as topic name
    DataStream(df, 1, topicInfo)
  }
}

class MockApi(kvStore: () => KVStore, val namespace: String) extends Api(null) {
  class PlusOneExternalHandler extends ExternalSourceHandler {
    override def fetch(requests: collection.Seq[Fetcher.Request]): Future[collection.Seq[Fetcher.Response]] = {
      Future(
        requests.map(req =>
          Response(req,
                   Success(req.keys.mapValues(_.asInstanceOf[Integer] + 1).mapValues(_.asInstanceOf[AnyRef]).toMap))))
    }
  }

  class AlwaysFailsHandler extends JavaExternalSourceHandler {
    override def fetchJava(requests: util.List[JavaRequest]): CompletableFuture[util.List[JavaResponse]] = {
      CompletableFuture.completedFuture[util.List[JavaResponse]](
        requests
          .iterator()
          .toScala
          .map(req =>
            new JavaResponse(
              req,
              JTry.failure(
                new RuntimeException("This handler always fails things")
              )
            ))
          .toList
          .toJava
      )
    }
  }

  class JavaPlusOneExternalHandler extends JavaExternalSourceHandler {
    override def fetchJava(requests: util.List[JavaRequest]): CompletableFuture[util.List[JavaResponse]] = {
      CompletableFuture.completedFuture(
        requests
          .iterator()
          .toScala
          .map { req =>
            new JavaResponse(req,
                             JTry.success(
                               req.keys
                                 .entrySet()
                                 .iterator()
                                 .toScala
                                 .map(e => e.getKey -> (e.getValue.asInstanceOf[Integer] + 1).asInstanceOf[AnyRef])
                                 .toMap
                                 .toJava
                             ))
          }
          .toSeq
          .toJava)
    }
  }

  val loggedResponseList: ConcurrentLinkedQueue[LoggableResponseBase64] =
    new ConcurrentLinkedQueue[LoggableResponseBase64]

  var streamSchema: StructType = null

  override def streamDecoder(parsedInfo: GroupByServingInfoParsed): StreamDecoder = {
    assert(streamSchema != null, s"Stream Schema is necessary for stream decoder")
    new MockDecoder(parsedInfo.streamChrononSchema, streamSchema)
  }

  override def genKvStore: KVStore = {
    kvStore()
  }

  override def logResponse(loggableResponse: LoggableResponse): Unit =
    loggedResponseList.add(
      LoggableResponseBase64(
        keyBase64 = Base64.getEncoder.encodeToString(loggableResponse.keyBytes),
        valueBase64 = Base64.getEncoder.encodeToString(loggableResponse.valueBytes),
        name = loggableResponse.joinName,
        tsMillis = loggableResponse.tsMillis,
        schemaHash = loggableResponse.schemaHash
      ))

  val logTable: String = s"$namespace.mock_log_table"
  val schemaTable: String = s"$namespace.mock_schema_table"

  def flushLoggedValues: Seq[LoggableResponseBase64] = {
    val loggedValues = loggedResponseList.iterator().toScala.toArray
    loggedResponseList.clear()
    loggedValues
  }

  def loggedValuesToDf(loggedValues: Seq[LoggableResponseBase64], session: SparkSession): DataFrame = {
    val df = session.sqlContext.createDataFrame(session.sparkContext.parallelize(loggedValues.toSeq))
    df.withTimeBasedColumn("ds", "tsMillis").camelToSnake
  }

  override def externalRegistry: ExternalSourceRegistry = {
    val registry = new ExternalSourceRegistry
    registry.add("plus_one", new PlusOneExternalHandler)
    registry.add("always_fails", new AlwaysFailsHandler)
    registry.add("java_plus_one", new JavaPlusOneExternalHandler)
    registry
  }
  override def generateStreamBuilder(streamType: String): StreamBuilder = {
    new MockStreamBuilder()
  }
}
