package ai.chronon.spark.test

import ai.chronon.api.{Constants, StructType}
import ai.chronon.online._
import ai.chronon.spark.Extensions._
import ai.chronon.spark.TableUtils
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.avro.io.{BinaryDecoder, DecoderFactory}
import org.apache.avro.specific.SpecificDatumReader
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.io.{ByteArrayInputStream, InputStream}
import java.util.concurrent.ConcurrentLinkedQueue
import scala.collection.JavaConverters.asScalaIteratorConverter

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

class MockApi(kvStore: () => KVStore, namespace: String) extends Api(null) {
  val loggedResponseList: ConcurrentLinkedQueue[LoggableResponse] = new ConcurrentLinkedQueue[LoggableResponse]

  var streamSchema: StructType = null

  override def streamDecoder(parsedInfo: GroupByServingInfoParsed): StreamDecoder = {
    assert(streamSchema != null, s"Stream Schema is necessary for stream decoder")
    new MockDecoder(parsedInfo.streamChrononSchema, streamSchema)
  }

  override def genKvStore: KVStore = {
    kvStore()
  }

  override def logResponse(loggableResponse: LoggableResponse): Unit =
    loggedResponseList.add(loggableResponse)

  override def logTable: String = s"$namespace.mock_log_table"

  def flushLoggedValues: Seq[LoggableResponse] = {
    val loggedValues = loggedResponseList.iterator().asScala.toSeq
    loggedResponseList.clear()
    loggedValues
  }

  def loggedValuesToDf(loggedValues: Seq[LoggableResponse], session: SparkSession): DataFrame = {
    val df = session.sqlContext.createDataFrame(session.sparkContext.parallelize(loggedValues))
    df.withTimeBasedColumn("ds", "tsMillis").camelToSnake
  }

  def loggedResponses(session: SparkSession, writeToHive: Boolean = false): DataFrame = {
    val loggedValues = flushLoggedValues

    val df = session.sqlContext.createDataFrame(session.sparkContext.parallelize(loggedValues))
    val dsDf = df.withTimeBasedColumn("ds", "tsMillis").camelToSnake

    if (writeToHive) {
      TableUtils(session).insertPartitions(dsDf, logTable, partitionColumns = Seq("join_name", "ds"))
      TableUtils(session).sql(s"select * from $logTable")
    } else {
      dsDf
    }
  }
}
