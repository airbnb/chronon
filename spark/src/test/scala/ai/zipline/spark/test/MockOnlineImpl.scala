package ai.zipline.spark.test

import java.io.{ByteArrayInputStream, InputStream}
import ai.zipline.api.{Constants, KVStore, Mutation, OnlineImpl, StreamDecoder, StructType}
import ai.zipline.fetcher.{ArrayRow, AvroUtils, RowConversions}
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.avro.io.{BinaryDecoder, DecoderFactory}
import org.apache.avro.specific.SpecificDatumReader
import ai.zipline.api

class MockDecoder(inputSchema: StructType) extends StreamDecoder {

  private def byteArrayToAvro(avro: Array[Byte], schema: Schema): GenericRecord = {
    val reader = new SpecificDatumReader[GenericRecord](schema)
    val input: InputStream = new ByteArrayInputStream(avro)
    val decoder: BinaryDecoder = DecoderFactory.get().binaryDecoder(input, null)
    reader.read(null, decoder)
  }

  private val tsIndex = schema.indexWhere(_.name == Constants.TimeColumn)

  override def decode(bytes: Array[Byte]): Mutation = {
    val avroSchema = AvroUtils.fromZiplineSchema(schema)
    val avroRecord = byteArrayToAvro(bytes, avroSchema)
    val after: Array[Any] = schema.fields.map { f =>
      RowConversions.fromAvroRecord(avroRecord.get(f.name), f.fieldType).asInstanceOf[AnyRef]
    }
    Mutation(schema, null, new ArrayRow(after, after(tsIndex).asInstanceOf[Long]))
  }

  override def schema: StructType = inputSchema
}

class MockOnlineImpl(kvStore: () => KVStore, userConf: Map[String, String]) extends OnlineImpl(userConf = userConf) {

  override def streamDecoder(groupBy: api.GroupBy, batchInputSchema: api.StructType): StreamDecoder = {
    new MockDecoder(batchInputSchema)
  }

  override def genKvStore: KVStore = {
    kvStore()
  }
}
