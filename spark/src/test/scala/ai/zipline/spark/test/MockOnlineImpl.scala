package ai.zipline.spark.test

import java.io.{ByteArrayInputStream, InputStream}
import ai.zipline.api.{Constants, Decoder, KVStore, Mutation, OnlineImpl, StructType}
import ai.zipline.fetcher.{ArrayRow, AvroUtils, RowConversions}
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.avro.io.{BinaryDecoder, DecoderFactory}
import org.apache.avro.specific.SpecificDatumReader

class MockDecoder(schema: StructType) extends Decoder {

  private def byteArrayToAvro(avro: Array[Byte], schema: Schema): GenericRecord = {
    val reader = new SpecificDatumReader[GenericRecord](schema)
    val input: InputStream = new ByteArrayInputStream(avro)
    val decoder: BinaryDecoder = DecoderFactory.get().binaryDecoder(input, null)
    reader.read(null, decoder)
  }

  val tsIndex = schema.indexWhere(_.name == Constants.TimeColumn)

  def decode(bytes: Array[Byte]): Mutation = {
    val avroSchema = AvroUtils.fromZiplineSchema(schema)
    val avroRecord = byteArrayToAvro(bytes, avroSchema)
    val after: Array[Any] = schema.fields.map(
      f => RowConversions.fromAvroRecord(avroRecord.get(f.name), f.fieldType).asInstanceOf[AnyRef])
    Mutation(schema, null, new ArrayRow(after, after(tsIndex).asInstanceOf[Long]))
  }
}

class MockOnlineImpl(kvStore: () => KVStore, userConf: Map[String, String]) extends OnlineImpl(userConf = userConf) {

  override def genStreamDecoder(inputSchema: StructType): Decoder = {
    new MockDecoder(inputSchema)
  }

  // users can set transform input avro schema from batch source to streaming compatible schema
  override def batchInputAvroSchemaToStreaming(batchSchema: StructType): StructType = batchSchema

  override def genKvStore: KVStore = {
    kvStore()
  }
}

