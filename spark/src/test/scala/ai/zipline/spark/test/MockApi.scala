package ai.zipline.spark.test

import java.io.{ByteArrayInputStream, InputStream}
import ai.zipline.api.{Constants, GroupByServingInfo, StructType}
import ai.zipline.online.{
  Api,
  ArrayRow,
  AvroUtils,
  GroupByServingInfoParsed,
  KVStore,
  Mutation,
  RowConversions,
  StreamDecoder
}
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

  override def decode(bytes: Array[Byte]): Mutation = {
    val avroSchema = AvroUtils.fromZiplineSchema(schema)
    val avroRecord = byteArrayToAvro(bytes, avroSchema)
    val after: Array[Any] = schema.fields.map { f =>
      RowConversions.fromAvroRecord(avroRecord.get(f.name), f.fieldType).asInstanceOf[AnyRef]
    }
    Mutation(schema, null, after)
  }

  override def schema: StructType = inputSchema
}

class MockApi(kvStore: () => KVStore) extends Api(null) {

  override def streamDecoder(parsedInfo: GroupByServingInfoParsed): StreamDecoder = {
    new MockDecoder(parsedInfo.inputZiplineSchema)
  }

  override def genKvStore: KVStore = {
    kvStore()
  }
}
