package ai.chronon.flink.test

import org.apache.avro.Schema

import java.io.ByteArrayOutputStream
import org.apache.avro.generic.{GenericData, GenericDatumWriter, GenericRecord}
import org.apache.avro.io.{DatumWriter, EncoderFactory}

object AvroFlinkSourceTestUtils {

  val e2EAvroSchema = """
{
  "type": "record",
  "name": "E2ETestEvent",
  "fields": [
    { "name": "id",         "type": "string" },
    { "name": "int_val",    "type": "int" },
    { "name": "double_val", "type": "double" },
    { "name": "created",    "type": "long" }
  ]
}
""".stripMargin
  private val e2ETestEventSchema: Schema = new Schema.Parser().parse(e2EAvroSchema)

  def toAvroBytes(event: E2ETestEvent): Array[Byte] = {
    // Create a record with the parsed schema
    val record = new GenericData.Record(e2ETestEventSchema)

    // Populate the record fields from the case class
    record.put("id", event.id)
    record.put("int_val", event.int_val)
    record.put("double_val", event.double_val)
    record.put("created", event.created)

    avroBytesFromGenericRecord(record)
  }
  private def avroBytesFromGenericRecord(record: GenericRecord): Array[Byte] = {
    // Create a ByteArrayOutputStream to hold the serialized data
    val byteStream = new ByteArrayOutputStream()

    // Create a binary encoder that writes to the byteStream
    val encoder = EncoderFactory.get.binaryEncoder(byteStream, null)

    // Create a datum writer for the record's schema
    val writer: DatumWriter[GenericRecord] = new GenericDatumWriter[GenericRecord](record.getSchema)

    // Write the record data to the encoder
    writer.write(record, encoder)
    encoder.flush()

    // Return the serialized Avro bytes
    byteStream.toByteArray
  }
}
