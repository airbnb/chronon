package ai.chronon.spark.test

import ai.chronon.api.StructType
import ai.chronon.online.AvroConversions
import ai.chronon.spark.Conversions
import org.apache.avro.generic.{GenericData, GenericRecord}
import org.apache.avro.io.{BinaryEncoder, EncoderFactory}
import org.apache.avro.specific.SpecificDatumWriter
import org.apache.commons.io.output.ByteArrayOutputStream
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

class InMemoryStream {

  private val MemoryStreamID = 42

  private def encode(schema: org.apache.avro.Schema)(row: Row): Array[Byte] = {
    val gr: GenericRecord = new GenericData.Record(schema)
    row.schema.fieldNames.foreach(name => gr.put(name, row.getAs(name)))

    val writer = new SpecificDatumWriter[GenericRecord](schema)
    val out = new ByteArrayOutputStream()
    val encoder: BinaryEncoder = EncoderFactory.get().binaryEncoder(out, null)
    writer.write(gr, encoder)
    encoder.flush()
    out.close()
    out.toByteArray
  }

  // encode input as avro byte array and insert into memory stream.
  def getInMemoryStreamDF(spark: SparkSession, inputDf: Dataset[Row]): DataFrame = {
    val schema: StructType = StructType.from("input", Conversions.toChrononSchema(inputDf.schema))
    val avroSchema = AvroConversions.fromChrononSchema(schema)
    import spark.implicits._
    val input: MemoryStream[Array[Byte]] = new MemoryStream[Array[Byte]](MemoryStreamID, spark.sqlContext)
    input.addData(inputDf.collect.map { row: Row =>
      encode(avroSchema)(row)
    })
    input.toDF
  }
}
