package ai.zipline.spark.test

import ai.zipline.api.{Mutation, StructType}
import ai.zipline.fetcher.AvroUtils
import ai.zipline.spark.Conversions
import org.apache.avro.generic.{GenericData, GenericRecord}
import org.apache.avro.io.{BinaryEncoder, EncoderFactory}
import org.apache.avro.specific.SpecificDatumWriter
import org.apache.commons.io.output.ByteArrayOutputStream
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.execution.streaming.MemoryStream


class InMemoryStream {

  private def encode(schema: org.apache.avro.Schema)(row: Row): Array[Byte] = {
  val gr: GenericRecord = new GenericData.Record(schema)
  row.schema.fieldNames.foreach(name => gr.put(name, row.getAs(name)))

  val writer = new SpecificDatumWriter[GenericRecord](schema)
  val out = new ByteArrayOutputStream()
  val encoder: BinaryEncoder = EncoderFactory.get().binaryEncoder(out, null)
  writer.write(gr, encoder)
  encoder.flush()
  out.close()

  out.toByteArray()
}

  // encode input as avro byte array and insert into memory stream.
  def getInMemoryStreamDF(spark: SparkSession, inputDf: Dataset[Row], schema: StructType): DataFrame = {
    val avroSchema = AvroUtils.fromZiplineSchema(schema)
    import spark.implicits._
    val input: MemoryStream[Array[Byte]] = new MemoryStream[Array[Byte]](42, spark.sqlContext)
    input.addData(
      inputDf.collect.map {
        row: Row =>
          encode(avroSchema)(row)
      })
    input.toDF
  }
}
