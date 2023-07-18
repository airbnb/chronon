package ai.chronon.spark.test

import ai.chronon.api.GroupBy
import ai.chronon.api.StructType
import ai.chronon.online.{AvroConversions, SparkConversions, TileCodec}
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
    val schema: StructType = StructType.from("input", SparkConversions.toChrononSchema(inputDf.schema))
    val avroSchema = AvroConversions.fromChrononSchema(schema)
    import spark.implicits._
    val input: MemoryStream[Array[Byte]] = new MemoryStream[Array[Byte]](MemoryStreamID, spark.sqlContext)
    input.addData(inputDf.collect.map { row: Row =>
      encode(avroSchema)(row)
    })
    input.toDF
  }

  def getInMemoryTiledStreamDF(spark: SparkSession, inputDf: Dataset[Row], groupBy: GroupBy): DataFrame = {
    val chrononSchema: StructType = StructType.from("input", SparkConversions.toChrononSchema(inputDf.schema))
    val avroSchema = AvroConversions.fromChrononSchema(chrononSchema)

    import spark.implicits._
    val input: MemoryStream[Array[Byte]] = new MemoryStream[Array[Byte]](MemoryStreamID, spark.sqlContext)

    // Split inputDf into 4 tiles to allow for tile aggregations to be tested
    inputDf.collect().grouped((inputDf.count() / 4).floor.toInt).map { rowSet: Array[Row] =>
      val rowAggregator = TileCodec.buildRowAggregator(groupBy, chrononSchema.iterator.map { field => (field.name, field.fieldType) }.toSeq)
      val aggIr = rowAggregator.init

      rowSet.map { row =>
        val gr: GenericRecord = new GenericData.Record(avroSchema)
        row.schema.fieldNames.foreach(name => gr.put(name, row.getAs(name)))
        val chrononRow = AvroConversions.toChrononRow(gr, chrononSchema).asInstanceOf[ai.chronon.api.Row]
        rowAggregator.update(aggIr, chrononRow)
      }

      val finalAggIr = rowAggregator.finalize(aggIr)
      val tileCodec = new TileCodec(rowAggregator, groupBy)
      val bytesTile = tileCodec.makeTileIr(finalAggIr, true)

      input.addData(bytesTile)
    }

    input.toDF()
  }
}
