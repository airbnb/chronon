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
import scala.collection.JavaConverters._

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

  /**
   *
   * @param spark SparkSession
   * @param inputDf Input dataframe of raw event rows
   * @param groupBy GroupBy
   * @return Array[(Array[Any], Array[Byte]) where Array[Any] is the list of keys and Array[Byte]
   *         a pre-aggregated tile of events. So the overall returned value is an array
   *         of pre-aggregated tiles and their respective keys.
   */
  def getInMemoryTiledStreamArray(spark: SparkSession, inputDf: Dataset[Row], groupBy: GroupBy): Array[(Array[Any], Array[Byte])] = {
    val chrononSchema: StructType = StructType.from("input", SparkConversions.toChrononSchema(inputDf.schema))
    val avroSchema = AvroConversions.fromChrononSchema(chrononSchema)

    // Split inputDf into 4 tiles to allow for tile aggregations to be tested
    val keyCols = groupBy.keyColumns.asScala.toArray
    val keyIndices = keyCols.map(inputDf.schema.fieldIndex)
    val tsIndex = 1

    val groupedRows = inputDf.collect().groupBy(row => {
      row.get(0)
    })
    groupedRows.toArray.flatMap { keyedRow =>
      val rowsKeys = Array(keyedRow._1)
      val rows = keyedRow._2

      val rowsPreAggBytes: Array[Array[Byte]] = rows.grouped((rows.length / 4).floor.toInt).toArray.map { rowSet =>
        val rowAggregator = TileCodec.buildRowAggregator(groupBy, chrononSchema.iterator.map { field => (field.name, field.fieldType) }.toSeq)
        val aggIr = rowAggregator.init

        rowSet.map { row =>
          val chrononRow = SparkConversions.toChrononRow(row, tsIndex).asInstanceOf[ai.chronon.api.Row]
          rowAggregator.update(aggIr, chrononRow)
        }

        val tileCodec = new TileCodec(rowAggregator, groupBy)
        tileCodec.makeTileIr(aggIr, true)
      }

      rowsPreAggBytes.map { rowPreAgg =>
        (rowsKeys, rowPreAgg)
      }
    }
  }
}
