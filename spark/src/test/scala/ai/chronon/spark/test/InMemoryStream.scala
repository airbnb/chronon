package ai.chronon.spark.test

import ai.chronon.api.Constants
import ai.chronon.api.GroupBy
import ai.chronon.api.StructType
import ai.chronon.online.{AvroConversions, Mutation, SparkConversions, TileCodec}
import ai.chronon.online.Extensions.StructTypeOps
import ai.chronon.spark.TableUtils
import com.esotericsoftware.kryo.Kryo
import org.apache.avro.generic.{GenericData, GenericRecord}
import org.apache.avro.io.{BinaryEncoder, EncoderFactory}
import org.apache.avro.specific.SpecificDatumWriter
import org.apache.commons.io.output.ByteArrayOutputStream
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.execution.streaming.sources.ContinuousMemoryStream
import org.apache.spark.sql.{DataFrame, Dataset, Encoder, Encoders, Row, SparkSession, types}
import scala.collection.JavaConverters._

import java.util.Base64

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
    out.toByteArray
  }

  // encode input as avro byte array and insert into memory stream.
  def getInMemoryStreamDF(spark: SparkSession, inputDf: Dataset[Row]): DataFrame = {
    val schema: StructType = StructType.from("input", SparkConversions.toChrononSchema(inputDf.schema))
    println(s"Creating in-memory stream with schema: ${SparkConversions.fromChrononSchema(schema).catalogString}")
    val avroSchema = AvroConversions.fromChrononSchema(schema)
    import spark.implicits._
    val input: MemoryStream[Array[Byte]] =
      new MemoryStream[Array[Byte]](inputDf.schema.catalogString.hashCode % 1000, spark.sqlContext)
    input.addData(inputDf.collect.map { row: Row =>
      val bytes = encode(avroSchema)(row)
      bytes
    })
    input.toDF
  }

  def getContinuousStreamDF(spark: SparkSession, baseInput: Dataset[Row]): DataFrame = {
    // align schema
    val noDs = baseInput.drop(TableUtils(spark).partitionColumn)
    val mutationColumns = Constants.MutationFields.map(_.name)
    val fields = noDs.schema.fieldNames
    val baseFields = fields.filterNot(mutationColumns.contains)
    val mutationFields = mutationColumns.filter(fields.contains)
    val inputDf = noDs.selectExpr(baseFields ++ mutationFields: _*)

    // encode and write
    println(s"encoding stream with schema: ${inputDf.schema.catalogString}")
    inputDf.show()
    val schema: StructType = StructType.from("input", SparkConversions.toChrononSchema(inputDf.schema))
    val avroSchema = AvroConversions.fromChrononSchema(schema)

    import spark.implicits._
    val input: MemoryStream[Array[Byte]] =
      new MemoryStream[Array[Byte]](inputDf.schema.catalogString.hashCode % 1000, spark.sqlContext)
    input.addData(inputDf.collect.map { row: Row =>
      val bytes = encode(avroSchema)(row)
      bytes
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
  def getInMemoryTiledStreamArray(spark: SparkSession, inputDf: Dataset[Row], groupBy: GroupBy): Array[(Array[Any], Long, Array[Byte])] = {
    val chrononSchema: StructType = StructType.from("input", SparkConversions.toChrononSchema(inputDf.schema))

    val entityIndex = 0
    val tsIndex = 1

    // Split inputDf into tiles to allow for pre-aggregations to be tested.
    // Test setup assumes the first column is the entity id, second column is timestamp,
    // and that, for test simplicity, events are grouped based on their exact timestamp into tiles.
    val entityTimestampGroupedRows = inputDf.collect().groupBy(row => {
      (row.get(entityIndex), row.get(tsIndex))
    })
    entityTimestampGroupedRows.toArray.map { keyedRow =>
      val rowsKeys = Array(keyedRow._1._1) // first entry of grouping tuple is entity id
      val tileTimestamp: Long = keyedRow._1._2.asInstanceOf[Long] // second entry of grouping tuple is tile timestamp
      val rows = keyedRow._2

      val tileCodec = new TileCodec(groupBy, chrononSchema.iterator.map { field => (field.name, field.fieldType) }.toSeq)
      val aggIr = tileCodec.rowAggregator.init

      rows.map { row =>
        val chrononRow = SparkConversions.toChrononRow(row, tsIndex).asInstanceOf[ai.chronon.api.Row]
        tileCodec.rowAggregator.update(aggIr, chrononRow)
      }

      val preAgg: Array[Byte] = tileCodec.makeTileIr(aggIr, true)

      (rowsKeys, tileTimestamp, preAgg)
    }
  }
}
