package ai.chronon.spark.test

import ai.chronon.api.{Constants, StructType}
import ai.chronon.online.{AvroConversions, Mutation, SparkConversions}
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
}
