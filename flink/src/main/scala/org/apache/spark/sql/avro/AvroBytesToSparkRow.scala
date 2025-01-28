package org.apache.spark.sql.avro

import org.apache.spark.sql.{Encoder, Row}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.types.StructType

/**
 * Thin wrapper to [[AvroDataToCatalyst]] that backs `from_avro` https://spark.apache.org/docs/3.5.1/sql-data-sources-avro.html#to_avro-and-from_avro
 * SparkSQL doesn't have this registered, so instead we use the underlying functionality instead.
 */
object AvroBytesToSparkRow {

  def mapperAndEncoder(avroSchemaJson: String,  options: Map[String, String] = Map()): (Array[Byte] => Row, Encoder[Row]) = {
    val catalyst = AvroDataToCatalyst(null, avroSchemaJson, options)
    val sparkSchema = catalyst.dataType.asInstanceOf[StructType]
    val rowEncoder = RowEncoder(sparkSchema)
    val sparkRowDeser = RowEncoder(sparkSchema).resolveAndBind().createDeserializer()
    val mapper = (bytes: Array[Byte]) =>
      sparkRowDeser(catalyst.nullSafeEval(bytes).asInstanceOf[InternalRow])
    (mapper, rowEncoder)
  }
}
