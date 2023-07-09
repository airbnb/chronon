package ai.chronon.spark

import ai.chronon.api.{HashUtils, StructField, StructType}
import ai.chronon.online.{AvroCodec, JoinCodec}
import com.google.gson.Gson

import scala.util.ScalaJavaConversions.MapOps

/*
 * Schema of a published log event. valueCodec includes both base and derived columns.
 */
case class LoggingSchema(keyCodec: AvroCodec, valueCodec: AvroCodec) {
  lazy val keyFields: StructType = keyCodec.chrononSchema.asInstanceOf[StructType]
  lazy val valueFields: StructType = valueCodec.chrononSchema.asInstanceOf[StructType]
  lazy val keyIndices: Map[StructField, Int] = keyFields.zipWithIndex.toMap
  lazy val valueIndices: Map[StructField, Int] = valueFields.zipWithIndex.toMap

  def hash(joinName: String): String = HashUtils.md5Base64(JoinCodec.buildLoggingSchema(joinName, keyCodec, valueCodec))
}

object LoggingSchema {
  def parseLoggingSchema(loggingSchemaStr: String): LoggingSchema = {
    val schemaMap = new Gson()
      .fromJson(
        loggingSchemaStr,
        classOf[java.util.Map[java.lang.String, java.lang.String]]
      )
      .toScala

    val keyCodec = new AvroCodec(schemaMap("key_schema"))
    val valueCodec = new AvroCodec(schemaMap("value_schema"))

    LoggingSchema(keyCodec, valueCodec)
  }
}
