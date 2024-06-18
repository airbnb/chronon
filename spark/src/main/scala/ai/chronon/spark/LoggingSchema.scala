/*
 *    Copyright (C) 2023 The Chronon Authors.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

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
