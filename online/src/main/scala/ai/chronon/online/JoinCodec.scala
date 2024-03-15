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

package ai.chronon.online

import ai.chronon.api.Extensions.{DerivationOps, JoinOps, MetadataOps}
import ai.chronon.api.{DataType, HashUtils, StructField, StructType}
import com.google.gson.Gson
import scala.collection.Seq
import scala.util.ScalaJavaConversions.JMapOps

import ai.chronon.online.OnlineDerivationUtil.{
  DerivationFunc,
  buildDerivationFunction,
  buildDerivedFields,
  buildRenameOnlyDerivationFunction,
  timeFields
}

case class JoinCodec(conf: JoinOps,
                     keySchema: StructType,
                     baseValueSchema: StructType,
                     keyCodec: AvroCodec,
                     baseValueCodec: AvroCodec)
    extends Serializable {

  @transient lazy val valueSchema: StructType = {
    val fields = if (conf.join == null || conf.join.derivations == null || baseValueSchema.fields.isEmpty) {
      baseValueSchema
    } else {
      buildDerivedFields(conf.derivationsScala, keySchema, baseValueSchema)
    }
    val derivedSchema: StructType = StructType(s"join_derived_${conf.join.metaData.cleanName}", fields.toArray)
    if (conf.logFullValues) {
      def toMap(schema: StructType): Map[String, DataType] = schema.map(field => (field.name, field.fieldType)).toMap
      val (baseMap, derivedMap) = (toMap(baseValueSchema), toMap(derivedSchema))
      StructType(
        s"join_combined_${conf.join.metaData.cleanName}",
        // derived values take precedence in case of collision
        (baseMap ++ derivedMap).map {
          case (name, dataTye) => StructField(name, dataTye)
        }.toArray
      )
    } else {
      derivedSchema
    }
  }

  @transient lazy val deriveFunc: DerivationFunc =
    buildDerivationFunction(conf.derivationsScala, keySchema, baseValueSchema)

  @transient lazy val renameOnlyDeriveFunc: (Map[String, Any], Map[String, Any]) => Map[String, Any] =
    buildRenameOnlyDerivationFunction(conf.derivationsScala)

  @transient lazy val valueCodec: AvroCodec = AvroCodec.of(AvroConversions.fromChrononSchema(valueSchema).toString)

  /*
   * Get the serialized string repr. of the logging schema.
   * key_schema and value_schema are first converted to strings and then serialized as part of Map[String, String] => String conversion.
   *
   * Example:
   * {"join_name":"unit_test/test_join","key_schema":"{\"type\":\"record\",\"name\":\"unit_test_test_join_key\",\"namespace\":\"ai.chronon.data\",\"doc\":\"\",\"fields\":[{\"name\":\"listing\",\"type\":[\"null\",\"long\"],\"doc\":\"\"}]}","value_schema":"{\"type\":\"record\",\"name\":\"unit_test_test_join_value\",\"namespace\":\"ai.chronon.data\",\"doc\":\"\",\"fields\":[{\"name\":\"unit_test_listing_views_v1_m_guests_sum\",\"type\":[\"null\",\"long\"],\"doc\":\"\"},{\"name\":\"unit_test_listing_views_v1_m_views_sum\",\"type\":[\"null\",\"long\"],\"doc\":\"\"}]}"}
   */
  lazy val loggingSchema: String = JoinCodec.buildLoggingSchema(conf.join.metaData.name, keyCodec, valueCodec)
  lazy val loggingSchemaHash: String = HashUtils.md5Base64(loggingSchema)

  val keys: Array[String] = keySchema.fields.iterator.map(_.name).toArray
  val values: Array[String] = valueSchema.fields.iterator.map(_.name).toArray

  val keyFields: Array[StructField] = keySchema.fields
  val valueFields: Array[StructField] = valueSchema.fields
  lazy val keyIndices: Map[StructField, Int] = keySchema.zipWithIndex.toMap
  lazy val valueIndices: Map[StructField, Int] = valueSchema.zipWithIndex.toMap
}

object JoinCodec {

  def buildLoggingSchema(joinName: String, keyCodec: AvroCodec, valueCodec: AvroCodec): String = {
    val schemaMap = Map(
      "join_name" -> joinName,
      "key_schema" -> keyCodec.schemaStr,
      "value_schema" -> valueCodec.schemaStr
    )
    new Gson().toJson(schemaMap.toJava)
  }
}
