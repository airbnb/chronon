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

import ai.chronon.api.Extensions.{JoinOps, MetadataOps}
import ai.chronon.api.{DataType, HashUtils, StructField, StructType}
import ai.chronon.online.Fetcher.ResponseWithContext
import com.google.gson.Gson

import scala.collection.Seq
import scala.util.ScalaJavaConversions.JMapOps
import ai.chronon.online.DerivationUtils.{
  DerivationFunc,
  buildDerivationFunction,
  buildDerivedFields,
  buildRenameOnlyDerivationFunction
}

case class JoinCodec(conf: JoinOps, keySchema: StructType, baseValueSchema: StructType) extends Serializable {

  lazy val derivedSchema: StructType = {
    val fields = if (conf.join == null || conf.join.derivations == null || baseValueSchema.fields.isEmpty) {
      baseValueSchema
    } else {
      buildDerivedFields(conf.derivationsScala, keySchema, baseValueSchema)
    }
    val derivedSchema = StructType(s"join_derived_${conf.join.metaData.cleanName}", fields.toArray)
    derivedSchema
  }

  // Merge schemas. If there are duplicate fields, the later ones take precedence.
  private def mergeSchema(name: String, schemas: StructType*): StructType = {
    def toMap(schema: StructType): Map[String, DataType] = schema.map(field => (field.name, field.fieldType)).toMap
    val fields = schemas
      .map(toMap)
      .fold(Map.empty)(_ ++ _)
      .map {
        case (name, dataType) => StructField(name, dataType)
      }
      .toArray
    StructType(name, fields)
  }

  @transient lazy val fullDerivedSchema: StructType = {
    val name = s"join_full_derived_${conf.join.metaData.cleanName}"
    mergeSchema(name, baseValueSchema, derivedSchema)
  }

  @transient lazy val valueSchema: StructType = {
    val name = s"join_combined_${conf.join.metaData.cleanName}"
    if (!conf.hasModelTransforms) {
      if (conf.logFullValues) {
        mergeSchema(name, baseValueSchema, derivedSchema)
      } else {
        derivedSchema
      }
    } else {
      if (conf.logFullValues) {
        mergeSchema(name, baseValueSchema, derivedSchema, conf.modelSchema)
      } else {
        conf.modelSchema
      }
    }
  }

  def buildLoggingValues(resp: ResponseWithContext): Map[String, AnyRef] = {
    if (!conf.hasModelTransforms) {
      if (conf.join.logFullValues) {
        resp.baseValues ++ resp.derivedValues.getOrElse(Map.empty)
      } else {
        resp.derivedValues.getOrElse(Map.empty)
      }
    } else {
      if (conf.join.logFullValues) {
        resp.baseValues ++ resp.derivedValues.getOrElse(Map.empty) ++ resp.modelTransformsValues.getOrElse(Map.empty)
      } else {
        resp.modelTransformsValues.getOrElse(Map.empty)
      }
    }
  }

  @transient lazy val deriveFunc: DerivationFunc =
    buildDerivationFunction(conf.derivationsScala, keySchema, baseValueSchema)

  @transient lazy val renameOnlyDeriveFunc: (Map[String, Any], Map[String, Any]) => Map[String, Any] =
    buildRenameOnlyDerivationFunction(conf.derivationsScala)

  @transient lazy val keySchemaStr: String = AvroConversions.fromChrononSchema(keySchema).toString
  @transient lazy val valueSchemaStr: String = AvroConversions.fromChrononSchema(valueSchema).toString
  def keyCodec: AvroCodec = AvroCodec.of(keySchemaStr)
  def valueCodec: AvroCodec = AvroCodec.of(valueSchemaStr)

  /*
   * Get the serialized string repr. of the logging schema.
   * key_schema and value_schema are first converted to strings and then serialized as part of Map[String, String] => String conversion.
   *
   * Example:
   * {"join_name":"unit_test/test_join","key_schema":"{\"type\":\"record\",\"name\":\"unit_test_test_join_key\",\"namespace\":\"ai.chronon.data\",\"doc\":\"\",\"fields\":[{\"name\":\"listing\",\"type\":[\"null\",\"long\"],\"doc\":\"\"}]}","value_schema":"{\"type\":\"record\",\"name\":\"unit_test_test_join_value\",\"namespace\":\"ai.chronon.data\",\"doc\":\"\",\"fields\":[{\"name\":\"unit_test_listing_views_v1_m_guests_sum\",\"type\":[\"null\",\"long\"],\"doc\":\"\"},{\"name\":\"unit_test_listing_views_v1_m_views_sum\",\"type\":[\"null\",\"long\"],\"doc\":\"\"}]}"}
   */
  lazy val loggingSchema: String = JoinCodec.buildLoggingSchema(conf.join.metaData.name, keySchemaStr, valueSchemaStr)
  lazy val loggingSchemaHash: String = HashUtils.md5Base64(loggingSchema)

  val keys: Array[String] = keySchema.fields.iterator.map(_.name).toArray
  val values: Array[String] = valueSchema.fields.iterator.map(_.name).toArray

  val keyFields: Array[StructField] = keySchema.fields
  val valueFields: Array[StructField] = valueSchema.fields
  lazy val keyIndices: Map[StructField, Int] = keySchema.zipWithIndex.toMap
  lazy val valueIndices: Map[StructField, Int] = valueSchema.zipWithIndex.toMap
}

object JoinCodec {

  def buildLoggingSchema(joinName: String, keySchemaStr: String, valueSchemaStr: String): String = {
    val schemaMap = Map(
      "join_name" -> joinName,
      "key_schema" -> keySchemaStr,
      "value_schema" -> valueSchemaStr
    )
    new Gson().toJson(schemaMap.toJava)
  }
}
