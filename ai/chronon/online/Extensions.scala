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

import ai.chronon.api
import org.apache.avro.Schema
import org.apache.spark.sql.types.StructType

object Extensions {

  implicit class ChrononStructTypeOps(schema: api.StructType) {
    def catalogString: String = {
      SparkConversions.fromChrononSchema(schema).catalogString
    }
  }

  implicit class StructTypeOps(schema: StructType) {
    def pretty: String = {
      val schemaTuples = schema.fields.map { field =>
        field.dataType.simpleString -> field.name
      }

      // pad the first column so that the second column is aligned vertically
      val padding = if (schemaTuples.isEmpty) 0 else schemaTuples.map(_._1.length).max
      schemaTuples
        .map {
          case (typ, name) => s"  ${typ.padTo(padding, ' ')} : $name"
        }
        .mkString("\n")
    }

    def toChrononSchema(name: String = null): api.StructType =
      api.StructType.from(name, SparkConversions.toChrononSchema(schema))

    def toAvroSchema(name: String = null): Schema = AvroConversions.fromChrononSchema(toChrononSchema(name))

    def toAvroCodec(name: String = null): AvroCodec = new AvroCodec(toAvroSchema(name).toString())
  }
}
