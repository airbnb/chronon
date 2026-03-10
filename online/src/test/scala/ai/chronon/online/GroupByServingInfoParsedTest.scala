/*
 *    Copyright (C) 2026 The Chronon Authors.
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

import ai.chronon.api.{LongType, StringType, StructField, StructType}
import ai.chronon.online.serde.AvroConversions
import org.apache.avro.Schema
import org.junit.Assert.{assertFalse, assertTrue}
import org.junit.Test

class GroupByServingInfoParsedTest {

  /**
    * When a struct column contains a sub-field with the same name as a top-level column,
    * AvroConversions.fromChrononSchema renames the outer field with a _REPEATED_NAME_N suffix
    * due to a shared global nameSet across nested struct serialization.
    *
    * selectedChrononSchema must strip the suffix (cleanName=true) so that aggregation input
    * lookups in valueChrononSchema succeed. Without the fix, the outer column is silently
    * dropped and RowAggregator throws NoSuchElementException: None.get at serving time.
    */
  @Test
  def testSelectedChrononSchemaStripsRepeatedNameSuffix(): Unit = {
    val filterDataStruct = StructType("FilterData", Array(
      StructField("session_id", StringType),
      StructField("category", StringType),
      StructField("timestamp", LongType) // inner field that collides with the outer column
    ))

    val selectedChrononType = StructType("Value", Array(
      StructField("filter_data", filterDataStruct),
      StructField("timestamp", LongType)
    ))

    // Serialize to Avro — the shared nameSet causes the outer "timestamp" to be renamed.
    val selectedAvroSchemaStr = AvroConversions.fromChrononSchema(selectedChrononType).toString()
    assertTrue(
      "selectedAvroSchema should contain _REPEATED_NAME_0 rename",
      selectedAvroSchemaStr.contains("timestamp_REPEATED_NAME_0")
    )

    // toChrononSchema with cleanName=true must strip the suffix on the read path.
    val parsedSchema = AvroConversions
      .toChrononSchema(new Schema.Parser().parse(selectedAvroSchemaStr), cleanName = true)
      .asInstanceOf[StructType]

    val fieldNames = parsedSchema.fields.map(_.name)
    assertTrue(
      s"Expected 'timestamp' in parsed schema, got: ${fieldNames.mkString(", ")}",
      fieldNames.contains("timestamp")
    )
    assertFalse(
      s"Did not expect REPEATED_NAME suffix in parsed schema, got: ${fieldNames.mkString(", ")}",
      fieldNames.exists(_.contains("REPEATED_NAME"))
    )
  }
}
