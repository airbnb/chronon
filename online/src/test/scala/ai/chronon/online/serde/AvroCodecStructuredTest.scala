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

package ai.chronon.online.serde

import ai.chronon.api._
import org.apache.avro.generic.GenericData
import org.junit.Assert.assertEquals
import org.junit.Test

import java.util

class AvroCodecStructuredTest {

  // Mirrors the review_quality_insight_validation example from the structured-data design doc:
  // a struct field ("review_quality_insight_validation") containing a list of structs ("items"),
  // each of which contains a nested list of structs ("contents").
  private val contentSchema =
    StructType("Content", Array(StructField("kind", StringType), StructField("text", StringType)))
  private val itemSchema = StructType("Item",
                                      Array(StructField("id_action", StringType),
                                            StructField("id_source", StringType),
                                            StructField("contents", ListType(contentSchema))))
  private val insightSchema = StructType("Insight", Array(StructField("items", ListType(itemSchema))))
  private val rootSchema = StructType("Root",
                                      Array(StructField("content_type", StringType),
                                            StructField("id_review", LongType),
                                            StructField("review_quality_insight_validation", insightSchema)))

  @Test
  def testDecodeStructuredNamesNestedStructsAndListsOfStructs(): Unit = {
    val avroSchemaStr = AvroConversions.fromChrononSchema(rootSchema).toString(true)
    val codec = new AvroCodec(avroSchemaStr)

    val content1: Array[Any] = Array("HIGHLIGHT", "Great host")
    val content2: Array[Any] = Array("HIGHLIGHT", "Spotless")
    val contentsList = new util.ArrayList[Any]()
    contentsList.add(content1)
    contentsList.add(content2)

    val item1: Array[Any] = Array("CLARIFY_CHECKIN_PROCESS", "review:12345", contentsList)
    val itemsList = new util.ArrayList[Any]()
    itemsList.add(item1)

    val insight: Array[Any] = Array(itemsList)
    val row: Array[Any] = Array("REVIEW_QUALITY", 12345L, insight)

    val record =
      AvroConversions.fromChrononRow(row, codec.chrononSchema, codec.schema).asInstanceOf[GenericData.Record]
    val bytes = codec.encodeBinary(record)

    // Sanity check: the classic decodeMap loses the nested field names - structs come back as
    // bare positional arrays/lists.
    val flat = codec.decodeMap(bytes)
    assertEquals("REVIEW_QUALITY", flat("content_type"))
    assertEquals(classOf[Array[Any]], flat("review_quality_insight_validation").getClass)

    val featureRecord = codec.decodeStructured(bytes)
    assertEquals("REVIEW_QUALITY", featureRecord.getString("content_type"))
    assertEquals(12345L, featureRecord.getLong("id_review"))

    val insightRecord = featureRecord.getStruct("review_quality_insight_validation")
    val items = insightRecord.getStructList("items")
    assertEquals(1, items.size)

    val firstItem = items.head
    assertEquals("CLARIFY_CHECKIN_PROCESS", firstItem.getString("id_action"))
    assertEquals("review:12345", firstItem.getString("id_source"))

    val contents = firstItem.getStructList("contents")
    assertEquals(2, contents.size)
    assertEquals("HIGHLIGHT", contents.head.getString("kind"))
    assertEquals("Great host", contents.head.getString("text"))
    assertEquals("Spotless", contents(1).getString("text"))
  }

  @Test
  def testDecodeStructuredOfNullBytesIsNull(): Unit = {
    val avroSchemaStr = AvroConversions.fromChrononSchema(rootSchema).toString(true)
    val codec = new AvroCodec(avroSchemaStr)
    assertEquals(null, codec.decodeStructured(null))
  }
}
