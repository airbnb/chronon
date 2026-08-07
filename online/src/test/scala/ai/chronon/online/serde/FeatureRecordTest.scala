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
import org.junit.Assert.{assertEquals, assertFalse, assertNull, assertTrue}
import org.junit.Test
import org.scalatest.Assertions.intercept

import java.util

class FeatureRecordTest {

  private val contentSchema = StructType("Content", Array(StructField("kind", StringType), StructField("text", StringType)))
  private val itemSchema = StructType(
    "Item",
    Array(StructField("id_action", StringType),
          StructField("id_source", StringType),
          StructField("contents", ListType(contentSchema))))
  private val insightSchema = StructType("Insight", Array(StructField("items", ListType(itemSchema))))
  private val rootSchema = StructType(
    "Root",
    Array(StructField("content_type", StringType),
          StructField("id_review", LongType),
          StructField("review_quality_insight_validation", insightSchema)))

  @Test
  def testFlatValuesPassThrough(): Unit = {
    val schema = StructType("Flat", Array(StructField("a", StringType), StructField("b", LongType)))
    val record = FeatureRecord.fromValueMap(schema, Map("a" -> "hello", "b" -> 5L))
    assertEquals("hello", record.getString("a"))
    assertEquals(5L, record.getLong("b"))
  }

  @Test
  def testNestedStructAndListOfStructsAreNamedAtEveryLevel(): Unit = {
    // Mirrors the shape produced by AvroConversions.toChrononRow: nested structs are positional
    // Array[Any], lists are java.util.ArrayList[Any]. Only the top level arrives already keyed by
    // name (as a fetcher response map would).
    val content1: Array[Any] = Array("HIGHLIGHT", "Great host")
    val content2: Array[Any] = Array("HIGHLIGHT", "Spotless")
    val contentsList = new util.ArrayList[Any]()
    contentsList.add(content1)
    contentsList.add(content2)

    val item1: Array[Any] = Array("CLARIFY_CHECKIN_PROCESS", "review:12345", contentsList)
    val itemsList = new util.ArrayList[Any]()
    itemsList.add(item1)

    val insight: Array[Any] = Array(itemsList)

    val valueMap: Map[String, Any] = Map(
      "content_type" -> "REVIEW_QUALITY",
      "id_review" -> 12345L,
      "review_quality_insight_validation" -> insight
    )

    val record = FeatureRecord.fromValueMap(rootSchema, valueMap)

    assertEquals("REVIEW_QUALITY", record.getString("content_type"))
    assertEquals(12345L, record.getLong("id_review"))

    val insightRecord = record.getStruct("review_quality_insight_validation")
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
  def testListOfPrimitivesIsNotWrapped(): Unit = {
    val schema = StructType("WithList", Array(StructField("tags", ListType(StringType))))
    val tagsList = new util.ArrayList[Any]()
    tagsList.add("a")
    tagsList.add("b")
    val record = FeatureRecord.fromValueMap(schema, Map("tags" -> tagsList))
    assertEquals(List("a", "b"), record.getList[String]("tags"))
  }

  @Test
  def testMissingFieldIsOmitted(): Unit = {
    val schema = StructType("Sparse", Array(StructField("a", StringType), StructField("b", StringType)))
    val record = FeatureRecord.fromValueMap(schema, Map("a" -> "present"))
    assertEquals("present", record.getString("a"))
    assertTrue(record.values.get("b").isEmpty)
    // the schema is narrowed to what the response actually carried
    assertEquals(Seq("a"), record.schema.fields.map(_.name).toSeq)
  }

  @Test
  def testNullTopLevelMapYieldsNullRecord(): Unit = {
    assertNull(FeatureRecord.fromValueMap(rootSchema, null))
  }

  @Test
  def testExceptionKeysAreRetainedEvenThoughTheyAreNotInTheSchema(): Unit = {
    // fetchJoin / fetchGroupBys report partial failures as `<name>_exception` entries in the value
    // map. Those keys are never part of the declared value schema, so they must pass through
    // rather than be dropped, otherwise a structured caller cannot see the failure at all.
    val schema = StructType("WithErrors", Array(StructField("a", StringType)))
    val record = FeatureRecord.fromValueMap(
      schema,
      Map("a" -> "ok", "some_group_by_exception" -> "boom", "derivation_fetch_exception" -> "kaboom"))

    assertEquals("ok", record.getString("a"))
    assertTrue(record.hasErrors)
    assertEquals(Set("some_group_by_exception", "derivation_fetch_exception"), record.errors.keySet)
    assertEquals("boom", record.errors("some_group_by_exception"))
  }

  @Test
  def testNoErrorsOnACleanRecord(): Unit = {
    val schema = StructType("Clean", Array(StructField("a", StringType)))
    val record = FeatureRecord.fromValueMap(schema, Map("a" -> "ok"))
    assertFalse(record.hasErrors)
    assertTrue(record.errors.isEmpty)
  }

  @Test
  def testPresentButNullFieldYieldsNoneNotSomeNull(): Unit = {
    // Nulls are routine: a groupBy with no data for a key returns nulls, and model-transform
    // passthrough inserts nulls explicitly. `Some(null)` would defeat the point of the Option and
    // break `.orElse(default)` at the call site.
    val schema = StructType("Nullable",
                            Array(StructField("s", StringType),
                                  StructField("n", LongType),
                                  StructField("b", BooleanType),
                                  StructField("st", insightSchema)))
    val record =
      FeatureRecord.fromValueMap(schema, Map("s" -> null, "n" -> null, "b" -> null, "st" -> null))

    assertEquals(None, record.getStringOpt("s"))
    assertEquals(None, record.getLongOpt("n"))
    assertEquals(None, record.getBooleanOpt("b"))
    assertEquals(None, record.getStructOpt("st"))
    assertEquals("fallback", record.getStringOpt("s").getOrElse("fallback"))
  }

  @Test
  def testAbsentFieldAlsoYieldsNone(): Unit = {
    val schema = StructType("Absent", Array(StructField("s", StringType)))
    val record = FeatureRecord.fromValueMap(schema, Map.empty[String, Any])
    assertEquals(None, record.getStringOpt("s"))
  }

  @Test
  def testNumericGettersWidenAcrossTypes(): Unit = {
    // A Chronon IntType field read as a Long matters because the Thrift-based consumer path maps
    // i64 fields onto getLongOpt.
    val schema = StructType("Nums",
                            Array(StructField("i", IntType),
                                  StructField("l", LongType),
                                  StructField("f", FloatType),
                                  StructField("d", DoubleType)))
    val record = FeatureRecord.fromValueMap(
      schema,
      Map("i" -> Integer.valueOf(5), "l" -> java.lang.Long.valueOf(6L), "f" -> java.lang.Float.valueOf(1.5f),
        "d" -> java.lang.Double.valueOf(2.5d)))

    assertEquals(5L, record.getLong("i"))
    assertEquals(Some(5L), record.getLongOpt("i"))
    assertEquals(5, record.getInt("i"))
    assertEquals(6, record.getInt("l"))
    assertEquals(1.5d, record.getDouble("f"), 0.0001)
    assertEquals(2.5d, record.getDouble("d"), 0.0001)
  }

  @Test
  def testNonNumericFieldReadAsNumberFailsClearly(): Unit = {
    val schema = StructType("Mixed", Array(StructField("s", StringType)))
    val record = FeatureRecord.fromValueMap(schema, Map("s" -> "not a number"))
    val ex = intercept[IllegalArgumentException](record.getLong("s"))
    assertTrue(ex.getMessage.contains("not numeric"))
  }

  @Test
  def testStructArrivingAsFieldKeyedMapIsNamed(): Unit = {
    // Derived struct columns can surface as maps keyed by field name rather than positional
    // arrays; Row.to handles the same two shapes.
    val scalaMapShape: Map[String, Any] = Map("kind" -> "HIGHLIGHT", "text" -> "Great host")
    val javaMapShape = new util.HashMap[String, Any]()
    javaMapShape.put("kind", "HIGHLIGHT")
    javaMapShape.put("text", "Spotless")

    val schema = StructType("Outer",
                            Array(StructField("from_scala_map", contentSchema),
                                  StructField("from_java_map", contentSchema)))
    val record =
      FeatureRecord.fromValueMap(schema, Map("from_scala_map" -> scalaMapShape, "from_java_map" -> javaMapShape))

    assertEquals("Great host", record.getStruct("from_scala_map").getString("text"))
    assertEquals("Spotless", record.getStruct("from_java_map").getString("text"))
  }

  @Test
  def testListOfStructsArrivingAsFieldKeyedMaps(): Unit = {
    val schema = StructType("WithStructList", Array(StructField("contents", ListType(contentSchema))))
    val elems = new util.ArrayList[Any]()
    elems.add(Map("kind" -> "HIGHLIGHT", "text" -> "a"))
    elems.add(Map("kind" -> "LOWLIGHT", "text" -> "b"))
    val record = FeatureRecord.fromValueMap(schema, Map("contents" -> elems))

    val contents = record.getStructList("contents")
    assertEquals(2, contents.size)
    assertEquals("a", contents.head.getString("text"))
    assertEquals("LOWLIGHT", contents(1).getString("kind"))
  }

  @Test
  def testMapTypedFieldWithStructValues(): Unit = {
    val schema = StructType("WithMap", Array(StructField("by_id", MapType(StringType, contentSchema))))
    val inner = new util.HashMap[Any, Any]()
    inner.put("k1", Array[Any]("HIGHLIGHT", "text one"))
    val record = FeatureRecord.fromValueMap(schema, Map("by_id" -> inner))

    val asMap = record.getMap[FeatureRecord]("by_id")
    assertEquals("text one", asMap("k1").getString("text"))
  }

  @Test
  def testUnsupportedStructShapeFailsClearly(): Unit = {
    val schema = StructType("Bad", Array(StructField("s", contentSchema)))
    val ex = intercept[IllegalArgumentException](FeatureRecord.fromValueMap(schema, Map("s" -> "just a string")))
    assertTrue(ex.getMessage.contains("Content"))
  }
}
