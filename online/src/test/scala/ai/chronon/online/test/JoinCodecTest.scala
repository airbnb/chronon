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

package ai.chronon.online.test

import ai.chronon.api.{LongType, StructField, StructType}
import ai.chronon.online.DerivationUtils.reintroduceExceptions
import ai.chronon.online.serde.{AvroCodec, AvroConversions}
import org.junit.Assert.assertEquals
import org.junit.Test

class JoinCodecTest {
  @Test
  def testReintroduceException(): Unit = {

    val preDerived = Map("group_by_2_exception" -> "ex", "group_by_1_exception" -> "ex", "group_by_4_exception" -> "ex")
    val derived = Map(
      "group_by_1_feature1" -> "val1",
      "group_by_2_feature1" -> "val1",
      "group_by_2_feature2" -> "val2",
      "group_by_3_feature1" -> "val1",
      "derived1" -> "val1",
      "derived2" -> "val2"
    )

    val result = reintroduceExceptions(derived, preDerived)

    val expected = Map(
      "group_by_3_feature1" -> "val1",
      "derived1" -> "val1",
      "derived2" -> "val2",
      "group_by_2_exception" -> "ex",
      "group_by_1_exception" -> "ex",
      "group_by_4_exception" -> "ex"
    )
    assertEquals(expected, result)
  }

  @Test
  def testDecodeMapCleanWithDuplicateNames(): Unit = {
    // Test that decodeMapClean properly handles duplicate field names by removing _REPEATED_NAME_ suffix
    // This simulates the scenario where nested structures have duplicate field names
    val schema = StructType(
      "test_schema",
      Array(
        StructField("id", LongType),
        StructField("updated_at_ts", LongType),
        StructField("nested_data", StructType(
          "nested_data",
          Array(
            StructField("updated_at_ts", LongType)  // Duplicate name - will get suffix
          )
        ))
      )
    )

    // Create codec
    val avroSchema = AvroConversions.fromChrononSchema(schema)
    val codec = new AvroCodec(avroSchema.toString(true))

    // Encode some data
    val data = Array(
      123L.asInstanceOf[AnyRef],                    // id
      456L.asInstanceOf[AnyRef],                    // updated_at_ts (top level)
      Array(789L.asInstanceOf[AnyRef])              // nested_data.updated_at_ts
    )
    val encoded = codec.encodeArray(data)

    // Decode with regular decodeMap (has suffixes)
    val decodedWithSuffix = codec.decodeMap(encoded)

    // Decode with decodeMapClean (no suffixes)
    val decodedClean = codec.decodeMapClean(encoded)

    // Verify regular decode has the suffix
    assert(codec.fieldNames.exists(_.contains("_REPEATED_NAME_")),
           "Schema should contain _REPEATED_NAME_ suffix for duplicate field")

    // Verify clean decode has no suffixes
    val cleanKeys = decodedClean.keys.mkString(",")
    assert(!cleanKeys.contains("_REPEATED_NAME_"),
           s"Clean decoded map should not contain _REPEATED_NAME_ suffix, but got keys: $cleanKeys")

    // Verify both have the same number of top-level keys
    assertEquals(3, decodedWithSuffix.size)
    assertEquals(3, decodedClean.size)

    // Verify clean decode has expected field names
    assert(decodedClean.contains("id"))
    assert(decodedClean.contains("updated_at_ts"))
    assert(decodedClean.contains("nested_data"))
  }
}
