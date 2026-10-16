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

package ai.chronon.spark.test

import ai.chronon.spark.{FastHashing, KeyWithHash}
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{BinaryType, DataType, IntegerType, StringType, StructField, StructType}
import org.junit.Assert.{assertEquals, assertNotEquals}
import org.junit.Test

class FastHashingTest {

  private def keyBuilder(dataType: DataType): Row => KeyWithHash =
    FastHashing.generateKeyBuilder(Array("k1", "k2"),
                                   StructType(Seq(StructField("k1", dataType), StructField("k2", dataType))))

  @Test
  def testDistinctStringKeysWithSameConcatenatedBytes(): Unit = {
    val key = keyBuilder(StringType)
    Seq(
      Row("1", "23") -> Row("12", "3"),
      Row("7", "8") -> Row("78", ""),
      Row(null, "a") -> Row("a", null),
      Row("", "a") -> Row(null, "a")
    ).foreach {
      case (a, b) => assertNotEquals(s"$a and $b", key(a), key(b))
    }
  }

  @Test
  def testDistinctBinaryKeysWithSameConcatenatedBytes(): Unit = {
    val key = keyBuilder(BinaryType)
    assertNotEquals(key(Row(Array[Byte](1), Array[Byte](2, 3))), key(Row(Array[Byte](1, 2), Array[Byte](3))))
  }

  @Test
  def testDistinctIntKeysWithNulls(): Unit = {
    val key = keyBuilder(IntegerType)
    assertNotEquals(key(Row(1, null)), key(Row(null, 1)))
  }

  @Test
  def testEqualKeysHaveEqualHashes(): Unit = {
    Seq(
      StringType -> (Row("1", "23"), Row(new String("1"), new String("23"))),
      StringType -> (Row(null, "a"), Row(null, "a")),
      BinaryType -> (Row(Array[Byte](1), Array[Byte](2, 3)), Row(Array[Byte](1), Array[Byte](2, 3))),
      IntegerType -> (Row(1, null), Row(1, null))
    ).foreach {
      case (dataType, (a, b)) =>
        val key = keyBuilder(dataType)
        assertEquals(s"$a and $b", key(a), key(b))
        assertEquals(s"$a and $b", key(a).hashCode, key(b).hashCode)
    }
  }
}
