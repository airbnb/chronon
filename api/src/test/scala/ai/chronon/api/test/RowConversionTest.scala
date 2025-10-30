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

package ai.chronon.api.test

import ai.chronon.api._
import org.junit.Assert._
import org.junit.Test

import java.util
import scala.collection.immutable.HashMap

class RowConversionTest {

  @Test
  def testScalaMapToStructType(): Unit = {
    // Define a simple struct type with nested struct
    val addressType = StructType(
      "address",
      Array(
        StructField("city", StringType),
        StructField("zipcode", IntType)
      )
    )

    val personType = StructType(
      "person",
      Array(
        StructField("name", StringType),
        StructField("age", IntType),
        StructField("address", addressType)
      )
    )

    // Create test data using Scala HashMap
    val addressMap = HashMap(
      "city" -> "San Francisco",
      "zipcode" -> 94102
    )

    val personMap = HashMap(
      "name" -> "Alice",
      "age" -> 30,
      "address" -> addressMap
    )

    // Convert using Row.to
    val result = Row.to[Array[Any], Array[Byte], util.ArrayList[Any], util.Map[Any, Any], DataType](
      personMap,
      personType,
      (data: Iterator[Any], dt: DataType, schema: Option[DataType]) => data.toArray,
      (bytes: Array[Byte]) => bytes,
      (elems: Iterator[Any], size: Int) => {
        val list = new util.ArrayList[Any](size)
        elems.foreach(list.add)
        list
      },
      (m: util.Map[Any, Any]) => m,
      extraneousRecord = null,
      schemaTraverser = None
    )

    // Verify the result
    assertNotNull(result)
    assertTrue(result.isInstanceOf[Array[Any]])

    val arr = result.asInstanceOf[Array[Any]]
    assertEquals(3, arr.length)

    // Check top-level fields
    assertEquals("Alice", arr(0))
    assertEquals(30, arr(1))

    // Check nested struct was converted to Array
    assertTrue(arr(2).isInstanceOf[Array[Any]])
    val addressArr = arr(2).asInstanceOf[Array[Any]]
    assertEquals(2, addressArr.length)
    assertEquals("San Francisco", addressArr(0))
    assertEquals(94102, addressArr(1))
  }

  @Test
  def testMutableMapToStructType(): Unit = {
    val structType = StructType(
      "data",
      Array(
        StructField("id", LongType),
        StructField("value", DoubleType)
      )
    )

    val mutableMap = scala.collection.mutable.HashMap(
      "id" -> 123L,
      "value" -> 45.67
    )

    val result = Row.to[Array[Any], Array[Byte], util.ArrayList[Any], util.Map[Any, Any], DataType](
      mutableMap,
      structType,
      (data: Iterator[Any], dt: DataType, schema: Option[DataType]) => data.toArray,
      (bytes: Array[Byte]) => bytes,
      (elems: Iterator[Any], size: Int) => {
        val list = new util.ArrayList[Any](size)
        elems.foreach(list.add)
        list
      },
      (m: util.Map[Any, Any]) => m,
      extraneousRecord = null,
      schemaTraverser = None
    )

    assertNotNull(result)
    val arr = result.asInstanceOf[Array[Any]]
    assertEquals(2, arr.length)
    assertEquals(123L, arr(0))
    assertEquals(45.67, arr(1))
  }

  @Test
  def testMapWithMissingFields(): Unit = {
    val structType = StructType(
      "test",
      Array(
        StructField("field1", StringType),
        StructField("field2", IntType),
        StructField("field3", LongType)
      )
    )

    // Map only contains field1
    val partialMap = HashMap("field1" -> "present")

    val result = Row.to[Array[Any], Array[Byte], util.ArrayList[Any], util.Map[Any, Any], DataType](
      partialMap,
      structType,
      (data: Iterator[Any], dt: DataType, schema: Option[DataType]) => data.toArray,
      (bytes: Array[Byte]) => bytes,
      (elems: Iterator[Any], size: Int) => {
        val list = new util.ArrayList[Any](size)
        elems.foreach(list.add)
        list
      },
      (m: util.Map[Any, Any]) => m,
      extraneousRecord = null,
      schemaTraverser = None
    )

    val arr = result.asInstanceOf[Array[Any]]
    assertEquals(3, arr.length)
    assertEquals("present", arr(0))
    assertNull(arr(1)) // field2 is missing
    assertNull(arr(2)) // field3 is missing
  }
}
