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

import ai.chronon.api
import ai.chronon.api.Accuracy
import ai.chronon.spark.PySparkUtils
import org.junit.Assert.{assertEquals, assertTrue}
import org.junit.Test

/**
  * Tests for PySparkUtils helper methods.
  * These methods provide utility functions for PySpark/Py4J interop.
  */
class PySparkUtilsTest {

  @Test
  def testGetIntOptionalWithNull(): Unit = {
    val result = PySparkUtils.getIntOptional(null)
    assertTrue("getIntOptional(null) should return None", result.isEmpty)
  }

  @Test
  def testGetIntOptionalWithValue(): Unit = {
    val result = PySparkUtils.getIntOptional("42")
    assertTrue("getIntOptional('42') should return Some", result.isDefined)
    assertEquals("getIntOptional('42') should return 42", 42, result.get)
  }

  @Test
  def testGetIntOptionalWithZero(): Unit = {
    val result = PySparkUtils.getIntOptional("0")
    assertTrue("getIntOptional('0') should return Some(0)", result.isDefined)
    assertEquals("getIntOptional('0') should return 0", 0, result.get)
  }

  @Test
  def testGetStringOptionalWithNull(): Unit = {
    val result = PySparkUtils.getStringOptional(null)
    assertTrue("getStringOptional(null) should return None", result.isEmpty)
  }

  @Test
  def testGetStringOptionalWithValue(): Unit = {
    val result = PySparkUtils.getStringOptional("test")
    assertTrue("getStringOptional('test') should return Some", result.isDefined)
    assertEquals("getStringOptional('test') should return 'test'", "test", result.get)
  }

  @Test
  def testGetStringOptionalWithEmptyString(): Unit = {
    val result = PySparkUtils.getStringOptional("")
    assertTrue("getStringOptional('') should return Some('')", result.isDefined)
    assertEquals("getStringOptional('') should return empty string", "", result.get)
  }

  @Test
  def testGetTimeRangeOptionalWithNull(): Unit = {
    val result = PySparkUtils.getTimeRangeOptional(null)
    assertTrue("getTimeRangeOptional(null) should return None", result.isEmpty)
  }

  @Test
  def testGetAccuracyTemporal(): Unit = {
    val result = PySparkUtils.getAccuracy(getTemporal = true)
    assertEquals("getAccuracy(true) should return TEMPORAL", Accuracy.TEMPORAL, result)
  }

  @Test
  def testGetAccuracySnapshot(): Unit = {
    val result = PySparkUtils.getAccuracy(getTemporal = false)
    assertEquals("getAccuracy(false) should return SNAPSHOT", Accuracy.SNAPSHOT, result)
  }

  @Test
  def testParseGroupByBasic(): Unit = {
    val groupByJson = """{"metaData":{"name":"test_group_by"}}"""
    val result = PySparkUtils.parseGroupBy(groupByJson)
    assertEquals("parseGroupBy should parse name correctly", "test_group_by", result.metaData.name)
  }

  @Test
  def testParseJoinBasic(): Unit = {
    val joinJson = """{"metaData":{"name":"test_join"}}"""
    val result = PySparkUtils.parseJoin(joinJson)
    assertEquals("parseJoin should parse name correctly", "test_join", result.metaData.name)
  }

  @Test
  def testParseSourceBasic(): Unit = {
    // A minimal source with events
    val sourceJson = """{"events":{"table":"test_table"}}"""
    val result = PySparkUtils.parseSource(sourceJson)
    assertEquals("parseSource should parse events table correctly", "test_table", result.getEvents.getTable)
  }

  @Test
  def testGetFiveMinuteResolution(): Unit = {
    val resolution = PySparkUtils.getFiveMinuteResolution
    // FiveMinuteResolution has hopSizes array ending with 5 minutes = 300000 millis
    assertTrue("getFiveMinuteResolution should return FiveMinuteResolution", resolution.hopSizes.contains(300000L))
  }

  @Test
  def testParseStagingQueryBasic(): Unit = {
    val stagingQueryJson = """{"metaData":{"name":"test_staging_query","outputNamespace":"test_namespace"},"query":"SELECT * FROM test_table","startPartition":"20250101"}"""
    val result = PySparkUtils.parseStagingQuery(stagingQueryJson)
    assertEquals("parseStagingQuery should parse name correctly", "test_staging_query", result.metaData.name)
    assertEquals("parseStagingQuery should parse outputNamespace correctly", "test_namespace", result.metaData.outputNamespace)
    assertEquals("parseStagingQuery should parse query correctly", "SELECT * FROM test_table", result.query)
    assertEquals("parseStagingQuery should parse startPartition correctly", "20250101", result.startPartition)
  }

  @Test
  def testParseStagingQueryWithCreateView(): Unit = {
    // Thrift JSON codec uses integers for booleans (1 = true, 0 = false)
    val stagingQueryJson = """{"metaData":{"name":"test_view"},"query":"SELECT * FROM test","createView":1}"""
    val result = PySparkUtils.parseStagingQuery(stagingQueryJson)
    assertTrue("parseStagingQuery should parse createView correctly", result.createView)
  }

  @Test
  def testGetBooleanOptionalWithNull(): Unit = {
    val result = PySparkUtils.getBooleanOptional(null)
    assertTrue("getBooleanOptional(null) should return None", result.isEmpty)
  }

  @Test
  def testGetBooleanOptionalWithTrue(): Unit = {
    val result = PySparkUtils.getBooleanOptional(java.lang.Boolean.TRUE)
    assertTrue("getBooleanOptional(true) should return Some", result.isDefined)
    assertTrue("getBooleanOptional(true) should return true", result.get)
  }

  @Test
  def testGetBooleanOptionalWithFalse(): Unit = {
    val result = PySparkUtils.getBooleanOptional(java.lang.Boolean.FALSE)
    assertTrue("getBooleanOptional(false) should return Some", result.isDefined)
    assertTrue("getBooleanOptional(false) should return false", !result.get)
  }
}
