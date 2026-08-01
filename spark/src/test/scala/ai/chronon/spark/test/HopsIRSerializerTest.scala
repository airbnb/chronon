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

import ai.chronon.aggregator.test.Column
import ai.chronon.aggregator.windowing.{FiveMinuteResolution, HopsAggregator}
import ai.chronon.aggregator.windowing.HopsAggregator.IrMapType
import ai.chronon.api.Extensions._
import ai.chronon.api._
import ai.chronon.online.serde.SparkConversions
import ai.chronon.spark.serde.HopsIRSerializer
import ai.chronon.spark.SparkSessionBuilder
import junit.framework.TestCase
import org.apache.spark.sql.SparkSession
import org.junit.Assert._
import org.junit.Test

import scala.collection.Seq
import scala.util.Random

/**
  * Comprehensive tests for HopsIRSerializer - hop-level IR structure serialization.
  *
  * Tests cover:
  * 1. Basic round-trip serialization/deserialization
  * 2. Multiple hop sizes with temporal bucketing
  * 3. Merge operation after deserialization
  * 4. Different aggregation types (SUM, AVERAGE, complex)
  * 5. Null/empty handling
  * 6. Schema generation
  */
class HopsIRSerializerTest extends TestCase {

  lazy val spark: SparkSession =
    SparkSessionBuilder.build("HopsIRSerializerTest_" + Random.alphanumeric.take(6).mkString, local = true)

  @Test
  def testBasicRoundTrip(): Unit = {
    val schema = List(
      Column("value", LongType, 100),
      Column("score", DoubleType, 100)
    )
    val df = DataFrameGen.events(spark, schema, count = 100, partitions = 2)

    // Create aggregations with windows
    val aggregations = Seq(
      Builders.Aggregation(Operation.SUM, "value", Seq(new Window(7, TimeUnit.DAYS))),
      Builders.Aggregation(Operation.AVERAGE, "score", Seq(new Window(7, TimeUnit.DAYS)))
    )

    val inputSchema = schema.map(c => c.name -> c.`type`)
    val resolution = FiveMinuteResolution

    // Get min timestamp from data for HopsAggregator
    val rows = df.collect()
    val minTs = rows.map(_.getLong(df.schema.fieldIndex("ts"))).min

    val hopsAgg = new HopsAggregator(minTs, aggregations, inputSchema, resolution)

    // Create and populate hops
    val irMapType = hopsAgg.init()
    rows.take(50).foreach { sparkRow =>
      val chrononRow = SparkConversions.toChrononRow(sparkRow, df.schema.fieldIndex("ts"))
      hopsAgg.update(irMapType, chrononRow)
    }

    // Serialize
    val sparkRows =
      HopsIRSerializer.toSparkRows(irMapType, resolution.hopSizes, hopsAgg.rowAggregator.columnAggregators).toArray
    assertNotNull("Spark rows should not be null", sparkRows)
    assertTrue("Should have at least some rows", sparkRows.length > 0)

    // Verify each row has hop_size and timestamp fields
    sparkRows.foreach { row =>
      val hopSize = row.getLong(0)
      val timestamp = row.getLong(1)
      assertTrue(s"Hop size $hopSize should be in resolution", resolution.hopSizes.contains(hopSize))
      assertTrue(s"Timestamp $timestamp should be positive", timestamp > 0)
    }

    // Deserialize
    val deserializedIrMapType =
      HopsIRSerializer.fromSparkRows(sparkRows, resolution.hopSizes, hopsAgg.rowAggregator.columnAggregators)
    assertNotNull("Deserialized IrMapType should not be null", deserializedIrMapType)
    assertEquals("Deserialized should have same length", irMapType.length, deserializedIrMapType.length)

    // Verify structure: check that each hop size's map has same keys
    for (i <- irMapType.indices) {
      val originalMap = irMapType(i)
      val deserializedMap = deserializedIrMapType(i)

      assertEquals(s"Hop size $i should have same number of hops", originalMap.size(), deserializedMap.size())

      import scala.collection.JavaConverters._
      val originalKeys = originalMap.keySet().asScala
      val deserializedKeys = deserializedMap.keySet().asScala

      assertEquals(s"Hop size $i should have same hop timestamps", originalKeys, deserializedKeys)
    }
  }

  @Test
  def testMergeAfterDeserialization(): Unit = {
    val schema = List(Column("value", LongType, 100))
    val df = DataFrameGen.events(spark, schema, count = 100, partitions = 2)

    val aggregations = Seq(
      Builders.Aggregation(Operation.SUM, "value", Seq(new Window(7, TimeUnit.DAYS)))
    )

    val inputSchema = schema.map(c => c.name -> c.`type`)
    val resolution = FiveMinuteResolution
    val rows = df.collect()
    val minTs = rows.map(_.getLong(df.schema.fieldIndex("ts"))).min

    val hopsAgg = new HopsAggregator(minTs, aggregations, inputSchema, resolution)

    // Create two separate hop maps
    val irMapType1 = hopsAgg.init()
    val irMapType2 = hopsAgg.init()

    // Populate first half in irMapType1
    rows.take(50).foreach { sparkRow =>
      val chrononRow = SparkConversions.toChrononRow(sparkRow, df.schema.fieldIndex("ts"))
      hopsAgg.update(irMapType1, chrononRow)
    }

    // Populate second half in irMapType2
    rows.drop(50).foreach { sparkRow =>
      val chrononRow = SparkConversions.toChrononRow(sparkRow, df.schema.fieldIndex("ts"))
      hopsAgg.update(irMapType2, chrononRow)
    }

    // Serialize both
    val sparkRows1 =
      HopsIRSerializer.toSparkRows(irMapType1, resolution.hopSizes, hopsAgg.rowAggregator.columnAggregators).toArray
    val sparkRows2 =
      HopsIRSerializer.toSparkRows(irMapType2, resolution.hopSizes, hopsAgg.rowAggregator.columnAggregators).toArray

    // Deserialize both
    val deserialized1 =
      HopsIRSerializer.fromSparkRows(sparkRows1, resolution.hopSizes, hopsAgg.rowAggregator.columnAggregators)
    val deserialized2 =
      HopsIRSerializer.fromSparkRows(sparkRows2, resolution.hopSizes, hopsAgg.rowAggregator.columnAggregators)

    // Merge deserialized hops
    val merged = hopsAgg.merge(deserialized1, deserialized2)
    assertNotNull("Merged result should not be null", merged)

    // Create reference by merging original hops (clone by serializing/deserializing)
    val clonedRows1 =
      HopsIRSerializer.toSparkRows(irMapType1, resolution.hopSizes, hopsAgg.rowAggregator.columnAggregators).toArray
    val clonedRows2 =
      HopsIRSerializer.toSparkRows(irMapType2, resolution.hopSizes, hopsAgg.rowAggregator.columnAggregators).toArray
    val cloned1 =
      HopsIRSerializer.fromSparkRows(clonedRows1, resolution.hopSizes, hopsAgg.rowAggregator.columnAggregators)
    val cloned2 =
      HopsIRSerializer.fromSparkRows(clonedRows2, resolution.hopSizes, hopsAgg.rowAggregator.columnAggregators)
    val referenceMerged = hopsAgg.merge(cloned1, cloned2)

    // Verify merged results match
    for (i <- merged.indices) {
      val mergedMap = merged(i)
      val referenceMap = referenceMerged(i)

      assertEquals(s"Hop size $i should have same number of hops after merge", referenceMap.size(), mergedMap.size())

      import scala.collection.JavaConverters._
      referenceMap.keySet().asScala.foreach { hopStart =>
        val mergedHopIr = mergedMap.get(hopStart)
        val referenceHopIr = referenceMap.get(hopStart)

        assertNotNull(s"Hop $hopStart should exist in merged", mergedHopIr)
        assertNotNull(s"Hop $hopStart should exist in reference", referenceHopIr)

        // Compare finalized results
        val mergedResult = hopsAgg.rowAggregator.columnAggregators(0).finalize(mergedHopIr(0))
        val referenceResult = hopsAgg.rowAggregator.columnAggregators(0).finalize(referenceHopIr(0))

        assertEquals(s"Hop $hopStart results should match", referenceResult, mergedResult)
      }
    }
  }

  @Test
  def testComplexAggregations(): Unit = {
    val schema = List(
      Column("category", StringType, 10),
      Column("value", LongType, 100)
    )
    val df = DataFrameGen.events(spark, schema, count = 100, partitions = 2)

    val aggregations = Seq(
      Builders.Aggregation(Operation.HISTOGRAM, "category", Seq(new Window(7, TimeUnit.DAYS))),
      Builders.Aggregation(Operation.LAST_K, "value", Seq(new Window(7, TimeUnit.DAYS)), Map("k" -> "5"))
    )

    val inputSchema = schema.map(c => c.name -> c.`type`)
    val resolution = FiveMinuteResolution
    val rows = df.collect()
    val minTs = rows.map(_.getLong(df.schema.fieldIndex("ts"))).min

    val hopsAgg = new HopsAggregator(minTs, aggregations, inputSchema, resolution)

    // Create and populate hops
    val irMapType = hopsAgg.init()
    rows.take(50).foreach { sparkRow =>
      val chrononRow = SparkConversions.toChrononRow(sparkRow, df.schema.fieldIndex("ts"))
      hopsAgg.update(irMapType, chrononRow)
    }

    // Serialize and deserialize
    val sparkRows =
      HopsIRSerializer.toSparkRows(irMapType, resolution.hopSizes, hopsAgg.rowAggregator.columnAggregators).toArray
    val deserialized =
      HopsIRSerializer.fromSparkRows(sparkRows, resolution.hopSizes, hopsAgg.rowAggregator.columnAggregators)

    // Verify a sample hop's results match
    if (irMapType(0).size() > 0) {
      import scala.collection.JavaConverters._
      val hopStart = irMapType(0).keySet().asScala.head

      val originalHopIr = irMapType(0).get(hopStart)
      val deserializedHopIr = deserialized(0).get(hopStart)

      // Check HISTOGRAM
      val origHist = hopsAgg.rowAggregator
        .columnAggregators(0)
        .finalize(originalHopIr(0))
        .asInstanceOf[java.util.Map[String, _]]
      val deserHist = hopsAgg.rowAggregator
        .columnAggregators(0)
        .finalize(deserializedHopIr(0))
        .asInstanceOf[java.util.Map[String, _]]

      assertEquals("HISTOGRAM should have same size", origHist.size(), deserHist.size())
      origHist.keySet().asScala.foreach { key =>
        assertEquals(s"HISTOGRAM[$key] should match", origHist.get(key), deserHist.get(key))
      }

      // Check LAST_K
      val origLastK = hopsAgg.rowAggregator
        .columnAggregators(1)
        .finalize(originalHopIr(1))
        .asInstanceOf[java.util.ArrayList[_]]
        .asScala
      val deserLastK = hopsAgg.rowAggregator
        .columnAggregators(1)
        .finalize(deserializedHopIr(1))
        .asInstanceOf[java.util.ArrayList[_]]
        .asScala

      assertEquals("LAST_K should have same size", origLastK.size, deserLastK.size)
      assertEquals("LAST_K values should match", origLastK, deserLastK)
    }
  }

  @Test
  def testEmptyHops(): Unit = {
    val schema = List(Column("value", LongType, 100))
    val aggregations = Seq(
      Builders.Aggregation(Operation.SUM, "value", Seq(new Window(7, TimeUnit.DAYS)))
    )

    val inputSchema = schema.map(c => c.name -> c.`type`)
    val resolution = FiveMinuteResolution
    val hopsAgg = new HopsAggregator(0L, aggregations, inputSchema, resolution)

    // Create empty hops (no updates)
    val irMapType = hopsAgg.init()

    // Serialize and deserialize empty structure
    val sparkRows =
      HopsIRSerializer.toSparkRows(irMapType, resolution.hopSizes, hopsAgg.rowAggregator.columnAggregators)
    val deserialized =
      HopsIRSerializer.fromSparkRows(sparkRows, resolution.hopSizes, hopsAgg.rowAggregator.columnAggregators)

    assertNotNull("Deserialized empty hops should not be null", deserialized)
    assertEquals("Should have correct number of hop sizes", irMapType.length, deserialized.length)

    for (i <- deserialized.indices) {
      assertTrue(s"Hop size $i should be empty", deserialized(i).isEmpty)
    }
  }

  @Test
  def testNullHandling(): Unit = {
    val schema = List(Column("value", LongType, 100))
    val aggregations = Seq(
      Builders.Aggregation(Operation.SUM, "value", Seq(new Window(7, TimeUnit.DAYS)))
    )

    val inputSchema = schema.map(c => c.name -> c.`type`)
    val resolution = FiveMinuteResolution
    val hopsAgg = new HopsAggregator(0L, aggregations, inputSchema, resolution)

    // Test null input
    val deserialized =
      HopsIRSerializer.fromSparkRows(null, resolution.hopSizes, hopsAgg.rowAggregator.columnAggregators)
    assertNull("Deserializing null should return null", deserialized)

    val serialized = HopsIRSerializer.toSparkRows(null, resolution.hopSizes, hopsAgg.rowAggregator.columnAggregators)
    assertNull("Serializing null should return null", serialized)
  }

  @Test
  def testSchemaGeneration(): Unit = {
    val schema = List(
      Column("value", LongType, 100),
      Column("score", DoubleType, 100)
    )

    val aggregations = Seq(
      Builders.Aggregation(Operation.SUM, "value", Seq(new Window(7, TimeUnit.DAYS))),
      Builders.Aggregation(Operation.AVERAGE, "score", Seq(new Window(7, TimeUnit.DAYS)))
    )

    val inputSchema = schema.map(c => c.name -> c.`type`)
    val resolution = FiveMinuteResolution
    val hopsAgg = new HopsAggregator(0L, aggregations, inputSchema, resolution)

    // Generate schema
    val sparkSchema = HopsIRSerializer.schema(hopsAgg.rowAggregator.columnAggregators)
    assertNotNull("Schema should not be null", sparkSchema)

    // Schema should be ArrayType(StructType(...))
    assertTrue("Schema should be ArrayType", sparkSchema.isInstanceOf[org.apache.spark.sql.types.ArrayType])

    val arrayType = sparkSchema.asInstanceOf[org.apache.spark.sql.types.ArrayType]
    assertTrue("Array element should be StructType",
               arrayType.elementType.isInstanceOf[org.apache.spark.sql.types.StructType])

    val rowStruct = arrayType.elementType.asInstanceOf[org.apache.spark.sql.types.StructType]

    // Should have: hop_size, timestamp, ir_0, ir_1, ...
    val expectedFieldCount = 2 + hopsAgg.rowAggregator.columnAggregators.length
    assertEquals("Struct should have hop_size + timestamp + IR fields", expectedFieldCount, rowStruct.fields.length)

    assertEquals("First field should be hop_size", "hop_size", rowStruct.fields(0).name)
    assertEquals("Second field should be timestamp", "timestamp", rowStruct.fields(1).name)
    assertEquals("hop_size should be LongType", org.apache.spark.sql.types.LongType, rowStruct.fields(0).dataType)
    assertEquals("timestamp should be LongType", org.apache.spark.sql.types.LongType, rowStruct.fields(1).dataType)
  }

  @Test
  def testTimestampPreservation(): Unit = {
    val schema = List(Column("value", LongType, 100))
    val df = DataFrameGen.events(spark, schema, count = 50, partitions = 1)

    val aggregations = Seq(
      Builders.Aggregation(Operation.SUM, "value", Seq(new Window(7, TimeUnit.DAYS)))
    )

    val inputSchema = schema.map(c => c.name -> c.`type`)
    val resolution = FiveMinuteResolution
    val rows = df.collect()
    val minTs = rows.map(_.getLong(df.schema.fieldIndex("ts"))).min

    val hopsAgg = new HopsAggregator(minTs, aggregations, inputSchema, resolution)

    // Create and populate hops
    val irMapType = hopsAgg.init()
    rows.foreach { sparkRow =>
      val chrononRow = SparkConversions.toChrononRow(sparkRow, df.schema.fieldIndex("ts"))
      hopsAgg.update(irMapType, chrononRow)
    }

    // Collect original timestamps from HopIr structures
    import scala.collection.JavaConverters._
    val originalTimestamps = irMapType(0)
      .values()
      .asScala
      .map { hopIr =>
        hopIr.last.asInstanceOf[Long] // Last element is timestamp
      }
      .toSet

    // Serialize and deserialize
    val sparkRows =
      HopsIRSerializer.toSparkRows(irMapType, resolution.hopSizes, hopsAgg.rowAggregator.columnAggregators).toArray
    val deserialized =
      HopsIRSerializer.fromSparkRows(sparkRows, resolution.hopSizes, hopsAgg.rowAggregator.columnAggregators)

    // Verify timestamps are preserved
    val deserializedTimestamps = deserialized(0)
      .values()
      .asScala
      .map { hopIr =>
        hopIr.last.asInstanceOf[Long]
      }
      .toSet

    assertEquals("Timestamps should be preserved", originalTimestamps, deserializedTimestamps)

    // Also verify timestamps match map keys
    deserialized(0).entrySet().asScala.foreach { entry =>
      val hopStart = entry.getKey
      val hopIr = entry.getValue
      val irTimestamp = hopIr.last.asInstanceOf[Long]

      assertEquals(s"HopIr timestamp should match map key", hopStart, irTimestamp)
    }
  }
}
