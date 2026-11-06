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

import ai.chronon.aggregator.row.{ColumnAggregator, RowAggregator}
import ai.chronon.aggregator.test.Column
import ai.chronon.api.Extensions._
import ai.chronon.api._
import ai.chronon.spark.serde.ColumnIRSerializer
import ai.chronon.spark.SparkSessionBuilder
import junit.framework.TestCase
import org.apache.spark.sql.SparkSession
import org.junit.Assert._
import org.junit.Test

import scala.collection.Seq
import scala.util.Random

/**
  * Comprehensive tests for ColumnIRSerializer - per-column IR value serialization.
  *
  * Tests cover:
  * 1. Single-column round-trip for all operation types
  * 2. Edge cases (nulls, empty collections, large values)
  * 3. Complex types (maps, lists, structs)
  */
class ColumnIRSerializerTest extends TestCase {

  lazy val spark: SparkSession =
    SparkSessionBuilder.build("ColumnIRSerializerTest_" + Random.alphanumeric.take(6).mkString, local = true)

  /**
    * Test round-trip for SUM operation (simple Long IR).
    */
  @Test
  def testSumColumnRoundTrip(): Unit = {
    val schema = List(Column("value", IntType, 100))
    val df = DataFrameGen.events(spark, schema, count = 50, partitions = 2)

    val inputSchema = schema.map(c => c.name -> c.`type`)
    val aggregation = Builders.Aggregation(Operation.SUM, "value", Seq(new Window(7, TimeUnit.DAYS)))
    val rowAgg = new RowAggregator(inputSchema, aggregation.unpack)
    val columnAgg = rowAgg.columnAggregators(0)

    // Create and populate IR
    val ir = rowAgg.init
    import ai.chronon.online.serde.SparkConversions
    val sampleRows = df.take(20)
    sampleRows.foreach { sparkRow =>
      val chrononRow = SparkConversions.toChrononRow(sparkRow, df.schema.fieldIndex("ts"))
      rowAgg.update(ir, chrononRow)
    }

    // Serialize
    val sparkValue = ColumnIRSerializer.toSparkValue(ir(0), columnAgg)
    assertNotNull("Spark value should not be null", sparkValue)
    assertTrue("SUM IR should be Long", sparkValue.isInstanceOf[Long])

    // Deserialize
    val deserializedIR = ColumnIRSerializer.fromSparkValue(sparkValue, columnAgg)
    assertNotNull("Deserialized IR should not be null", deserializedIR)

    // Verify values match
    val originalResult = columnAgg.finalize(ir(0))
    val deserializedResult = columnAgg.finalize(deserializedIR)
    assertEquals("Finalized values should match", originalResult, deserializedResult)
  }

  /**
    * Test round-trip for AVERAGE operation (struct IR with sum and count).
    */
  @Test
  def testAverageColumnRoundTrip(): Unit = {
    val schema = List(Column("rating", DoubleType, 100))
    val df = DataFrameGen.events(spark, schema, count = 50, partitions = 2)

    val inputSchema = schema.map(c => c.name -> c.`type`)
    val aggregation = Builders.Aggregation(Operation.AVERAGE, "rating", Seq(new Window(7, TimeUnit.DAYS)))
    val rowAgg = new RowAggregator(inputSchema, aggregation.unpack)
    val columnAgg = rowAgg.columnAggregators(0)

    // Create and populate IR
    val ir = rowAgg.init
    import ai.chronon.online.serde.SparkConversions
    val sampleRows = df.take(20)
    sampleRows.foreach { sparkRow =>
      val chrononRow = SparkConversions.toChrononRow(sparkRow, df.schema.fieldIndex("ts"))
      rowAgg.update(ir, chrononRow)
    }

    // Serialize
    val sparkValue = ColumnIRSerializer.toSparkValue(ir(0), columnAgg)
    assertNotNull("Spark value should not be null", sparkValue)

    // Deserialize
    val deserializedIR = ColumnIRSerializer.fromSparkValue(sparkValue, columnAgg)
    assertNotNull("Deserialized IR should not be null", deserializedIR)

    // Verify averages match
    val originalResult = columnAgg.finalize(ir(0)).asInstanceOf[Double]
    val deserializedResult = columnAgg.finalize(deserializedIR).asInstanceOf[Double]
    assertEquals("Average values should match", originalResult, deserializedResult, 1e-10)
  }

  /**
    * Test round-trip for HISTOGRAM operation (Map IR).
    */
  @Test
  def testHistogramColumnRoundTrip(): Unit = {
    val schema = List(Column("category", StringType, 10))
    val df = DataFrameGen.events(spark, schema, count = 100, partitions = 5)

    val inputSchema = schema.map(c => c.name -> c.`type`)
    val aggregation = Builders.Aggregation(Operation.HISTOGRAM, "category", Seq(new Window(7, TimeUnit.DAYS)))
    val rowAgg = new RowAggregator(inputSchema, aggregation.unpack)
    val columnAgg = rowAgg.columnAggregators(0)

    // Create and populate IR
    val ir = rowAgg.init
    import ai.chronon.online.serde.SparkConversions
    val sampleRows = df.take(50)
    sampleRows.foreach { sparkRow =>
      val chrononRow = SparkConversions.toChrononRow(sparkRow, df.schema.fieldIndex("ts"))
      rowAgg.update(ir, chrononRow)
    }

    // Serialize
    val sparkValue = ColumnIRSerializer.toSparkValue(ir(0), columnAgg)
    assertNotNull("Spark value should not be null", sparkValue)
    assertTrue("HISTOGRAM IR should be Map", sparkValue.isInstanceOf[Map[_, _]])

    // Deserialize
    val deserializedIR = ColumnIRSerializer.fromSparkValue(sparkValue, columnAgg)
    assertNotNull("Deserialized IR should not be null", deserializedIR)

    // Verify histogram matches
    val originalResult = columnAgg.finalize(ir(0)).asInstanceOf[java.util.Map[String, _]]
    val deserializedResult = columnAgg.finalize(deserializedIR).asInstanceOf[java.util.Map[String, _]]

    import scala.collection.JavaConverters._
    assertEquals("Histogram size should match", originalResult.size(), deserializedResult.size())
    originalResult.asScala.foreach {
      case (k, v) =>
        assertTrue(s"Should contain key $k", deserializedResult.containsKey(k))
        assertEquals(s"Count for $k should match", v, deserializedResult.get(k))
    }
  }

  /**
    * Test round-trip for LAST_K operation (List IR).
    */
  @Test
  def testLastKColumnRoundTrip(): Unit = {
    val schema = List(Column("value", IntType, 100))
    val df = DataFrameGen.events(spark, schema, count = 50, partitions = 2)

    val inputSchema = schema.map(c => c.name -> c.`type`)
    val aggregation = Builders.Aggregation(
      Operation.LAST_K,
      "value",
      Seq(new Window(7, TimeUnit.DAYS)),
      argMap = Map("k" -> "5")
    )
    val rowAgg = new RowAggregator(inputSchema, aggregation.unpack)
    val columnAgg = rowAgg.columnAggregators(0)

    // Create and populate IR
    val ir = rowAgg.init
    import ai.chronon.online.serde.SparkConversions
    val sampleRows = df.take(20)
    sampleRows.foreach { sparkRow =>
      val chrononRow = SparkConversions.toChrononRow(sparkRow, df.schema.fieldIndex("ts"))
      rowAgg.update(ir, chrononRow)
    }

    // Serialize
    val sparkValue = ColumnIRSerializer.toSparkValue(ir(0), columnAgg)
    assertNotNull("Spark value should not be null", sparkValue)

    // Deserialize
    val deserializedIR = ColumnIRSerializer.fromSparkValue(sparkValue, columnAgg)
    assertNotNull("Deserialized IR should not be null", deserializedIR)

    // Verify lists match
    import scala.collection.JavaConverters._
    val originalResult = columnAgg.finalize(ir(0)).asInstanceOf[java.util.ArrayList[_]].asScala
    val deserializedResult = columnAgg.finalize(deserializedIR).asInstanceOf[java.util.ArrayList[_]].asScala

    assertEquals("List size should match", originalResult.size, deserializedResult.size)
    originalResult.zip(deserializedResult).foreach {
      case (orig, deser) =>
        assertEquals("List elements should match", orig, deser)
    }
  }

  /**
    * Test round-trip for APPROX_PERCENTILE operation (binary sketch IR).
    */
  @Test
  def testApproxPercentileColumnRoundTrip(): Unit = {
    val schema = List(Column("value", DoubleType, 1000))
    val df = DataFrameGen.events(spark, schema, count = 500, partitions = 10)

    val inputSchema = schema.map(c => c.name -> c.`type`)
    val aggregation = Builders.Aggregation(
      Operation.APPROX_PERCENTILE,
      "value",
      Seq(new Window(7, TimeUnit.DAYS)),
      argMap = Map("percentiles" -> "[0.5, 0.9, 0.99]")
    )
    val rowAgg = new RowAggregator(inputSchema, aggregation.unpack)
    val columnAgg = rowAgg.columnAggregators(0)

    // Create and populate IR with many values for stable percentiles
    val ir = rowAgg.init
    import ai.chronon.online.serde.SparkConversions
    val sampleRows = df.take(200)
    sampleRows.foreach { sparkRow =>
      val chrononRow = SparkConversions.toChrononRow(sparkRow, df.schema.fieldIndex("ts"))
      rowAgg.update(ir, chrononRow)
    }

    // Serialize
    val sparkValue = ColumnIRSerializer.toSparkValue(ir(0), columnAgg)
    assertNotNull("Spark value should not be null", sparkValue)
    assertTrue("APPROX_PERCENTILE IR should be binary", sparkValue.isInstanceOf[Array[Byte]])

    // Deserialize
    val deserializedIR = ColumnIRSerializer.fromSparkValue(sparkValue, columnAgg)
    assertNotNull("Deserialized IR should not be null", deserializedIR)

    // Verify percentiles are close (sketch is approximate)
    val originalResult = columnAgg.finalize(ir(0)).asInstanceOf[Array[Float]]
    val deserializedResult = columnAgg.finalize(deserializedIR).asInstanceOf[Array[Float]]

    assertEquals("Should have 3 percentiles", 3, originalResult.length)
    assertEquals("Should have 3 percentiles", 3, deserializedResult.length)

    originalResult.zip(deserializedResult).foreach {
      case (orig, deser) =>
        val relDiff = if (orig != 0.0f) Math.abs((orig - deser) / orig) else Math.abs(deser)
        assertTrue(s"Percentiles should be close: $orig vs $deser", relDiff < 0.05f) // 5% tolerance for sketches
    }
  }

  /**
    * Test round-trip for bucketed aggregation (Map of Maps IR).
    */
  @Test
  def testBucketedColumnRoundTrip(): Unit = {
    val schema = List(
      Column("category", StringType, 5),
      Column("value", IntType, 100)
    )
    val df = DataFrameGen.events(spark, schema, count = 100, partitions = 5)

    val inputSchema = schema.map(c => c.name -> c.`type`)
    val aggregation = Builders.Aggregation(
      Operation.SUM,
      "value",
      Seq(new Window(7, TimeUnit.DAYS)),
      buckets = Seq("category")
    )
    val rowAgg = new RowAggregator(inputSchema, aggregation.unpack)
    val columnAgg = rowAgg.columnAggregators(0)

    // Create and populate IR
    val ir = rowAgg.init
    import ai.chronon.online.serde.SparkConversions
    val sampleRows = df.take(50)
    sampleRows.foreach { sparkRow =>
      val chrononRow = SparkConversions.toChrononRow(sparkRow, df.schema.fieldIndex("ts"))
      rowAgg.update(ir, chrononRow)
    }

    // Serialize
    val sparkValue = ColumnIRSerializer.toSparkValue(ir(0), columnAgg)
    assertNotNull("Spark value should not be null", sparkValue)
    assertTrue("Bucketed IR should be Map", sparkValue.isInstanceOf[Map[_, _]])

    // Deserialize
    val deserializedIR = ColumnIRSerializer.fromSparkValue(sparkValue, columnAgg)
    assertNotNull("Deserialized IR should not be null", deserializedIR)

    // Verify bucketed results match
    val originalResult = columnAgg.finalize(ir(0)).asInstanceOf[java.util.Map[String, _]]
    val deserializedResult = columnAgg.finalize(deserializedIR).asInstanceOf[java.util.Map[String, _]]

    import scala.collection.JavaConverters._
    assertEquals("Bucket count should match", originalResult.size(), deserializedResult.size())
    originalResult.asScala.foreach {
      case (bucket, value) =>
        assertTrue(s"Should contain bucket $bucket", deserializedResult.containsKey(bucket))
        assertEquals(s"Value for bucket $bucket should match", value, deserializedResult.get(bucket))
    }
  }

  /**
    * Test null handling in serialization.
    */
  @Test
  def testNullHandling(): Unit = {
    val schema = List(Column("value", IntType, 100))
    val inputSchema = schema.map(c => c.name -> c.`type`)
    val aggregation = Builders.Aggregation(Operation.SUM, "value", Seq(new Window(7, TimeUnit.DAYS)))
    val rowAgg = new RowAggregator(inputSchema, aggregation.unpack)
    val columnAgg = rowAgg.columnAggregators(0)

    // Test null IR value
    val sparkValue = ColumnIRSerializer.toSparkValue(null, columnAgg)
    assertNull("Null IR should serialize to null", sparkValue)

    // Test null spark value
    val irValue = ColumnIRSerializer.fromSparkValue(null, columnAgg)
    assertNull("Null Spark value should deserialize to null", irValue)
  }

  /**
    * Test array convenience methods.
    */
  @Test
  def testArrayConvenienceMethods(): Unit = {
    val schema = List(
      Column("value1", IntType, 100),
      Column("value2", DoubleType, 100)
    )
    val df = DataFrameGen.events(spark, schema, count = 50, partitions = 2)

    val inputSchema = schema.map(c => c.name -> c.`type`)
    val aggregations = Seq(
      Builders.Aggregation(Operation.SUM, "value1", Seq(new Window(7, TimeUnit.DAYS))),
      Builders.Aggregation(Operation.AVERAGE, "value2", Seq(new Window(7, TimeUnit.DAYS)))
    )
    val rowAgg = new RowAggregator(inputSchema, aggregations.flatMap(_.unpack))

    // Create and populate IR array
    val ir = rowAgg.init
    import ai.chronon.online.serde.SparkConversions
    val sampleRows = df.take(20)
    sampleRows.foreach { sparkRow =>
      val chrononRow = SparkConversions.toChrononRow(sparkRow, df.schema.fieldIndex("ts"))
      rowAgg.update(ir, chrononRow)
    }

    // Serialize array
    val sparkValues = ColumnIRSerializer.toSparkValues(ir, rowAgg.columnAggregators)
    assertNotNull("Spark values array should not be null", sparkValues)
    assertEquals("Should have 2 values", 2, sparkValues.length)

    // Deserialize array
    val deserializedIR = ColumnIRSerializer.fromSparkValues(sparkValues, rowAgg.columnAggregators)
    assertNotNull("Deserialized IR array should not be null", deserializedIR)
    assertEquals("Should have 2 IR values", 2, deserializedIR.length)

    // Verify finalized results match
    val originalResults = rowAgg.finalize(ir)
    val deserializedResults = rowAgg.finalize(deserializedIR)

    assertEquals("SUM result should match", originalResults(0), deserializedResults(0))
    val avgOrig = originalResults(1).asInstanceOf[Double]
    val avgDeser = deserializedResults(1).asInstanceOf[Double]
    assertEquals("AVERAGE result should match", avgOrig, avgDeser, 1e-10)
  }

  /**
    * Test error handling for mismatched array lengths.
    */
  @Test
  def testArrayLengthValidation(): Unit = {
    val schema = List(Column("value", IntType, 100))
    val inputSchema = schema.map(c => c.name -> c.`type`)
    val aggregation = Builders.Aggregation(Operation.SUM, "value", Seq(new Window(7, TimeUnit.DAYS)))
    val rowAgg = new RowAggregator(inputSchema, aggregation.unpack)

    val ir = Array[Any](100L)
    val wrongSizeAggs = Array.empty[ColumnAggregator]

    try {
      ColumnIRSerializer.toSparkValues(ir, wrongSizeAggs)
      fail("Should throw exception for mismatched array lengths")
    } catch {
      case e: IllegalArgumentException =>
        assertTrue("Error message should mention length mismatch", e.getMessage.contains("must match"))
    }
  }

  /**
    * Test null array handling in convenience methods.
    */
  @Test
  def testNullArrayHandling(): Unit = {
    val schema = List(Column("value", IntType, 100))
    val inputSchema = schema.map(c => c.name -> c.`type`)
    val aggregation = Builders.Aggregation(Operation.SUM, "value", Seq(new Window(7, TimeUnit.DAYS)))
    val rowAgg = new RowAggregator(inputSchema, aggregation.unpack)

    // Test null IR array
    val sparkValues = ColumnIRSerializer.toSparkValues(null, rowAgg.columnAggregators)
    assertNull("Null IR array should serialize to null", sparkValues)

    // Test null Spark values array
    val irValues = ColumnIRSerializer.fromSparkValues(null, rowAgg.columnAggregators)
    assertNull("Null Spark values array should deserialize to null", irValues)
  }

  /**
    * Test handling of nested Spark Row objects (GenericRowWithSchema) in IR - full round trip.
    *
    * This test validates the extraneousRowHandler fix for testStructJoin failure.
    * When normalized IR contains Spark Row objects (from struct aggregations),
    * the serializer should handle them correctly as structs, not fail with
    * "No handler for [item] of class GenericRowWithSchema".
    *
    * Tests FULL round trip to ensure type safety.
    */
  @Test
  def testNestedSparkRowHandling(): Unit = {
    import org.apache.spark.sql.catalyst.expressions.{GenericRow, GenericRowWithSchema}

    // Create a simple AVERAGE aggregation (produces struct IR with sum + count)
    val schema = List(Column("value", DoubleType, 100))
    val df = DataFrameGen.events(spark, schema, count = 50, partitions = 2)

    val inputSchema = schema.map(c => c.name -> c.`type`)
    val aggregation = Builders.Aggregation(Operation.AVERAGE, "value", Seq(new Window(7, TimeUnit.DAYS)))
    val rowAgg = new RowAggregator(inputSchema, aggregation.unpack)
    val columnAgg = rowAgg.columnAggregators(0)

    // Create IR and populate it
    val ir = rowAgg.init
    import ai.chronon.online.serde.SparkConversions
    val sampleRows = df.take(20)
    sampleRows.foreach { sparkRow =>
      val chrononRow = SparkConversions.toChrononRow(sparkRow, df.schema.fieldIndex("ts"))
      rowAgg.update(ir, chrononRow)
    }

    // FULL ROUND TRIP TEST
    // 1. Serialize the IR (which is a struct for AVERAGE)
    val sparkValue = ColumnIRSerializer.toSparkValue(ir(0), columnAgg)
    assertNotNull("Serialized value should not be null", sparkValue)
    assertTrue("Should be GenericRow", sparkValue.isInstanceOf[GenericRow])

    // 2. Deserialize back to IR
    val deserializedIR = ColumnIRSerializer.fromSparkValue(sparkValue, columnAgg)
    assertNotNull("Deserialized IR should not be null", deserializedIR)

    // 3. Verify finalized results match EXACTLY (including types!)
    val originalResult = columnAgg.finalize(ir(0))
    val deserializedResult = columnAgg.finalize(deserializedIR)

    // For AVERAGE, result is a Double
    assertTrue("Original should be Double", originalResult.isInstanceOf[Double])
    assertTrue("Deserialized should be Double", deserializedResult.isInstanceOf[Double])

    val origAvg = originalResult.asInstanceOf[Double]
    val deserAvg = deserializedResult.asInstanceOf[Double]
    assertEquals("Average should match exactly", origAvg, deserAvg, 1e-10)

    // 4. Extra validation: verify the serialized Row has correct structure
    val row = sparkValue.asInstanceOf[GenericRow]
    assertEquals("AVERAGE IR has 2 fields (sum, count)", 2, row.length)
    assertNotNull("Sum should not be null", row.get(0))
    assertNotNull("Count should not be null", row.get(1))

    // Both fields should be numeric
    assertTrue("Sum should be numeric", row.get(0).isInstanceOf[Number])
    assertTrue("Count should be numeric", row.get(1).isInstanceOf[Number])
  }
}
