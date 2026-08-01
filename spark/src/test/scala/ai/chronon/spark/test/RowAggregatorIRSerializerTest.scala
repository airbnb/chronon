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

import ai.chronon.aggregator.row.RowAggregator
import ai.chronon.aggregator.test.Column
import ai.chronon.api.Extensions._
import ai.chronon.api._
import ai.chronon.online.serde.SparkConversions
import ai.chronon.spark.serde.RowAggregatorIRSerializer
import ai.chronon.spark.SparkSessionBuilder
import junit.framework.TestCase
import org.apache.spark.sql.SparkSession
import org.junit.Assert._
import org.junit.Test

import scala.collection.Seq
import scala.collection.JavaConverters._
import scala.util.Random

/**
  * Comprehensive tests for RowAggregatorIRSerializer - row-level IR array serialization.
  *
  * Tests cover:
  * 1. Basic aggregations (SUM, AVERAGE, COUNT) in a single RowAggregator
  * 2. Complex aggregations (HISTOGRAM, LAST_K, APPROX_PERCENTILE)
  * 3. Bucketed aggregations
  * 4. Null handling
  * 5. Schema generation
  * 6. Both API signatures (columnAggs array and RowAggregator)
  * 7. Mixed types in single row
  */
class RowAggregatorIRSerializerTest extends TestCase {

  lazy val spark: SparkSession =
    SparkSessionBuilder.build("RowAggregatorIRSerializerTest_" + Random.alphanumeric.take(6).mkString, local = true)

  @Test
  def testBasicAggregationsRoundTrip(): Unit = {
    val schema = List(
      Column("value", LongType, 100),
      Column("score", DoubleType, 100),
      Column("category", StringType, 10)
    )
    val df = DataFrameGen.events(spark, schema, count = 50, partitions = 2)

    // Create multiple aggregations
    val aggregations = Seq(
      Builders.Aggregation(Operation.SUM, "value", Seq(new Window(7, TimeUnit.DAYS))),
      Builders.Aggregation(Operation.AVERAGE, "score", Seq(new Window(7, TimeUnit.DAYS))),
      Builders.Aggregation(Operation.COUNT, "category", Seq(new Window(7, TimeUnit.DAYS)))
    )

    val inputSchema = schema.map(c => c.name -> c.`type`)
    val rowAgg = new RowAggregator(inputSchema, aggregations.flatMap(_.unpack))

    // Create and populate IR array
    val irArray = rowAgg.init
    val sampleRows = df.take(20)
    sampleRows.foreach { sparkRow =>
      val chrononRow = SparkConversions.toChrononRow(sparkRow, df.schema.fieldIndex("ts"))
      rowAgg.update(irArray, chrononRow)
    }

    // Serialize to Spark Row
    val sparkRow = RowAggregatorIRSerializer.toSparkRow(irArray, rowAgg)
    assertNotNull("Serialized row should not be null", sparkRow)
    assertEquals("Row should have correct number of columns", irArray.length, sparkRow.length)

    // Deserialize back to IR array
    val deserializedIR = RowAggregatorIRSerializer.fromSparkRow(sparkRow, rowAgg)
    assertNotNull("Deserialized IR should not be null", deserializedIR)
    assertEquals("Deserialized IR should have correct length", irArray.length, deserializedIR.length)

    // Verify all finalized values match
    for (i <- irArray.indices) {
      val originalResult = rowAgg.columnAggregators(i).finalize(irArray(i))
      val deserializedResult = rowAgg.columnAggregators(i).finalize(deserializedIR(i))
      assertEquals(s"Result for column $i should match", originalResult, deserializedResult)
    }
  }

  @Test
  def testComplexAggregationsRoundTrip(): Unit = {
    val schema = List(
      Column("category", StringType, 10),
      Column("value", LongType, 100),
      Column("score", FloatType, 100)
    )
    val df = DataFrameGen.events(spark, schema, count = 50, partitions = 2)

    val aggregations = Seq(
      Builders.Aggregation(Operation.HISTOGRAM, "category", Seq(new Window(7, TimeUnit.DAYS))),
      Builders.Aggregation(Operation.LAST_K, "value", Seq(new Window(7, TimeUnit.DAYS)), Map("k" -> "3")),
      Builders.Aggregation(Operation.APPROX_PERCENTILE,
                           "score",
                           Seq(new Window(7, TimeUnit.DAYS)),
                           Map("k" -> "128", "percentiles" -> "[0.5, 0.9]"))
    )

    val inputSchema = schema.map(c => c.name -> c.`type`)
    val rowAgg = new RowAggregator(inputSchema, aggregations.flatMap(_.unpack))

    // Create and populate IR array
    val irArray = rowAgg.init
    val sampleRows = df.take(30)
    sampleRows.foreach { sparkRow =>
      val chrononRow = SparkConversions.toChrononRow(sparkRow, df.schema.fieldIndex("ts"))
      rowAgg.update(irArray, chrononRow)
    }

    // Serialize and deserialize
    val sparkRow = RowAggregatorIRSerializer.toSparkRow(irArray, rowAgg)
    val deserializedIR = RowAggregatorIRSerializer.fromSparkRow(sparkRow, rowAgg)

    // Verify HISTOGRAM
    val histOriginal = rowAgg.columnAggregators(0).finalize(irArray(0)).asInstanceOf[java.util.Map[String, _]]
    val histDeserialized =
      rowAgg.columnAggregators(0).finalize(deserializedIR(0)).asInstanceOf[java.util.Map[String, _]]
    assertEquals("HISTOGRAM should have same size", histOriginal.size(), histDeserialized.size())
    import scala.collection.JavaConverters._
    histOriginal.keySet().asScala.foreach { key =>
      assertEquals(s"HISTOGRAM[$key] should match", histOriginal.get(key), histDeserialized.get(key))
    }

    // Verify LAST_K
    val lastKOriginal = rowAgg.columnAggregators(1).finalize(irArray(1)).asInstanceOf[java.util.ArrayList[_]].asScala
    val lastKDeserialized =
      rowAgg.columnAggregators(1).finalize(deserializedIR(1)).asInstanceOf[java.util.ArrayList[_]].asScala
    assertEquals("LAST_K should have same size", lastKOriginal.size, lastKDeserialized.size)
    assertEquals("LAST_K values should match", lastKOriginal, lastKDeserialized)

    // Verify APPROX_PERCENTILE
    val percOriginal = rowAgg.columnAggregators(2).finalize(irArray(2)).asInstanceOf[Array[Float]]
    val percDeserialized = rowAgg.columnAggregators(2).finalize(deserializedIR(2)).asInstanceOf[Array[Float]]
    assertEquals("APPROX_PERCENTILE should have same length", percOriginal.length, percDeserialized.length)
    for (i <- percOriginal.indices) {
      assertEquals(s"APPROX_PERCENTILE[$i] should match", percOriginal(i), percDeserialized(i), 0.1f)
    }
  }

  @Test
  def testBucketedAggregationsRoundTrip(): Unit = {
    val schema = List(
      Column("category", StringType, 5),
      Column("value", LongType, 100)
    )
    val df = DataFrameGen.events(spark, schema, count = 50, partitions = 2)

    val aggregations = Seq(
      Builders.Aggregation(Operation.SUM, "value", Seq(new Window(7, TimeUnit.DAYS)), buckets = Seq("category"))
    )

    val inputSchema = schema.map(c => c.name -> c.`type`)
    val rowAgg = new RowAggregator(inputSchema, aggregations.flatMap(_.unpack))

    // Create and populate IR array
    val irArray = rowAgg.init
    val sampleRows = df.take(30)
    sampleRows.foreach { sparkRow =>
      val chrononRow = SparkConversions.toChrononRow(sparkRow, df.schema.fieldIndex("ts"))
      rowAgg.update(irArray, chrononRow)
    }

    // Serialize and deserialize
    val sparkRow = RowAggregatorIRSerializer.toSparkRow(irArray, rowAgg)
    val deserializedIR = RowAggregatorIRSerializer.fromSparkRow(sparkRow, rowAgg)

    // Verify bucketed results
    val bucketedOriginal = rowAgg.columnAggregators(0).finalize(irArray(0)).asInstanceOf[java.util.Map[String, _]]
    val bucketedDeserialized =
      rowAgg.columnAggregators(0).finalize(deserializedIR(0)).asInstanceOf[java.util.Map[String, _]]

    assertEquals("Bucketed result should have same size", bucketedOriginal.size(), bucketedDeserialized.size())
    import scala.collection.JavaConverters._
    bucketedOriginal.keySet().asScala.foreach { bucket =>
      assertEquals(s"Bucket '$bucket' should match", bucketedOriginal.get(bucket), bucketedDeserialized.get(bucket))
    }
  }

  @Test
  def testNullHandling(): Unit = {
    val schema = List(Column("value", LongType, 100))
    val aggregations = Seq(
      Builders.Aggregation(Operation.SUM, "value", Seq(new Window(7, TimeUnit.DAYS)))
    )

    val inputSchema = schema.map(c => c.name -> c.`type`)
    val rowAgg = new RowAggregator(inputSchema, aggregations.flatMap(_.unpack))

    // Create empty IR array (all nulls)
    val irArray = rowAgg.init

    // Serialize to Spark Row (without any updates, IR values are null)
    val sparkRow = RowAggregatorIRSerializer.toSparkRow(irArray, rowAgg)
    assertNotNull("Serialized row should not be null", sparkRow)

    // Deserialize back to IR array
    val deserializedIR = RowAggregatorIRSerializer.fromSparkRow(sparkRow, rowAgg)
    assertNotNull("Deserialized IR should not be null", deserializedIR)

    // Verify all IR values are still null
    for (i <- irArray.indices) {
      assertEquals(s"IR[$i] should be null", null, deserializedIR(i))
    }
  }

  @Test
  def testSchemaGeneration(): Unit = {
    val schema = List(
      Column("value", LongType, 100),
      Column("score", DoubleType, 100),
      Column("category", StringType, 10)
    )

    val aggregations = Seq(
      Builders.Aggregation(Operation.SUM, "value", Seq(new Window(7, TimeUnit.DAYS))),
      Builders.Aggregation(Operation.AVERAGE, "score", Seq(new Window(7, TimeUnit.DAYS))),
      Builders.Aggregation(Operation.COUNT, "category", Seq(new Window(7, TimeUnit.DAYS)))
    )

    val inputSchema = schema.map(c => c.name -> c.`type`)
    val rowAgg = new RowAggregator(inputSchema, aggregations.flatMap(_.unpack))

    // Generate schema
    val sparkSchema = RowAggregatorIRSerializer.schema(rowAgg)
    assertNotNull("Schema should not be null", sparkSchema)
    assertEquals("Schema should have correct number of fields",
                 rowAgg.columnAggregators.length,
                 sparkSchema.fields.length)

    // Verify all fields are nullable
    assertTrue("All fields should be nullable", sparkSchema.fields.forall(_.nullable))
  }

  @Test
  def testColumnAggsArraySignature(): Unit = {
    val schema = List(Column("value", LongType, 100))
    val df = DataFrameGen.events(spark, schema, count = 30, partitions = 2)

    val aggregations = Seq(
      Builders.Aggregation(Operation.SUM, "value", Seq(new Window(7, TimeUnit.DAYS)))
    )

    val inputSchema = schema.map(c => c.name -> c.`type`)
    val rowAgg = new RowAggregator(inputSchema, aggregations.flatMap(_.unpack))

    // Create and populate IR array
    val irArray = rowAgg.init
    val sampleRows = df.take(20)
    sampleRows.foreach { sparkRow =>
      val chrononRow = SparkConversions.toChrononRow(sparkRow, df.schema.fieldIndex("ts"))
      rowAgg.update(irArray, chrononRow)
    }

    // Test using columnAggs array directly (not RowAggregator)
    val sparkRow = RowAggregatorIRSerializer.toSparkRow(irArray, rowAgg.columnAggregators)
    assertNotNull("Serialized row should not be null", sparkRow)

    val deserializedIR = RowAggregatorIRSerializer.fromSparkRow(sparkRow, rowAgg.columnAggregators)
    assertNotNull("Deserialized IR should not be null", deserializedIR)

    // Verify result
    val originalResult = rowAgg.columnAggregators(0).finalize(irArray(0))
    val deserializedResult = rowAgg.columnAggregators(0).finalize(deserializedIR(0))
    assertEquals("Results should match", originalResult, deserializedResult)

    // Test schema generation with columnAggs array
    val schema1 = RowAggregatorIRSerializer.schema(rowAgg.columnAggregators)
    assertNotNull("Schema should not be null", schema1)
    assertEquals("Schema should have 1 field", 1, schema1.fields.length)
  }

  @Test
  def testMixedTypesInSingleRow(): Unit = {
    val schema = List(
      Column("int_val", IntType, 100),
      Column("long_val", LongType, 100),
      Column("double_val", DoubleType, 100),
      Column("string_val", StringType, 20)
    )
    val df = DataFrameGen.events(spark, schema, count = 50, partitions = 2)

    val aggregations = Seq(
      Builders.Aggregation(Operation.SUM, "int_val", Seq(new Window(7, TimeUnit.DAYS))),
      Builders.Aggregation(Operation.SUM, "long_val", Seq(new Window(7, TimeUnit.DAYS))),
      Builders.Aggregation(Operation.AVERAGE, "double_val", Seq(new Window(7, TimeUnit.DAYS))),
      Builders.Aggregation(Operation.LAST, "string_val", Seq(new Window(7, TimeUnit.DAYS)))
    )

    val inputSchema = schema.map(c => c.name -> c.`type`)
    val rowAgg = new RowAggregator(inputSchema, aggregations.flatMap(_.unpack))

    // Create and populate IR array
    val irArray = rowAgg.init
    val sampleRows = df.take(30)
    sampleRows.foreach { sparkRow =>
      val chrononRow = SparkConversions.toChrononRow(sparkRow, df.schema.fieldIndex("ts"))
      rowAgg.update(irArray, chrononRow)
    }

    // Serialize and deserialize
    val sparkRow = RowAggregatorIRSerializer.toSparkRow(irArray, rowAgg)
    val deserializedIR = RowAggregatorIRSerializer.fromSparkRow(sparkRow, rowAgg)

    // Verify all types work correctly
    for (i <- irArray.indices) {
      val originalResult = rowAgg.columnAggregators(i).finalize(irArray(i))
      val deserializedResult = rowAgg.columnAggregators(i).finalize(deserializedIR(i))

      // For AVERAGE, compare with tolerance
      if (i == 2) {
        assertEquals(s"Result for column $i (AVERAGE) should match",
                     originalResult.asInstanceOf[Double],
                     deserializedResult.asInstanceOf[Double],
                     0.0001)
      } else {
        assertEquals(s"Result for column $i should match", originalResult, deserializedResult)
      }
    }
  }
}
