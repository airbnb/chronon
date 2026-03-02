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
import ai.chronon.spark.serde.ColumnIRSerializer
import ai.chronon.spark.{GroupBy, SparkSessionBuilder}
import junit.framework.TestCase
import org.apache.spark.sql.SparkSession
import org.junit.Assert._
import org.junit.Test

import scala.util.Random

/**
  * Comprehensive fuzz tests for ColumnIRSerializer.
  *
  * Tests all operation types with random data to ensure:
  * 1. No crashes on diverse inputs
  * 2. Round-trip correctness for all operations
  * 3. Proper handling of edge cases
  * 4. Consistency across different data patterns
  */
class ColumnIRSerializerFuzzTest extends TestCase {

  lazy val spark: SparkSession =
    SparkSessionBuilder.build("ColumnIRSerializerFuzz_" + Random.alphanumeric.take(6).mkString, local = true)

  /**
    * Test all basic operations (non-deletable monoids).
    */
  @Test
  def testMonoidOperationsFuzz(): Unit = {
    val operations = Seq(
      Operation.MIN,
      Operation.MAX,
      Operation.FIRST,
      Operation.LAST,
      Operation.UNIQUE_COUNT,
      Operation.APPROX_UNIQUE_COUNT
    )

    val schema = List(
      Column("int_val", IntType, 100),
      Column("double_val", DoubleType, 100),
      Column("string_val", StringType, 50)
    )

    operations.foreach { op =>
      println(s"\nTesting monoid operation: $op")

      // Test with different column types
      val testColumns = if (op == Operation.UNIQUE_COUNT || op == Operation.APPROX_UNIQUE_COUNT) {
        Seq("string_val") // These work on any type, use string for variety
      } else {
        Seq("int_val", "double_val")
      }

      testColumns.foreach { column =>
        val inputSchema = schema.map(c => c.name -> c.`type`)
        val aggregation = Builders.Aggregation(op, column, Seq(new Window(7, TimeUnit.DAYS)))
        val rowAgg = new RowAggregator(inputSchema, aggregation.unpack)
        val columnAgg = rowAgg.columnAggregators(0)

        val df = DataFrameGen.events(spark, schema, count = 100, partitions = 5)
        val sampleRows = df.take(50)

        // Build up IR with random data
        val irArray = rowAgg.init
        import ai.chronon.online.serde.SparkConversions
        sampleRows.foreach { sparkRow =>
          val chrononRow = SparkConversions.toChrononRow(sparkRow, df.schema.fieldIndex("ts"))
          rowAgg.update(irArray, chrononRow)
        }

        // Serialize and deserialize
        val sparkValue = ColumnIRSerializer.toSparkValue(irArray(0), columnAgg)
        assertNotNull(s"[$op:$column] Spark value should not be null", sparkValue)

        val deserializedIR = ColumnIRSerializer.fromSparkValue(sparkValue, columnAgg)
        assertNotNull(s"[$op:$column] Deserialized IR should not be null", deserializedIR)

        // Verify finalized results match
        val originalResult = columnAgg.finalize(irArray(0))
        val deserializedResult = columnAgg.finalize(deserializedIR)

        (originalResult, deserializedResult) match {
          case (null, null) => // OK
          case (o: Double, d: Double) =>
            val diff = Math.abs(o - d)
            assertTrue(s"[$op:$column] Results should match: $o vs $d", diff < 1e-10)
          case (o: Float, d: Float) =>
            val diff = Math.abs(o - d)
            assertTrue(s"[$op:$column] Results should match: $o vs $d", diff < 1e-6f)
          case (o, d) =>
            assertEquals(s"[$op:$column] Results should match", o, d)
        }

        println(s"  ✓ $op on $column passed")
      }
    }
  }

  /**
    * Test all deletable operations (abelian groups).
    */
  @Test
  def testAbelianGroupOperationsFuzz(): Unit = {
    val operations = Seq(
      Operation.COUNT,
      Operation.SUM,
      Operation.AVERAGE,
      Operation.VARIANCE,
      Operation.SKEW,
      Operation.KURTOSIS
    )

    val schema = List(
      Column("int_val", IntType, 1000),
      Column("double_val", DoubleType, 1000)
    )

    operations.foreach { op =>
      println(s"\nTesting abelian group operation: $op")

      val testColumns = if (op == Operation.COUNT) {
        Seq("int_val") // COUNT works on any type
      } else {
        Seq("int_val", "double_val")
      }

      testColumns.foreach { column =>
        val inputSchema = schema.map(c => c.name -> c.`type`)
        val aggregation = Builders.Aggregation(op, column, Seq(new Window(7, TimeUnit.DAYS)))
        val rowAgg = new RowAggregator(inputSchema, aggregation.unpack)
        val columnAgg = rowAgg.columnAggregators(0)

        val df = DataFrameGen.events(spark, schema, count = 200, partitions = 10)
        val sampleRows = df.take(100)

        // Build up IR with random data
        val irArray = rowAgg.init

        import ai.chronon.online.serde.SparkConversions
        sampleRows.foreach { sparkRow =>
          val chrononRow = SparkConversions.toChrononRow(sparkRow, df.schema.fieldIndex("ts"))
          rowAgg.update(irArray, chrononRow)
        }

        // Serialize and deserialize
        val sparkValue = ColumnIRSerializer.toSparkValue(irArray(0), columnAgg)
        assertNotNull(s"[$op:$column] Spark value should not be null", sparkValue)

        val deserializedIR = ColumnIRSerializer.fromSparkValue(sparkValue, columnAgg)
        assertNotNull(s"[$op:$column] Deserialized IR should not be null", deserializedIR)

        // Verify finalized results match (allowing for floating point error)
        val originalResult = columnAgg.finalize(irArray(0))
        val deserializedResult = columnAgg.finalize(deserializedIR)

        (originalResult, deserializedResult) match {
          case (null, null) => // OK
          case (o: Double, d: Double) =>
            val relDiff = if (o != 0.0) Math.abs((o - d) / o) else Math.abs(d)
            assertTrue(s"[$op:$column] Results should match: $o vs $d (rel diff: $relDiff)",
                       relDiff < 1e-10 || Math.abs(o - d) < 1e-10)
          case (o, d) =>
            assertEquals(s"[$op:$column] Results should match", o, d)
        }

        println(s"  ✓ $op on $column passed")
      }
    }
  }

  /**
    * Test K-based operations with different K values.
    */
  @Test
  def testKBasedOperationsFuzz(): Unit = {
    val operations = Seq(
      Operation.LAST_K,
      Operation.FIRST_K,
      Operation.TOP_K,
      Operation.BOTTOM_K
    )

    val kValues = Seq(1, 3, 5, 10, 20)

    val schema = List(
      Column("int_val", IntType, 1000),
      Column("double_val", DoubleType, 1000)
    )

    operations.foreach { op =>
      println(s"\nTesting K-based operation: $op")

      kValues.foreach { k =>
        val column = if (op == Operation.TOP_K || op == Operation.BOTTOM_K) {
          "double_val" // These need numeric types
        } else {
          "int_val"
        }

        val inputSchema = schema.map(c => c.name -> c.`type`)
        val aggregation = Builders.Aggregation(
          op,
          column,
          Seq(new Window(7, TimeUnit.DAYS)),
          argMap = Map("k" -> k.toString)
        )
        val rowAgg = new RowAggregator(inputSchema, aggregation.unpack)
        val columnAgg = rowAgg.columnAggregators(0)

        val df = DataFrameGen.events(spark, schema, count = 150, partitions = 5)
        val sampleRows = df.take(50)

        // Build up IR
        val irArray = rowAgg.init
        import ai.chronon.online.serde.SparkConversions
        sampleRows.foreach { sparkRow =>
          val chrononRow = SparkConversions.toChrononRow(sparkRow, df.schema.fieldIndex("ts"))
          rowAgg.update(irArray, chrononRow)
        }

        // Serialize and deserialize
        val sparkValue = ColumnIRSerializer.toSparkValue(irArray(0), columnAgg)
        assertNotNull(s"[$op:k=$k] Spark value should not be null", sparkValue)

        val deserializedIR = ColumnIRSerializer.fromSparkValue(sparkValue, columnAgg)
        assertNotNull(s"[$op:k=$k] Deserialized IR should not be null", deserializedIR)

        // Verify results
        import scala.collection.JavaConverters._
        val originalResult = columnAgg.finalize(irArray(0)).asInstanceOf[java.util.ArrayList[_]].asScala
        val deserializedResult = columnAgg.finalize(deserializedIR).asInstanceOf[java.util.ArrayList[_]].asScala

        assertEquals(s"[$op:k=$k] Array lengths should match", originalResult.size, deserializedResult.size)
        assertTrue(s"[$op:k=$k] Array length should be <= k", originalResult.size <= k)

        // Verify array contents match
        originalResult.zip(deserializedResult).foreach {
          case (o, d) =>
            assertEquals(s"[$op:k=$k] Array elements should match", o, d)
        }

        println(s"  ✓ $op with k=$k passed")
      }
    }
  }

  /**
    * Test HISTOGRAM and APPROX_HISTOGRAM_K operations.
    */
  @Test
  def testHistogramOperationsFuzz(): Unit = {
    val schema = List(
      Column("category", StringType, 20),
      Column("value", IntType, 100)
    )

    val inputSchema = schema.map(c => c.name -> c.`type`)

    // Test HISTOGRAM
    println("\nTesting HISTOGRAM operation")
    val histAgg = Builders.Aggregation(Operation.HISTOGRAM, "category", Seq(new Window(7, TimeUnit.DAYS)))
    val histRowAgg = new RowAggregator(inputSchema, histAgg.unpack)
    val histColumnAgg = histRowAgg.columnAggregators(0)

    val df = DataFrameGen.events(spark, schema, count = 200, partitions = 10)
    val sampleRows = df.take(100)

    val histIRArray = histRowAgg.init
    import ai.chronon.online.serde.SparkConversions
    sampleRows.foreach { sparkRow =>
      val chrononRow = SparkConversions.toChrononRow(sparkRow, df.schema.fieldIndex("ts"))
      histRowAgg.update(histIRArray, chrononRow)
    }

    val histSparkValue = ColumnIRSerializer.toSparkValue(histIRArray(0), histColumnAgg)
    assertNotNull("HISTOGRAM Spark value should not be null", histSparkValue)
    assertTrue("HISTOGRAM should produce Map", histSparkValue.isInstanceOf[Map[_, _]])

    val histDeserializedIR = ColumnIRSerializer.fromSparkValue(histSparkValue, histColumnAgg)
    val histOrigResult = histColumnAgg.finalize(histIRArray(0)).asInstanceOf[java.util.Map[String, _]]
    val histDeserResult = histColumnAgg.finalize(histDeserializedIR).asInstanceOf[java.util.Map[String, _]]

    import scala.collection.JavaConverters._
    assertEquals("HISTOGRAM size should match", histOrigResult.size(), histDeserResult.size())
    histOrigResult.asScala.foreach {
      case (k, v) =>
        assertEquals(s"HISTOGRAM count for $k should match", v, histDeserResult.get(k))
    }
    println("  ✓ HISTOGRAM passed")

    // Test APPROX_HISTOGRAM_K
    println("\nTesting APPROX_HISTOGRAM_K operation")
    Seq(5, 10, 20).foreach { k =>
      val approxAgg = Builders.Aggregation(
        Operation.APPROX_HISTOGRAM_K,
        "category",
        Seq(new Window(7, TimeUnit.DAYS)),
        argMap = Map("k" -> k.toString)
      )
      val approxRowAgg = new RowAggregator(inputSchema, approxAgg.unpack)
      val approxColumnAgg = approxRowAgg.columnAggregators(0)

      val approxIRArray = approxRowAgg.init
      sampleRows.foreach { sparkRow =>
        val chrononRow = SparkConversions.toChrononRow(sparkRow, df.schema.fieldIndex("ts"))
        approxRowAgg.update(approxIRArray, chrononRow)
      }

      val approxSparkValue = ColumnIRSerializer.toSparkValue(approxIRArray(0), approxColumnAgg)
      assertNotNull(s"APPROX_HISTOGRAM_K (k=$k) Spark value should not be null", approxSparkValue)

      val approxDeserializedIR = ColumnIRSerializer.fromSparkValue(approxSparkValue, approxColumnAgg)
      val approxOrigResult = approxColumnAgg.finalize(approxIRArray(0)).asInstanceOf[java.util.Map[String, _]]
      val approxDeserResult = approxColumnAgg.finalize(approxDeserializedIR).asInstanceOf[java.util.Map[String, _]]

      assertTrue(s"APPROX_HISTOGRAM_K size should be <= k (k=$k)", approxOrigResult.size() <= k)
      assertEquals(s"APPROX_HISTOGRAM_K sizes should match (k=$k)", approxOrigResult.size(), approxDeserResult.size())

      println(s"  ✓ APPROX_HISTOGRAM_K with k=$k passed")
    }
  }

  /**
    * Test APPROX_PERCENTILE with different percentile configurations.
    */
  @Test
  def testApproxPercentileFuzz(): Unit = {
    println("\nTesting APPROX_PERCENTILE operation")

    val percentileConfigs = Seq(
      "[0.5]", // Single percentile
      "[0.5, 0.9]", // Two percentiles
      "[0.5, 0.9, 0.99]", // Three percentiles
      "[0.1, 0.25, 0.5, 0.75, 0.9]" // Five percentiles
    )

    val schema = List(
      Column("value", DoubleType, 1000)
    )

    percentileConfigs.foreach { percentiles =>
      val inputSchema = schema.map(c => c.name -> c.`type`)
      val aggregation = Builders.Aggregation(
        Operation.APPROX_PERCENTILE,
        "value",
        Seq(new Window(7, TimeUnit.DAYS)),
        argMap = Map("percentiles" -> percentiles)
      )
      val rowAgg = new RowAggregator(inputSchema, aggregation.unpack)
      val columnAgg = rowAgg.columnAggregators(0)

      val df = DataFrameGen.events(spark, schema, count = 500, partitions = 10)
      val sampleRows = df.take(200)

      // Build sketch with many values
      val irArray = rowAgg.init
      import ai.chronon.online.serde.SparkConversions
      sampleRows.foreach { sparkRow =>
        val chrononRow = SparkConversions.toChrononRow(sparkRow, df.schema.fieldIndex("ts"))
        rowAgg.update(irArray, chrononRow)
      }

      // Serialize and deserialize
      val sparkValue = ColumnIRSerializer.toSparkValue(irArray(0), columnAgg)
      assertNotNull(s"[$percentiles] Spark value should not be null", sparkValue)
      assertTrue(s"[$percentiles] Should be binary", sparkValue.isInstanceOf[Array[Byte]])

      val deserializedIR = ColumnIRSerializer.fromSparkValue(sparkValue, columnAgg)
      assertNotNull(s"[$percentiles] Deserialized IR should not be null", deserializedIR)

      // Verify percentiles (with tolerance for sketch approximation)
      val originalResult = columnAgg.finalize(irArray(0)).asInstanceOf[Array[Float]]
      val deserializedResult = columnAgg.finalize(deserializedIR).asInstanceOf[Array[Float]]

      assertEquals(s"[$percentiles] Array lengths should match", originalResult.length, deserializedResult.length)

      originalResult.zip(deserializedResult).zipWithIndex.foreach {
        case ((o, d), idx) =>
          val relDiff = if (o != 0.0f) Math.abs((o - d) / o) else Math.abs(d)
          assertTrue(s"[$percentiles] Percentile $idx should be close: $o vs $d (rel diff: $relDiff)",
                     relDiff < 0.05f
          ) // 5% tolerance for sketches
      }

      println(s"  ✓ APPROX_PERCENTILE with percentiles=$percentiles passed")
    }
  }

  /**
    * Test bucketed aggregations with different bucket cardinalities.
    */
  @Test
  def testBucketedAggregationsFuzz(): Unit = {
    println("\nTesting bucketed aggregations")

    val operations = Seq(Operation.SUM, Operation.COUNT, Operation.AVERAGE)
    val schema = List(
      Column("category", StringType, 10), // 10 unique categories
      Column("value", IntType, 1000)
    )

    operations.foreach { op =>
      val inputSchema = schema.map(c => c.name -> c.`type`)
      val column = if (op == Operation.COUNT) "category" else "value"
      val aggregation = Builders.Aggregation(
        op,
        column,
        Seq(new Window(7, TimeUnit.DAYS)),
        buckets = Seq("category")
      )
      val rowAgg = new RowAggregator(inputSchema, aggregation.unpack)
      val columnAgg = rowAgg.columnAggregators(0)

      val df = DataFrameGen.events(spark, schema, count = 200, partitions = 10)
      val sampleRows = df.take(100)

      // Build bucketed IR
      val irArray = rowAgg.init
      import ai.chronon.online.serde.SparkConversions
      sampleRows.foreach { sparkRow =>
        val chrononRow = SparkConversions.toChrononRow(sparkRow, df.schema.fieldIndex("ts"))
        rowAgg.update(irArray, chrononRow)
      }

      // Serialize and deserialize
      val sparkValue = ColumnIRSerializer.toSparkValue(irArray(0), columnAgg)
      assertNotNull(s"[Bucketed $op] Spark value should not be null", sparkValue)
      assertTrue(s"[Bucketed $op] Should be Map", sparkValue.isInstanceOf[Map[_, _]])

      val deserializedIR = ColumnIRSerializer.fromSparkValue(sparkValue, columnAgg)
      assertNotNull(s"[Bucketed $op] Deserialized IR should not be null", deserializedIR)

      // Verify bucketed results
      val originalResult = columnAgg.finalize(irArray(0)).asInstanceOf[java.util.Map[String, _]]
      val deserializedResult = columnAgg.finalize(deserializedIR).asInstanceOf[java.util.Map[String, _]]

      import scala.collection.JavaConverters._
      assertEquals(s"[Bucketed $op] Bucket counts should match", originalResult.size(), deserializedResult.size())
      originalResult.asScala.foreach {
        case (bucket, value) =>
          assertTrue(s"[Bucketed $op] Should contain bucket $bucket", deserializedResult.containsKey(bucket))
          val origVal = value
          val deserVal = deserializedResult.get(bucket)
          (origVal, deserVal) match {
            case (o: Double, d: Double) =>
              val relDiff = if (o != 0.0) Math.abs((o - d) / o) else Math.abs(d)
              assertTrue(s"[Bucketed $op] Values for bucket $bucket should match: $o vs $d", relDiff < 1e-10)
            case _ =>
              assertEquals(s"[Bucketed $op] Values for bucket $bucket should match", origVal, deserVal)
          }
      }

      println(s"  ✓ Bucketed $op passed")
    }
  }

  /**
    * Comprehensive fuzz test: random combinations of operations.
    */
  @Test
  def testRandomOperationCombinationsFuzz(): Unit = {
    println("\nTesting random operation combinations (fuzz)")

    val random = new Random(12345) // Fixed seed for reproducibility
    val schema = List(
      Column("string_val", StringType, 30),
      Column("int_val", IntType, 1000),
      Column("double_val", DoubleType, 1000)
    )

    val testableOps = Seq(
      Operation.MIN,
      Operation.MAX,
      Operation.FIRST,
      Operation.LAST,
      Operation.COUNT,
      Operation.SUM,
      Operation.AVERAGE,
      Operation.UNIQUE_COUNT,
      Operation.APPROX_UNIQUE_COUNT,
      Operation.HISTOGRAM,
      Operation.LAST_K,
      Operation.FIRST_K
    )

    // Run 30 fuzz iterations
    (0 until 30).foreach { iteration =>
      val op = testableOps(random.nextInt(testableOps.length))
      val column = op match {
        case Operation.SUM | Operation.AVERAGE => Seq("int_val", "double_val")(random.nextInt(2))
        case Operation.HISTOGRAM               => "string_val"
        case _                                 => Seq("string_val", "int_val", "double_val")(random.nextInt(3))
      }

      val argMap = op match {
        case Operation.LAST_K | Operation.FIRST_K => Map("k" -> s"${3 + random.nextInt(5)}")
        case _                                    => Map.empty[String, String]
      }

      val inputSchema = schema.map(c => c.name -> c.`type`)
      val aggregation = Builders.Aggregation(
        op,
        column,
        Seq(new Window(7, TimeUnit.DAYS)),
        argMap = argMap
      )
      val rowAgg = new RowAggregator(inputSchema, aggregation.unpack)
      val columnAgg = rowAgg.columnAggregators(0)

      val df = DataFrameGen.events(spark, schema, count = 100, partitions = 5)
      val sampleRows = df.take(30)

      // Build IR
      val irArray = rowAgg.init
      import ai.chronon.online.serde.SparkConversions
      sampleRows.foreach { sparkRow =>
        val chrononRow = SparkConversions.toChrononRow(sparkRow, df.schema.fieldIndex("ts"))
        rowAgg.update(irArray, chrononRow)
      }

      // Serialize and deserialize
      val sparkValue = ColumnIRSerializer.toSparkValue(irArray(0), columnAgg)
      assertNotNull(s"[Fuzz $iteration: $op:$column] Spark value should not be null", sparkValue)

      val deserializedIR = ColumnIRSerializer.fromSparkValue(sparkValue, columnAgg)
      assertNotNull(s"[Fuzz $iteration: $op:$column] Deserialized IR should not be null", deserializedIR)

      // Just verify no crashes - detailed correctness checked in other tests
      val originalResult = columnAgg.finalize(irArray(0))
      val deserializedResult = columnAgg.finalize(deserializedIR)
      assertNotNull(s"[Fuzz $iteration: $op:$column] Results should not be null", originalResult)
      assertNotNull(s"[Fuzz $iteration: $op:$column] Results should not be null", deserializedResult)

      if (iteration % 10 == 0) {
        println(s"  ✓ Completed $iteration fuzz iterations")
      }
    }

    println(s"  ✓ All 30 fuzz iterations passed")
  }

  /**
    * Test FIRST and LAST operations against randomized value schemas.
    *
    * Exercises list-typed payloads through the serialization path to verify
    * that complex value types round-trip correctly through ColumnIRSerializer.
    *
    * Proves functional equivalence by:
    * 1. Round-trip: finalize(deserialize(serialize(IR))) == finalize(IR)
    * 2. Merge after round-trip: split rows in half, serialize/deserialize the first half's IR,
    *    merge with second half, compare against directly computing from all rows.
    */
  @Test
  def testFirstLastWithComplexValueSchemas(): Unit = {
    println("\nTesting FIRST/LAST with complex value schemas")

    val operations = Seq(Operation.FIRST, Operation.LAST)

    // Schemas for the FIRST/LAST value column ("complex_col"), paired with a human-readable
    // label that appears in assertion messages (e.g. "FIRST:List[Int] round-trip").
    // Each schema also includes an "int_val" column so the DataFrame has more than one field.
    //
    // List schemas exercise the basic collection path. Struct and Map schemas exercise deeper
    // serialization: nested Row.buildToConverter traversal, extraneousRowHandler for Spark Rows,
    // and recursive map conversion. The final nested-struct schema combines all three.
    val complexSchemas: Seq[(String, List[Column])] = Seq(
      ("List[Int]",
       List(
         Column("complex_col", ListType(IntType), 50, chunkSize = 5),
         Column("int_val", IntType, 100)
       )),
      ("List[Double]",
       List(
         Column("complex_col", ListType(DoubleType), 50, chunkSize = 5),
         Column("int_val", IntType, 100)
       )),
      ("List[String]",
       List(
         Column("complex_col", ListType(StringType), 20, chunkSize = 3),
         Column("int_val", IntType, 100)
       )),
      ("List[Long]",
       List(
         Column("complex_col", ListType(LongType), 50, chunkSize = 5),
         Column("int_val", IntType, 100)
       ))
      // Note: Struct and Map column schemas are omitted here because DataFrameGen.events +
      // df.take() doesn't support Spark's catalyst encoding for nested struct columns.
      // Struct IR serialization is already covered by testNestedSparkRowHandling (AVERAGE
      // produces struct IR) and the map path is covered by testHistogramColumnRoundTrip.
    )

    import scala.collection.JavaConverters._

    /** Normalize and deeply compare two values that may differ in container types after round-trip. */
    def deepEquals(a: Any, b: Any, label: String): Unit =
      (a, b) match {
        case (null, null) => // OK
        case (null, _)    => fail(s"[$label] expected null but got ${b.getClass}: $b")
        case (_, null)    => fail(s"[$label] expected ${a.getClass}: $a but got null")
        case (ad: Double, bd: Double) =>
          assertTrue(s"[$label] Doubles differ: $ad vs $bd", Math.abs(ad - bd) < 1e-10)
        case (af: Float, bf: Float) =>
          assertTrue(s"[$label] Floats differ: $af vs $bf", Math.abs(af - bf) < 1e-6f)
        case _ =>
          // Normalize both sides to Seq for ordered collections
          val aSeq = toNormalizedSeq(a)
          val bSeq = toNormalizedSeq(b)
          if (aSeq.isDefined && bSeq.isDefined) {
            assertEquals(s"[$label] Seq sizes differ", aSeq.get.size, bSeq.get.size)
            aSeq.get.zip(bSeq.get).zipWithIndex.foreach {
              case ((ae, be), idx) =>
                deepEquals(ae, be, s"$label[$idx]")
            }
          } else {
            // Try normalized map comparison
            val aMap = toNormalizedMap(a)
            val bMap = toNormalizedMap(b)
            if (aMap.isDefined && bMap.isDefined) {
              assertEquals(s"[$label] Map sizes differ", aMap.get.size, bMap.get.size)
              aMap.get.foreach {
                case (k, v) =>
                  assertTrue(s"[$label] Missing key $k", bMap.get.contains(k))
                  deepEquals(v, bMap.get(k), s"$label{$k}")
              }
            } else {
              assertEquals(s"[$label] Values differ", a, b)
            }
          }
      }

    def toNormalizedSeq(v: Any): Option[Seq[Any]] =
      v match {
        case al: java.util.ArrayList[_]  => Some(al.asScala.toSeq.asInstanceOf[Seq[Any]])
        case s: Seq[_]                   => Some(s.asInstanceOf[Seq[Any]])
        case a: Array[_]                 => Some(a.toSeq.asInstanceOf[Seq[Any]])
        case r: org.apache.spark.sql.Row => Some(r.toSeq.asInstanceOf[Seq[Any]])
        case _                           => None
      }

    def toNormalizedMap(v: Any): Option[Map[Any, Any]] =
      v match {
        case jm: java.util.Map[_, _]        => Some(jm.asScala.toMap.asInstanceOf[Map[Any, Any]])
        case sm: scala.collection.Map[_, _] => Some(sm.toMap.asInstanceOf[Map[Any, Any]])
        case _                              => None
      }

    operations.foreach { op =>
      complexSchemas.foreach {
        case (schemaName, schema) =>
          val label = s"$op:$schemaName"
          val inputSchema = schema.map(c => c.name -> c.`type`)
          val aggregation = Builders.Aggregation(op, "complex_col", Seq(new Window(7, TimeUnit.DAYS)))

          val df = DataFrameGen.events(spark, schema, count = 100, partitions = 5)
          val allRows = df.take(80)
          val firstHalf = allRows.take(40)
          val secondHalf = allRows.drop(40)

          import ai.chronon.online.serde.SparkConversions
          val tsIdx = df.schema.fieldIndex("ts")

          // --- Path A: build IR from first half, serialize, deserialize, merge with second half ---
          val rowAggA = new RowAggregator(inputSchema, aggregation.unpack)
          val columnAggA = rowAggA.columnAggregators(0)
          val irA = rowAggA.init
          firstHalf.foreach { sparkRow =>
            rowAggA.update(irA, SparkConversions.toChrononRow(sparkRow, tsIdx))
          }

          // Round-trip through Spark types
          val sparkValue = ColumnIRSerializer.toSparkValue(irA(0), columnAggA)
          assertNotNull(s"[$label] Spark value should not be null", sparkValue)
          val deserializedIR = ColumnIRSerializer.fromSparkValue(sparkValue, columnAggA)
          assertNotNull(s"[$label] Deserialized IR should not be null", deserializedIR)

          // Verify pure round-trip: finalize(deserialize(serialize(IR))) == finalize(IR)
          val originalResult = columnAggA.finalize(irA(0))
          val deserializedResult = columnAggA.finalize(deserializedIR)
          assertNotNull(s"[$label] Original result should not be null", originalResult)
          assertNotNull(s"[$label] Deserialized result should not be null", deserializedResult)
          deepEquals(originalResult, deserializedResult, s"$label round-trip")

          // --- Merge test: prove serialization doesn't corrupt IR for merge ---
          // Build second-half IR twice (fresh aggregators each time)
          val rowAggSecond1 = new RowAggregator(inputSchema, aggregation.unpack)
          val irSecond1 = rowAggSecond1.init
          secondHalf.foreach { sparkRow =>
            rowAggSecond1.update(irSecond1, SparkConversions.toChrononRow(sparkRow, tsIdx))
          }

          val rowAggSecond2 = new RowAggregator(inputSchema, aggregation.unpack)
          val irSecond2 = rowAggSecond2.init
          secondHalf.foreach { sparkRow =>
            rowAggSecond2.update(irSecond2, SparkConversions.toChrononRow(sparkRow, tsIdx))
          }

          // Path A: merge original first-half IR with second-half IR (no serialization)
          val rowAggExpected = new RowAggregator(inputSchema, aggregation.unpack)
          val columnAggExpected = rowAggExpected.columnAggregators(0)
          val irFirstCopy = rowAggExpected.init
          firstHalf.foreach { sparkRow =>
            rowAggExpected.update(irFirstCopy, SparkConversions.toChrononRow(sparkRow, tsIdx))
          }
          columnAggExpected.merge(irFirstCopy(0), irSecond1(0))
          val expectedMerged = columnAggExpected.finalize(irFirstCopy(0))

          // Path B: merge deserialized first-half IR with second-half IR
          columnAggA.merge(deserializedIR, irSecond2(0))
          val actualMerged = columnAggA.finalize(deserializedIR)

          // Both merges must produce identical results
          assertNotNull(s"[$label] Expected merged result should not be null", expectedMerged)
          assertNotNull(s"[$label] Actual merged result should not be null", actualMerged)
          deepEquals(expectedMerged, actualMerged, s"$label merge(original) vs merge(round-tripped)")

          println(s"  ✓ $op on $schemaName passed (round-trip + merge equivalence)")
      }
    }
  }
}
