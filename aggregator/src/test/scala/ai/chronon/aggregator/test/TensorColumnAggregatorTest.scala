/*
 *    Copyright (C) 2025 The Chronon Authors.
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

package ai.chronon.aggregator.test

import org.slf4j.LoggerFactory
import ai.chronon.aggregator.row.RowAggregator
import ai.chronon.api._
import junit.framework.TestCase
import org.junit.Assert._
import org.junit.Test

import scala.jdk.CollectionConverters._

class TensorColumnAggregatorTest extends TestCase {
  @transient lazy val logger = LoggerFactory.getLogger(getClass)

  def createTestRow(embeddings: Array[Double]): Row = {
    TestRow(Seq(embeddings: _*))
  }

  @Test
  def testSingleTensor(): Unit = {
    val schema = List("embeddings" -> ListType(DoubleType))
    val aggregationPart = Builders.AggregationPart(Operation.AVERAGE, "embeddings", tensorElementWiseOperation = true)
    val rowAggregator = new RowAggregator(schema, Seq(aggregationPart))

    val tensor = Array(1.5, 2.5, 3.5)
    val row = createTestRow(tensor)

    val ir = rowAggregator.init
    rowAggregator.update(ir, row)
    val result = rowAggregator.finalize(ir).head.asInstanceOf[java.util.List[Double]].asScala.toArray

    // Single tensor should return itself
    logger.info(s"Result: ${result.mkString("[", ", ", "]")}, Expected: ${tensor.mkString("[", ", ", "]")}")

    assertEquals(tensor.length, result.length)
    for (i <- tensor.indices) {
      assertEquals(tensor(i), result(i), 0.0001)
    }
  }

  @Test
  def testBasicTensorAverage(): Unit = {
    val schema = List("embeddings" -> ListType(DoubleType))
    val aggregationPart = Builders.AggregationPart(Operation.AVERAGE, "embeddings", tensorElementWiseOperation = true)
    val rowAggregator = new RowAggregator(schema, Seq(aggregationPart))

    // Test case from the requirements: [1.0, 2.0, 3.0] and [4.0, 5.0, 6.0] should result in [2.5, 3.5, 4.5]
    val tensor1 = Array(1.0, 2.0, 3.0)
    val tensor2 = Array(4.0, 5.0, 6.0)

    val ir = rowAggregator.init
    rowAggregator.update(ir, createTestRow(tensor1))
    rowAggregator.update(ir, createTestRow(tensor2))
    val result = rowAggregator.finalize(ir).head.asInstanceOf[java.util.List[Double]].asScala.toArray

    val expected = Array(2.5, 3.5, 4.5)
    logger.info(s"Result: ${result.mkString("[", ", ", "]")}, Expected: ${expected.mkString("[", ", ", "]")}")

    assertEquals(expected.length, result.length)
    for (i <- expected.indices) {
      assertEquals(expected(i), result(i), 0.0001)
    }
  }

  @Test
  def testDifferentSizedTensors(): Unit = {
    val schema = List("embeddings" -> ListType(DoubleType))
    val aggregationPart = Builders.AggregationPart(Operation.AVERAGE, "embeddings", tensorElementWiseOperation = true)
    val rowAggregator = new RowAggregator(schema, Seq(aggregationPart))

    // Test tensors of different sizes
    val tensor1 = Array(1.0, 2.0)
    val tensor2 = Array(3.0, 4.0, 5.0)

    val ir = rowAggregator.init
    rowAggregator.update(ir, createTestRow(tensor1))

    try {
      rowAggregator.update(ir, createTestRow(tensor2))
      fail("Expected an IllegalStateException but no exception was thrown")
    } catch {
      case e: IllegalStateException =>
        assertTrue("Exception message should mention tensor dimensions",
                   e.getMessage.contains("Tensor dimensions must match"))
      case _: Throwable =>
        fail("Expected an IllegalStateException but caught a different exception")
    }
  }

  @Test
  def testMergeOperation(): Unit = {
    val schema = List("embeddings" -> ListType(DoubleType))
    val aggregationPart = Builders.AggregationPart(Operation.AVERAGE, "embeddings", tensorElementWiseOperation = true)
    val rowAggregator = new RowAggregator(schema, Seq(aggregationPart))

    // Create two separate aggregations
    val ir1 = rowAggregator.init
    rowAggregator.update(ir1, createTestRow(Array(1.0, 2.0)))
    rowAggregator.update(ir1, createTestRow(Array(3.0, 4.0)))

    val ir2 = rowAggregator.init
    rowAggregator.update(ir2, createTestRow(Array(5.0, 6.0)))
    rowAggregator.update(ir2, createTestRow(Array(7.0, 8.0)))

    // Merge them
    rowAggregator.merge(ir1, ir2)
    val result = rowAggregator.finalize(ir1).head.asInstanceOf[java.util.List[Double]].asScala.toArray

    // Expected: [(1+3+5+7)/4, (2+4+6+8)/4] = [4.0, 5.0]
    val expected = Array(4.0, 5.0)
    logger.info(s"Result: ${result.mkString("[", ", ", "]")}, Expected: ${expected.mkString("[", ", ", "]")}")

    assertEquals(expected.length, result.length)
    for (i <- expected.indices) {
      assertEquals(expected(i), result(i), 0.0001)
    }
  }

  @Test
  def testDeleteOperation(): Unit = {
    val schema = List("embeddings" -> ListType(DoubleType))
    val aggregationPart = Builders.AggregationPart(Operation.AVERAGE, "embeddings", tensorElementWiseOperation = true)
    val rowAggregator = new RowAggregator(schema, Seq(aggregationPart))

    // Add three tensors
    val ir = rowAggregator.init
    rowAggregator.update(ir, createTestRow(Array(1.0, 2.0)))
    rowAggregator.update(ir, createTestRow(Array(3.0, 4.0)))
    rowAggregator.update(ir, createTestRow(Array(5.0, 6.0)))

    // Delete one tensor
    rowAggregator.delete(ir, createTestRow(Array(3.0, 4.0)))
    val result = rowAggregator.finalize(ir).head.asInstanceOf[java.util.List[Double]].asScala.toArray

    // Expected: [(1+5)/2, (2+6)/2] = [3.0, 4.0]
    val expected = Array(3.0, 4.0)
    logger.info(s"Result: ${result.mkString("[", ", ", "]")}, Expected: ${expected.mkString("[", ", ", "]")}")

    assertEquals(expected.length, result.length)
    for (i <- expected.indices) {
      assertEquals(expected(i), result(i), 0.0001)
    }
  }

  @Test
  def testTensorSum(): Unit = {
    val schema = List("embeddings" -> ListType(DoubleType))
    val aggregationPart = Builders.AggregationPart(Operation.SUM, "embeddings", tensorElementWiseOperation = true)
    val rowAggregator = new RowAggregator(schema, Seq(aggregationPart))

    // Test SUM with tensor flag - element-wise sum
    val tensor1 = Array(1.0, 2.0, 3.0)
    val tensor2 = Array(4.0, 5.0, 6.0)

    val ir = rowAggregator.init
    rowAggregator.update(ir, createTestRow(tensor1))
    rowAggregator.update(ir, createTestRow(tensor2))
    val result = rowAggregator.finalize(ir).head.asInstanceOf[java.util.List[Double]].asScala.toArray

    // Expected: [1+4, 2+5, 3+6] = [5.0, 7.0, 9.0]
    val expected = Array(5.0, 7.0, 9.0)
    logger.info(s"Result: ${result.mkString("[", ", ", "]")}, Expected: ${expected.mkString("[", ", ", "]")}")

    assertEquals(expected.length, result.length)
    for (i <- expected.indices) {
      assertEquals(expected(i), result(i), 0.0001)
    }
  }

  @Test
  def testTensorMax(): Unit = {
    val schema = List("embeddings" -> ListType(DoubleType))
    val aggregationPart = Builders.AggregationPart(Operation.MAX, "embeddings", tensorElementWiseOperation = true)
    val rowAggregator = new RowAggregator(schema, Seq(aggregationPart))

    // Test MAX with tensor flag - element-wise max
    val tensor1 = Array(1.0, 5.0, 3.0)
    val tensor2 = Array(4.0, 2.0, 6.0)

    val ir = rowAggregator.init
    rowAggregator.update(ir, createTestRow(tensor1))
    rowAggregator.update(ir, createTestRow(tensor2))
    val result = rowAggregator.finalize(ir).head.asInstanceOf[java.util.List[Double]].asScala.toArray

    // Expected: [max(1,4), max(5,2), max(3,6)] = [4.0, 5.0, 6.0]
    val expected = Array(4.0, 5.0, 6.0)
    logger.info(s"Result: ${result.mkString("[", ", ", "]")}, Expected: ${expected.mkString("[", ", ", "]")}")

    assertEquals(expected.length, result.length)
    for (i <- expected.indices) {
      assertEquals(expected(i), result(i), 0.0001)
    }
  }

  @Test
  def testNullWithinTensor(): Unit = {
    val schema = List("embeddings" -> ListType(DoubleType))
    val aggregationPart = Builders.AggregationPart(Operation.AVERAGE, "embeddings", tensorElementWiseOperation = true)
    val rowAggregator = new RowAggregator(schema, Seq(aggregationPart))

    // Create a row with a null value inside the tensor
    val rowWithNull = TestRow(Seq(1.0, null, 3.0))

    val ir = rowAggregator.init

    try {
      rowAggregator.update(ir, rowWithNull)
      fail("Expected an IllegalArgumentException but no exception was thrown")
    } catch {
      case e: IllegalArgumentException =>
        assertTrue("Exception message should mention null values not allowed",
                   e.getMessage.contains("Null values are not allowed within tensors"))
      case _: Throwable =>
        fail("Expected an IllegalArgumentException but caught a different exception")
    }
  }
}
