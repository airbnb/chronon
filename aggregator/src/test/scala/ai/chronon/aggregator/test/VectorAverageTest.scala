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
import ai.chronon.aggregator.base.VectorAverage
import junit.framework.TestCase
import org.junit.Assert._
import org.junit.Test

class VectorAverageTest extends TestCase {
  @transient lazy val logger = LoggerFactory.getLogger(getClass)

  @Test
  def testSingleVector: Unit = {
    val vectorAverage = new VectorAverage

    val vector = Array(1.5, 2.5, 3.5)
    val ir = vectorAverage.prepare(vector)
    val result = vectorAverage.finalize(ir)

    // Single vector should return itself
    logger.info(s"Result: $result, Expected: $vector")

    assertEquals(vector.length, result.length)
    for (i <- vector.indices) {
      assertEquals(vector(i), result(i), 0.0001)
    }
  }

  @Test
  def testBasicVectorAverage: Unit = {
    val vectorAverage = new VectorAverage
    
    // Test case from the requirements: [1.0, 2.0, 3.0] and [4.0, 5.0, 6.0] should result in [2.5, 3.5, 4.5]
    val vector1 = Array(1.0, 2.0, 3.0)
    val vector2 = Array(4.0, 5.0, 6.0)
    
    val ir1 = vectorAverage.prepare(vector1)
    val ir2 = vectorAverage.update(ir1, vector2)
    val result = vectorAverage.finalize(ir2)
    
    val expected = Array(2.5, 3.5, 4.5)
    logger.info(s"Result: $result, Expected: $expected")
    
    assertEquals(expected.length, result.length)
    for (i <- expected.indices) {
      assertEquals(expected(i), result(i), 0.0001)
    }
  }
  
  @Test
  def testDifferentSizedVectors: Unit = {
    val vectorAverage = new VectorAverage
    
    // Test vectors of different sizes
    val vector1 = Array(1.0, 2.0)
    val vector2 = Array(3.0, 4.0, 5.0)

    val ir1 = vectorAverage.prepare(vector1)

    try {
      vectorAverage.update(ir1, vector2)
    } catch {
      case e: IllegalStateException =>
        assertEquals("Vectors must all have same dimension", e.getMessage)
      case _: Throwable =>
        fail("Expected an IllegalStateException but caught a different exception")
    }
  }
  
  @Test
  def testMergeOperation: Unit = {
    val vectorAverage = new VectorAverage
    
    // Create two separate aggregations
    val ir1 = vectorAverage.prepare(Array(1.0, 2.0))
    val ir1Updated = vectorAverage.update(ir1, Array(3.0, 4.0))
    
    val ir2 = vectorAverage.prepare(Array(5.0, 6.0))
    val ir2Updated = vectorAverage.update(ir2, Array(7.0, 8.0))
    
    // Merge them
    val merged = vectorAverage.merge(ir1Updated, ir2Updated)
    val result = vectorAverage.finalize(merged)
    
    // Expected: [(1+3+5+7)/4, (2+4+6+8)/4] = [4.0, 5.0]
    val expected = Array(4.0, 5.0)
    logger.info(s"Result: $result, Expected: $expected")
    
    assertEquals(expected.length, result.length)
    for (i <- expected.indices) {
      assertEquals(expected(i), result(i), 0.0001)
    }
  }
  
  @Test
  def testDeleteOperation: Unit = {
    val vectorAverage = new VectorAverage
    
    // Add three vectors
    val ir1 = vectorAverage.prepare(Array(1.0, 2.0))
    val ir2 = vectorAverage.update(ir1, Array(3.0, 4.0))
    val ir3 = vectorAverage.update(ir2, Array(5.0, 6.0))
    
    // Delete one vector
    val irDeleted = vectorAverage.delete(ir3, Array(3.0, 4.0))
    val result = vectorAverage.finalize(irDeleted)
    
    // Expected: [(1+5)/2, (2+6)/2] = [3.0, 4.0]
    val expected = Array(3.0, 4.0)
    logger.info(s"Result: $result, Expected: $expected")
    
    assertEquals(expected.length, result.length)
    for (i <- expected.indices) {
      assertEquals(expected(i), result(i), 0.0001)
    }
  }
}
