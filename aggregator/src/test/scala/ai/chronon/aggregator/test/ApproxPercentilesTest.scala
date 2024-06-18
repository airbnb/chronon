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

package ai.chronon.aggregator.test

import org.slf4j.LoggerFactory
import ai.chronon.aggregator.base.ApproxPercentiles
import ai.chronon.aggregator.row.StatsGenerator
import com.yahoo.sketches.kll.KllFloatsSketch
import junit.framework.TestCase
import org.junit.Assert._

import scala.util.Random

class ApproxPercentilesTest extends TestCase {
  @transient lazy val logger = LoggerFactory.getLogger(getClass)
  def testBasicImpl(nums: Int, slide: Int, k: Int, percentiles: Array[Double], errorPercent: Float): Unit = {
    val sorted = (0 to nums).map(_.toFloat)
    val elems = Random.shuffle(sorted.toList).toArray
    val chunks = elems.sliding(slide, slide)
    val agg = new ApproxPercentiles(k, percentiles)
    val irs = chunks.map { chunk =>
      val init = agg.prepare(chunk.head)
      chunk.tail.foldLeft(init)(agg.update)
    }
    val merged = irs.reduce { agg.merge }
    val result = agg.finalize(merged)
    val clonedResult = agg.finalize(agg.clone(merged))
    assertTrue(result sameElements clonedResult)
    val step = nums / (result.size - 1)
    val expected = result.indices.map(_ * step).map(_.toFloat).toArray
    val diffs = result.indices.map(i => Math.abs(result(i) - expected(i)))
    val errorMargin = (nums.toFloat * errorPercent) / 100.0
    logger.info(s"""
         |sketch size: ${merged.getSerializedSizeBytes}
         |result: ${result.toVector}
         |result size: ${result.size}
         |diffs: ${diffs.toVector}
         |errorMargin: $errorMargin
         |""".stripMargin)
    diffs.foreach(diff => assertTrue(diff < errorMargin))
  }

  def testBasicPercentiles: Unit = {
    val percentiles_tested: Int = 31
    val percentiles: Array[Double] = (0 to percentiles_tested).toArray.map(i => i * 1.0 / percentiles_tested)
    testBasicImpl(3000, 5, 100, percentiles, errorPercent = 4)
    testBasicImpl(30000, 50, 200, percentiles, errorPercent = 2)
    testBasicImpl(30000, 50, 50, percentiles, errorPercent = 5)
  }

  def getPSIDrift(sample1: Array[Float], sample2: Array[Float]): Double = {
    val sketch1 = new KllFloatsSketch(200)
    val sketch2 = new KllFloatsSketch(200)
    sample1.map(sketch1.update)
    sample2.map(sketch2.update)
    val drift = StatsGenerator.PSIKllSketch(sketch1.toByteArray, sketch2.toByteArray).asInstanceOf[Double]
    logger.info(s"PSI drift: $drift")
    drift
  }

  def testPSIDrifts(): Unit = {
    assertTrue(
      getPSIDrift(
        Array(1, 1, 1, 2, 2, 3, 3, 4, 4, 5, 5, 6, 6, 7, 7).map(_.toFloat),
        Array(1, 1, 2, 2, 3, 3, 4, 4, 5, 5, 6, 6, 7, 7, 7).map(_.toFloat)
      ) < 0.1
    ) // Little shift
    assertTrue(
      getPSIDrift(
        Array(1, 1, 1, 1, 1, 1, 1, 1, 4, 5, 5, 6, 6, 7, 7).map(_.toFloat),
        Array(1, 1, 2, 2, 3, 3, 4, 4, 5, 5, 6, 6, 7, 7, 7).map(_.toFloat)
      ) > 0.25
    ) // Action required
    assertTrue(
      getPSIDrift(
        Array(1, 1, 1, 1, 1, 2, 2, 3, 3, 4, 4, 5, 5, 6, 6, 7, 7).map(_.toFloat),
        Array(1, 1, 2, 2, 3, 3, 4, 4, 5, 5, 6, 6, 7, 7, 7).map(_.toFloat)
      ) < 0.25
    ) // Moderate shift
  }
}
