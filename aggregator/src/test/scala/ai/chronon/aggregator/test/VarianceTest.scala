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
import ai.chronon.aggregator.base.Variance
import junit.framework.TestCase
import org.junit.Assert._

class VarianceTest extends TestCase {
  @transient lazy val logger = LoggerFactory.getLogger(getClass)

  def mean(elems: Seq[Double]): Double = elems.sum / elems.length
  def naive(elems: Seq[Double]): Double = {
    // two pass algo  - stable but we need single-pass
    val meanVal = mean(elems)
    elems.map(x => (x - meanVal) * (x - meanVal)).sum / elems.length
  }

  def sumOfSquares(elems: Seq[Double]): Double = {
    val meanOfSquares = mean(elems.map(x => x * x))
    val meanVal = mean(elems)
    meanOfSquares - (meanVal * meanVal)
  }

  def welford(elems: Seq[Double], chunkSize: Int = 10): Double = {
    val variance = new Variance
    val chunks = elems.sliding(chunkSize, chunkSize)
    val mergedIr = chunks
      .map { chunk =>
        val ir = variance.prepare(chunk.head)
        val chunkIr = chunk.tail.foldLeft(ir)(variance.update)
        chunkIr
      }
      .reduce(variance.merge)
    variance.finalize(mergedIr)
  }

  def compare(cardinality: Int, min: Double = 0.0, max: Double = 1000000.0): Unit = {
    val nums = (0 until cardinality).map { _ => min + math.random * (max - min) }
    val naiveResult = naive(nums)
    val welfordResult = welford(nums)
    logger.info(s"naive $naiveResult - welford $welfordResult - sum of squares ${sumOfSquares(nums)}")
    logger.info(((naiveResult - welfordResult) / naiveResult).toString)
    assertTrue((naiveResult - welfordResult) / naiveResult < 0.0000001)
  }

  def testVariance: Unit = {
    compare(1000000)
    compare(1000000, min = 100000, max = 100001)
  }
}
