package ai.chronon.aggregator.test

import ai.chronon.aggregator.base.ApproxPercentiles
import junit.framework.TestCase
import org.junit.Assert._

import scala.util.Random

class ApproxPercentilesTest extends TestCase {
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
    println(s"""
         |sketch size: ${merged.getSerializedSizeBytes}
         |result: ${result.toVector}
         |result size: ${result.size}
         |diffs: ${diffs.toVector}
         |errorMargin: $errorMargin
         |""".stripMargin)
    diffs.foreach(diff => assertTrue(diff < errorMargin))
  }

  def testBasic: Unit = {
    testBasicImpl(3000, 5, 100, Array(0.25, 0.5, 0.75, 0.95, 0.99), errorPercent = 4)
    testBasicImpl(30000, 50, 200, Array(0.25, 0.5, 0.75, 0.95, 0.99), errorPercent = 2)
    testBasicImpl(30000, 50, 50, Array(0.25, 0.5, 0.75, 0.95, 0.99), errorPercent = 5)
  }
}
