package ai.zipline.aggregator.test

import ai.zipline.aggregator.base.Variance
import junit.framework.TestCase
import org.junit.Assert._

class VarianceTest extends TestCase {

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
    println(s"naive $naiveResult - welford $welfordResult - sum of squares ${sumOfSquares(nums)}")
    println((naiveResult - welfordResult) / naiveResult)
    assertTrue((naiveResult - welfordResult) / naiveResult < 0.0000001)
  }

  def testVariance: Unit = {
    compare(1000000)
    compare(1000000, min = 100000, max = 100001)
  }
}
