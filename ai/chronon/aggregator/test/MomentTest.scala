package ai.chronon.aggregator.test

import ai.chronon.aggregator.base._
import junit.framework.TestCase
import org.apache.commons.math3.stat.descriptive.moment.{Kurtosis => ApacheKurtosis, Skewness => ApacheSkew}
import org.junit.Assert._

class MomentTest extends TestCase {
  def makeAgg(aggregator: MomentAggregator, values: Seq[Double]): (MomentAggregator, MomentsIR) = {
    var ir = aggregator.prepare(values.head)

    values.tail.foreach(v => {
      ir = aggregator.update(ir, v)
    })

    (aggregator, ir)
  }

  def expectedSkew(values: Seq[Double]): Double = new ApacheSkew().evaluate(values.toArray)
  def expectedKurtosis(values: Seq[Double]): Double = new ApacheKurtosis().evaluate(values.toArray)

  def assertUpdate(aggregator: MomentAggregator, values: Seq[Double], expected: Seq[Double] => Double): Unit = {
    val (agg, ir) = makeAgg(aggregator, values)
    assertEquals(expected(values), agg.finalize(ir), 0.1)
  }

  def assertMerge(aggregator: MomentAggregator,
                  v1: Seq[Double],
                  v2: Seq[Double],
                  expected: Seq[Double] => Double): Unit = {
    val (agg, ir1) = makeAgg(aggregator, v1)
    val (_, ir2) = makeAgg(aggregator, v2)

    val ir = agg.merge(ir1, ir2)
    assertEquals(expected(v1 ++ v2), agg.finalize(ir), 0.1)
  }

  def testUpdate(): Unit = {
    val values = Seq(1.1, 2.2, 3.3, 4.4, 5.5)
    assertUpdate(new Skew(), values, expectedSkew)
    assertUpdate(new Kurtosis(), values, expectedKurtosis)
  }

  def testInsufficientSizes(): Unit = {
    val values = Seq(1.1, 2.2, 3.3, 4.4)
    assertUpdate(new Skew(), values.take(2), _ => Double.NaN)
    assertUpdate(new Kurtosis(), values.take(3), _ => Double.NaN)
  }

  def testNoVariance(): Unit = {
    val values = Seq(1.0, 1.0, 1.0, 1.0)
    assertUpdate(new Skew(), values, _ => Double.NaN)
    assertUpdate(new Kurtosis(), values, _ => Double.NaN)
  }

  def testMerge(): Unit = {
    val values1 = Seq(1.1, 2.2, 3.3)
    val values2 = Seq(4.4, 5.5)
    assertMerge(new Kurtosis(), values1, values2, expectedKurtosis)
    assertMerge(new Skew(), values1, values2, expectedSkew)
  }

  def testNormalize(): Unit = {
    val values = Seq(1.0, 2.0, 3.0, 4.0, 5.0)
    val (agg, ir) = makeAgg(new Kurtosis, values)

    val normalized = agg.normalize(ir)
    val denormalized = agg.denormalize(normalized)

    assertEquals(ir, denormalized)
  }
}
