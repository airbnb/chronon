package ai.chronon.aggregator.test

import ai.chronon.aggregator.base.{ApproxHistogram, ApproxHistogramIr}
import junit.framework.TestCase
import org.junit.Assert._

class ApproxHistogramTest extends TestCase {
  def testHistogram(): Unit = {
    val approxHistogram = new ApproxHistogram[String](3)
    val counts = (1 to 3).map(i => i.toString -> i).toMap
    val ir = makeIr(approxHistogram, counts)

    assertTrue(!ir.isApprox)
    assertTrue(ir.sketch.isEmpty)
    assertEquals(counts, approxHistogram.finalize(ir))
  }

  def testSketch(): Unit = {
    val approxHistogram = new ApproxHistogram[String](3)
    val counts = (1 to 4).map(i => i.toString -> i).toMap
    val expected = counts.toSeq.sortBy(_._2).reverse.take(3).toMap
    val ir = makeIr(approxHistogram, counts)

    assertTrue(ir.isApprox)
    assertTrue(ir.histogram.isEmpty)
    assertEquals(expected, approxHistogram.finalize(ir))
  }

  def testMergeSketches(): Unit = {
    val approxHistogram = new ApproxHistogram[String](3)
    val counts1 = Map("5" -> 5, "4" -> 4, "2" -> 2, "1" -> 1)
    val counts2 = Map("6" -> 6, "4" -> 4, "2" -> 2, "1" -> 1)

    val ir1 = makeIr(approxHistogram, counts1)
    val ir2 = makeIr(approxHistogram, counts2)

    assertTrue(ir1.isApprox)
    assertTrue(ir2.isApprox)

    val ir = approxHistogram.merge(ir1, ir2)
    assertEquals(Map(
                   "4" -> 8,
                   "6" -> 6,
                   "5" -> 5
                 ),
                 approxHistogram.finalize(ir))
    assertTrue(ir.isApprox)
    assertTrue(ir.histogram.isEmpty)
  }

  def testMergeHistograms(): Unit = {
    val approxHistogram = new ApproxHistogram[String](3)
    val counts1 = Map("4" -> 4, "2" -> 2)
    val counts2 = Map("3" -> 3, "2" -> 2)

    val ir1 = makeIr(approxHistogram, counts1)
    val ir2 = makeIr(approxHistogram, counts2)

    assertTrue(!ir1.isApprox)
    assertTrue(!ir2.isApprox)

    val ir = approxHistogram.merge(ir1, ir2)

    assertEquals(Map(
      "2" -> 4,
      "4" -> 4,
      "3" -> 3
    ), approxHistogram.finalize(ir))
    assertTrue(!ir.isApprox)
    assertTrue(ir.sketch.isEmpty)
  }

  def testMergeHistogramsToSketch(): Unit = {
    val approxHistogram = new ApproxHistogram[String](3)
    val counts1 = Map("4" -> 4, "3" -> 3)
    val counts2 = Map("2" -> 2, "1" -> 1)

    val ir1 = makeIr(approxHistogram, counts1)
    val ir2 = makeIr(approxHistogram, counts2)

    assertTrue(!ir1.isApprox)
    assertTrue(!ir2.isApprox)

    val ir = approxHistogram.merge(ir1, ir2)

    assertEquals(Map(
      "4" -> 4,
      "3" -> 3,
      "2" -> 2
    ), approxHistogram.finalize(ir))

    assertTrue(ir.isApprox)
    assertTrue(ir.histogram.isEmpty)
  }

  def testMergeSketchAndHistogram(): Unit = {
    val approxHistogram = new ApproxHistogram[String](3)
    val counts1 = Map("5" -> 5, "3" -> 3, "2" -> 2, "1" -> 1)
    val counts2 = Map("2" -> 2)

    val ir1 = makeIr(approxHistogram, counts1)
    val ir2 = makeIr(approxHistogram, counts2)

    assertTrue(ir1.isApprox)
    assertTrue(!ir2.isApprox)

    val ir = approxHistogram.merge(ir1, ir2)

    assertEquals(Map(
      "5" -> 5,
      "2" -> 4,
      "3" -> 3
    ), approxHistogram.finalize(ir))
    assertTrue(ir.isApprox)
    assert(ir.histogram.isEmpty)
  }

  def testNormalizeHistogram(): Unit = {
    val approxHistogram = new ApproxHistogram[String](3)
    val counts = (1 to 3).map(i => i.toString -> i).toMap
    val ir = makeIr(approxHistogram, counts)
    assertTrue(ir.histogram.isDefined)

    val normalized = approxHistogram.denormalize(approxHistogram.normalize(ir))
    assertEquals(ir, normalized)
  }

  def testNormalizeSketch(): Unit = {
    val approxHistogram = new ApproxHistogram[String](3)
    val counts = (1 to 4).map(i => i.toString -> i).toMap
    val expected = counts.toSeq.sortBy(_._2).reverse.take(3).toMap
    val ir = makeIr(approxHistogram, counts)
    assertTrue(ir.sketch.isDefined)

    val normalized = approxHistogram.denormalize(approxHistogram.normalize(ir))
    assertEquals(expected, approxHistogram.finalize(normalized))
  }

  def makeIr[T](agg: ApproxHistogram[T], counts: Map[T, Int]): ApproxHistogramIr[T] = {
    val values = counts.toSeq.sortBy(_._2)

    var ir = agg.prepare(values.head._1)

    (1 until values.head._2).foreach(_ => ir = agg.update(ir, values.head._1))

    values.tail.foreach({
      case (k, v) =>
        (1 to v).foreach(_ => {
          ir = agg.update(ir, k)
        })
    })

    ir
  }
}
