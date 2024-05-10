package ai.chronon.aggregator.test

import ai.chronon.aggregator.base.{FrequentItemType, FrequentItems, FrequentItemsFriendly, ItemsSketchIR}
import junit.framework.TestCase
import org.junit.Assert._

import java.util
import scala.jdk.CollectionConverters._

class FrequentItemsTest extends TestCase {
  def testNonPowerOfTwoAndTruncate(): Unit = {
    val size = 3
    val items = new FrequentItems[String](size)
    val ir = items.prepare("4")

    def update(value: String, times: Int): Unit = (1 to times).foreach({ _ => items.update(ir, value) })

    update("4", 3)
    update("3", 3)
    update("2", 2)
    update("1", 1)

    val result = items.finalize(ir)

    assertEquals(toHashMap(Map(
      "4" -> 4,
      "3" -> 3,
      "2" -> 2
    )), result)
  }

  def testLessItemsThanSize(): Unit = {
    val size = 10
    val items = new FrequentItems[java.lang.Long](size)
    val ir = items.prepare(3)

    def update(value: Long, times: Int): Unit = (1 to times).foreach({ _ => items.update(ir, value) })

    update(3, 2)
    update(2, 2)
    update(1, 1)

    val result = items.finalize(ir)

    assertEquals(toHashMap(Map(
      "3" -> 3L,
      "2" -> 2L,
      "1" -> 1L
    )), result)
  }

  def testZeroSize(): Unit = {
    val size = 0
    val items = new FrequentItems[java.lang.Double](size)
    val ir = items.prepare(3.0)

    def update(value: java.lang.Double, times: Int): Unit = (1 to times).foreach({ _ => items.update(ir, value) })

    update(3.0, 2)
    update(2.0, 2)
    update(1.0, 1)

    val result = items.finalize(ir)

    assertEquals(new util.HashMap[String, Double](), result)
  }

  def testSketchSizes(): Unit = {
    val expectedSketchSizes =
      Map(
        -1 -> 2,
        0 -> 2,
        1 -> 2,
        31 -> 32,
        32 -> 32,
        33 -> 64
      )

    val actualSketchSizes =
      expectedSketchSizes.keys
        .map(k => k -> new FrequentItems[java.lang.Long](k).sketchSize)
        .toMap

    assertEquals(expectedSketchSizes, actualSketchSizes)
  }

  def testNormalization(): Unit = {
    val testValues = (1 to 4)
      .map(i => i -> i)
      .toMap

    def serialize[T: FrequentItemsFriendly](values: Map[T, Int]) = {
      val (sketch, ir) = toSketch(values)
      val bytes = sketch.normalize(ir)
      val cloned = sketch.denormalize(bytes)
      (cloned.sketchType, values.keys.map({ k => k -> cloned.sketch.getEstimate(k) }).toMap)
    }

    // Longs
    val expectedLongValues = testValues.map({ case (k, v) => k.toLong.asInstanceOf[java.lang.Long] -> v })
    val (longSketchType, actualLongValues) = serialize(expectedLongValues)
    assertEquals(FrequentItemType.LongItemType, longSketchType)
    assertEquals(expectedLongValues, actualLongValues)

    // Doubles
    val expectedDoubleValues = testValues.map({ case (k, v) => k.toDouble.asInstanceOf[java.lang.Double] -> v })
    val (doubleSketchType, actualDoubleValues) = serialize(expectedDoubleValues)
    assertEquals(FrequentItemType.DoubleItemType, doubleSketchType)
    assertEquals(expectedDoubleValues, actualDoubleValues)

    // Strings
    val expectedStringValues = testValues.map({ case (k, v) => k.toString -> v })
    val (stringSketchType, actualStringValues) = serialize(expectedStringValues)
    assertEquals(FrequentItemType.StringItemType, stringSketchType)
    assertEquals(expectedStringValues, actualStringValues)
  }

   def testBulkMerge(): Unit = {
     val sketch = new FrequentItems[String](3)

     val irs = Seq(
       toSketch(Map("3" -> 3)),
       toSketch(Map("2" -> 2)),
       toSketch(Map("1" -> 1)),
     ).map(i => i._2).iterator

     val ir = sketch.bulkMerge(irs)

     assertEquals(toHashMap(Map(
       "3" -> 3,
       "2" -> 2,
       "1" -> 1
     )), sketch.finalize(ir))
   }

  private def toSketch[T: FrequentItemsFriendly](counts: Map[T, Int]): (FrequentItems[T], ItemsSketchIR[T]) = {
    val sketch = new FrequentItems[T](4)
    val items = counts.toSeq.sortBy(_._2).reverse
    val ir = sketch.prepare(items.head._1)

    def increment(value: T, times: Int) = {
      (1 to times).foreach({ _ => sketch.update(ir, value) })
    }

    increment(items.head._1, items.head._2 - 1)
    items.tail.foreach(item => increment(item._1, item._2))

    (sketch, ir)
  }

  def toHashMap[T](map: Map[T, Long]): java.util.HashMap[T, Long] = new java.util.HashMap[T, Long](map.asJava)
}
