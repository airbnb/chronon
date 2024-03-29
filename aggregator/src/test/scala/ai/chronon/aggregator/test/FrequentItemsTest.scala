package ai.chronon.aggregator.test

import ai.chronon.aggregator.base.FrequentItems
import junit.framework.TestCase

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

    assert(Map(
      "4" -> 4,
      "3" -> 3,
      "2" -> 2
    ) == result)
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

    assert(Map(
      3 -> 3,
      2 -> 2,
      1 -> 1
    ) == result)
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

    assert(Map() == result)
  }
}
