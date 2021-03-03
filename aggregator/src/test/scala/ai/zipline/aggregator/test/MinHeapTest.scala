package ai.zipline.aggregator.test

import java.util

import ai.zipline.aggregator.base.MinHeap
import junit.framework.TestCase
import org.junit.Assert._

import scala.collection.JavaConverters._

class MinHeapTest extends TestCase {
  def testInserts(): Unit = {
    val mh = new MinHeap[Int](maxSize = 4, Ordering.Int)

    def make_container = new util.ArrayList[Int](4)

    val arr1 = make_container
    mh.insert(arr1, 5)
    mh.insert(arr1, 4)
    mh.insert(arr1, 9)
    mh.insert(arr1, 10)
    mh.insert(arr1, -1)
    assertArrayEquals(arr1.asScala.toArray, Array(9, 4, 5, -1))

    val arr2 = make_container
    mh.insert(arr2, 5)
    mh.insert(arr2, 4)
    mh.insert(arr2, 9)
    mh.insert(arr2, 10)
    mh.insert(arr2, 1)
    mh.insert(arr2, 2)
    assertArrayEquals(arr2.asScala.toArray, Array(5, 4, 2, 1))

    mh.merge(arr1, arr2)
    assertArrayEquals(arr1.asScala.toArray, Array(4, 2, 1, -1))
  }
}
