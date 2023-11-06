package ai.chronon.aggregator.test

import ai.chronon.aggregator.base.MinHeap
import junit.framework.TestCase
import org.junit.Assert._

import java.util
import scala.collection.JavaConverters._
import ai.chronon.aggregator.base.TimeTuple

import java.util.concurrent.ThreadLocalRandom
import scala.collection.immutable
import scala.util.Random

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

  /**
   * We test for deterministic timestamp tiebreaking for TimeTuples by
   * sorting random items with the same TimeTuple timestamps.
   *
   * We want to ensure aggs like LastK that use this logic produce consistent
   * results when aggregating over events that arrive with the same timestamp
   */
  def testTimeTupleSorts(): Unit = {
    val size = 100
    val elems: immutable.Seq[Int] = (1 to 200).map(_ => ThreadLocalRandom.current().nextInt(-100, 100))
    def makeContainer = new util.ArrayList[TimeTuple.typ](size)

    val results: Seq[Any] = (1 to 1000).map { _ =>
      val mh = new MinHeap[TimeTuple.typ](size, TimeTuple)
      val heapArr = makeContainer
      Random.shuffle(elems).map { x =>
        mh.insert(heapArr, TimeTuple.make(1L, x))
      }
      val sorted = mh.sort(heapArr)
      println(sorted)
      sorted
    }
    assert(results.forall(_ == results.head))
  }
}
