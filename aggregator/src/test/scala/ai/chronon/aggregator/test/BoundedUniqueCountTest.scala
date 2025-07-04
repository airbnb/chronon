package ai.chronon.aggregator.test

import ai.chronon.aggregator.base.BoundedUniqueCount
import ai.chronon.api.StringType
import junit.framework.TestCase
import org.junit.Assert._

import java.util
import scala.jdk.CollectionConverters._

class BoundedUniqueCountTest extends TestCase {
  def testHappyCase(): Unit = {
    val boundedDistinctCount = new BoundedUniqueCount[String](StringType, 5)
    var ir = boundedDistinctCount.prepare("1")
    ir = boundedDistinctCount.update(ir, "1")
    ir = boundedDistinctCount.update(ir, "2")

    val result = boundedDistinctCount.finalize(ir)
    assertEquals(2, result)
  }

  def testExceedSize(): Unit = {
    val boundedDistinctCount = new BoundedUniqueCount[String](StringType, 5)
    var ir = boundedDistinctCount.prepare("1")
    ir = boundedDistinctCount.update(ir, "2")
    ir = boundedDistinctCount.update(ir, "3")
    ir = boundedDistinctCount.update(ir, "4")
    ir = boundedDistinctCount.update(ir, "5")
    ir = boundedDistinctCount.update(ir, "6")
    ir = boundedDistinctCount.update(ir, "7")

    val result = boundedDistinctCount.finalize(ir)
    assertEquals(5, result)
  }

  def testMerge(): Unit = {
    val boundedDistinctCount = new BoundedUniqueCount[String](StringType, 5)
    val ir1 = new util.HashSet[String](Seq("1", "2", "3").asJava)
    val ir2 = new util.HashSet[String](Seq("4", "5", "6").asJava)

    val merged = boundedDistinctCount.merge(ir1, ir2)
    assertEquals(merged.size(), 5)
  }
}