package ai.chronon.aggregator.test

import ai.chronon.aggregator.base.BoundedUniqueCount
import ai.chronon.api.{BinaryType, DataType, DoubleType, FloatType, IntType, LongType, ShortType, StringType}
import junit.framework.TestCase
import org.junit.Assert._

import java.util
import scala.util.ScalaJavaConversions.{JListOps, ListOps}

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
    val ir1 = new util.HashSet[Any](Seq("1", "2", "3").toJava)
    val ir2 = new util.HashSet[Any](Seq("4", "5", "6").toJava)

    val merged = boundedDistinctCount.merge(ir1, ir2)
    val result = boundedDistinctCount.finalize(merged)
    assertEquals(5, result) // Should return k=5 when exceeding the limit
  }

  def testIntTypeHappyCase(): Unit = {
    val boundedDistinctCount = new BoundedUniqueCount[Int](IntType, 5)
    var ir = boundedDistinctCount.prepare(1)
    ir = boundedDistinctCount.update(ir, 1)
    ir = boundedDistinctCount.update(ir, 2)

    val result = boundedDistinctCount.finalize(ir)
    assertEquals(2, result)
  }

  def testIntTypeExceedSize(): Unit = {
    val boundedDistinctCount = new BoundedUniqueCount[Int](IntType, 5)
    var ir = boundedDistinctCount.prepare(1)
    ir = boundedDistinctCount.update(ir, 2)
    ir = boundedDistinctCount.update(ir, 3)
    ir = boundedDistinctCount.update(ir, 4)
    ir = boundedDistinctCount.update(ir, 5)
    ir = boundedDistinctCount.update(ir, 6)
    ir = boundedDistinctCount.update(ir, 7)

    val result = boundedDistinctCount.finalize(ir)
    assertEquals(5, result)
  }

  def testIntTypeMerge(): Unit = {
    val boundedDistinctCount = new BoundedUniqueCount[Int](IntType, 5)
    val ir1 = new util.HashSet[Any](Seq(1, 2, 3).toJava)
    val ir2 = new util.HashSet[Any](Seq(4, 5, 6).toJava)

    val merged = boundedDistinctCount.merge(ir1, ir2)
    val result = boundedDistinctCount.finalize(merged)
    assertEquals(5, result) // Should return k=5 when exceeding the limit
  }

  def testLongTypeHappyCase(): Unit = {
    val boundedDistinctCount = new BoundedUniqueCount[Long](LongType, 5)
    var ir = boundedDistinctCount.prepare(1L)
    ir = boundedDistinctCount.update(ir, 1L)
    ir = boundedDistinctCount.update(ir, 2L)

    val result = boundedDistinctCount.finalize(ir)
    assertEquals(2, result)
  }

  def testLongTypeExceedSize(): Unit = {
    val boundedDistinctCount = new BoundedUniqueCount[Long](LongType, 5)
    var ir = boundedDistinctCount.prepare(1L)
    ir = boundedDistinctCount.update(ir, 2L)
    ir = boundedDistinctCount.update(ir, 3L)
    ir = boundedDistinctCount.update(ir, 4L)
    ir = boundedDistinctCount.update(ir, 5L)
    ir = boundedDistinctCount.update(ir, 6L)
    ir = boundedDistinctCount.update(ir, 7L)

    val result = boundedDistinctCount.finalize(ir)
    assertEquals(5, result)
  }

  def testDoubleTypeHappyCase(): Unit = {
    val boundedDistinctCount = new BoundedUniqueCount[Double](DoubleType, 5)
    var ir = boundedDistinctCount.prepare(1.0)
    ir = boundedDistinctCount.update(ir, 1.0)
    ir = boundedDistinctCount.update(ir, 2.5)

    val result = boundedDistinctCount.finalize(ir)
    assertEquals(2, result)
  }

  def testDoubleTypeExceedSize(): Unit = {
    val boundedDistinctCount = new BoundedUniqueCount[Double](DoubleType, 5)
    var ir = boundedDistinctCount.prepare(1.0)
    ir = boundedDistinctCount.update(ir, 2.5)
    ir = boundedDistinctCount.update(ir, 3.7)
    ir = boundedDistinctCount.update(ir, 4.2)
    ir = boundedDistinctCount.update(ir, 5.8)
    ir = boundedDistinctCount.update(ir, 6.3)
    ir = boundedDistinctCount.update(ir, 7.9)

    val result = boundedDistinctCount.finalize(ir)
    assertEquals(5, result)
  }

  def testFloatTypeHappyCase(): Unit = {
    val boundedDistinctCount = new BoundedUniqueCount[Float](FloatType, 5)
    var ir = boundedDistinctCount.prepare(1.0f)
    ir = boundedDistinctCount.update(ir, 1.0f)
    ir = boundedDistinctCount.update(ir, 2.5f)

    val result = boundedDistinctCount.finalize(ir)
    assertEquals(2, result)
  }

  def testFloatTypeExceedSize(): Unit = {
    val boundedDistinctCount = new BoundedUniqueCount[Float](FloatType, 5)
    var ir = boundedDistinctCount.prepare(1.0f)
    ir = boundedDistinctCount.update(ir, 2.5f)
    ir = boundedDistinctCount.update(ir, 3.7f)
    ir = boundedDistinctCount.update(ir, 4.2f)
    ir = boundedDistinctCount.update(ir, 5.8f)
    ir = boundedDistinctCount.update(ir, 6.3f)
    ir = boundedDistinctCount.update(ir, 7.9f)

    val result = boundedDistinctCount.finalize(ir)
    assertEquals(5, result)
  }

  def testBinaryTypeHappyCase(): Unit = {
    val boundedDistinctCount = new BoundedUniqueCount[Array[Byte]](BinaryType, 5)
    val bytes1 = Array[Byte](1, 2, 3)
    val bytes2 = Array[Byte](4, 5, 6)

    var ir = boundedDistinctCount.prepare(bytes1)
    ir = boundedDistinctCount.update(ir, bytes1)
    ir = boundedDistinctCount.update(ir, bytes2)

    val result = boundedDistinctCount.finalize(ir)
    assertEquals(2, result)
  }

  def testBinaryTypeExceedSize(): Unit = {
    val boundedDistinctCount = new BoundedUniqueCount[Array[Byte]](BinaryType, 5)
    var ir = boundedDistinctCount.prepare(Array[Byte](1))
    ir = boundedDistinctCount.update(ir, Array[Byte](2))
    ir = boundedDistinctCount.update(ir, Array[Byte](3))
    ir = boundedDistinctCount.update(ir, Array[Byte](4))
    ir = boundedDistinctCount.update(ir, Array[Byte](5))
    ir = boundedDistinctCount.update(ir, Array[Byte](6))
    ir = boundedDistinctCount.update(ir, Array[Byte](7))

    val result = boundedDistinctCount.finalize(ir)
    assertEquals(5, result)
  }

  def testBinaryTypeMerge(): Unit = {
    val boundedDistinctCount = new BoundedUniqueCount[Array[Byte]](BinaryType, 5)
    val ir1 = new util.HashSet[Any](Seq(Array[Byte](1), Array[Byte](2), Array[Byte](3)).toJava)
    val ir2 = new util.HashSet[Any](Seq(Array[Byte](4), Array[Byte](5), Array[Byte](6)).toJava)

    val merged = boundedDistinctCount.merge(ir1, ir2)
    val result = boundedDistinctCount.finalize(merged)
    assertEquals(5, result) // Should return k=5 when exceeding the limit
  }

  def testNumericTypeIrType(): Unit = {
    val intBoundedDistinctCount = new BoundedUniqueCount[Int](IntType, 5)
    val longBoundedDistinctCount = new BoundedUniqueCount[Long](LongType, 5)
    val doubleBoundedDistinctCount = new BoundedUniqueCount[Double](DoubleType, 5)
    val floatBoundedDistinctCount = new BoundedUniqueCount[Float](FloatType, 5)
    val binaryBoundedDistinctCount = new BoundedUniqueCount[Array[Byte]](BinaryType, 5)
    val stringBoundedDistinctCount = new BoundedUniqueCount[String](StringType, 5)

    // For numeric and binary types, irType should be ListType(inputType)
    assertEquals(ai.chronon.api.ListType(IntType), intBoundedDistinctCount.irType)
    assertEquals(ai.chronon.api.ListType(LongType), longBoundedDistinctCount.irType)
    assertEquals(ai.chronon.api.ListType(DoubleType), doubleBoundedDistinctCount.irType)
    assertEquals(ai.chronon.api.ListType(FloatType), floatBoundedDistinctCount.irType)
    assertEquals(ai.chronon.api.ListType(BinaryType), binaryBoundedDistinctCount.irType)

    // For non-numeric types, irType should be ListType(StringType)
    assertEquals(ai.chronon.api.ListType(StringType), stringBoundedDistinctCount.irType)
  }

  private def assertSaturatedIrKeepsInputType[T](inputType: DataType, inputs: Seq[T], elementClass: Class[_]): Unit = {
    val boundedDistinctCount = new BoundedUniqueCount[T](inputType, 3)
    val ir = inputs.tail.foldLeft(boundedDistinctCount.prepare(inputs.head))(boundedDistinctCount.update)
    val normalized = boundedDistinctCount.normalize(ir).asInstanceOf[util.ArrayList[Any]]

    normalized.toScala.foreach(v => assertTrue(s"$inputType IR holds $v", elementClass.isInstance(v)))
    assertEquals(3, normalized.size())
    assertEquals(3, boundedDistinctCount.finalize(boundedDistinctCount.denormalize(normalized)))
  }

  // IR writers store each element as irType's element type, so a saturated numeric or binary IR must hold
  // values of that type, not a string marker.
  def testSaturatedIrKeepsInputType(): Unit = {
    assertSaturatedIrKeepsInputType[Int](IntType, Seq(1, 2, 3, 4, 5), classOf[java.lang.Integer])
    assertSaturatedIrKeepsInputType[Long](LongType, Seq(1L, 2L, 3L, 4L, 5L), classOf[java.lang.Long])
    assertSaturatedIrKeepsInputType[Short](ShortType, Seq[Short](1, 2, 3, 4, 5), classOf[java.lang.Short])
    assertSaturatedIrKeepsInputType[Double](DoubleType, Seq(1.0, 2.0, 3.0, 4.0, 5.0), classOf[java.lang.Double])
    assertSaturatedIrKeepsInputType[Float](FloatType, Seq(1.0f, 2.0f, 3.0f, 4.0f, 5.0f), classOf[java.lang.Float])
    assertSaturatedIrKeepsInputType[Array[Byte]](BinaryType,
                                                 (1 to 5).map(i => Array[Byte](i.toByte)),
                                                 classOf[Array[Byte]])
    assertSaturatedIrKeepsInputType[String](StringType, Seq("1", "2", "3", "4", "5"), classOf[String])
  }

  // IRs written before saturated IRs kept their values hold the "__SENTINEL__" marker, and still read as saturated.
  def testSentinelIrStillSaturated(): Unit = {
    val boundedDistinctCount = new BoundedUniqueCount[String](StringType, 5)
    def sentinelIr = boundedDistinctCount.denormalize(new util.ArrayList[Any](Seq[Any]("__SENTINEL__").toJava))

    assertEquals(5, boundedDistinctCount.finalize(sentinelIr))
    assertEquals(5, boundedDistinctCount.finalize(boundedDistinctCount.update(sentinelIr, "1")))
    assertEquals(
      5,
      boundedDistinctCount.finalize(boundedDistinctCount.merge(sentinelIr, boundedDistinctCount.prepare("1"))))
    assertEquals(
      5,
      boundedDistinctCount.finalize(boundedDistinctCount.merge(boundedDistinctCount.prepare("1"), sentinelIr)))
  }
}
