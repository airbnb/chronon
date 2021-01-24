package ai.zipline.aggregator.base

import java.util

import com.yahoo.sketches.cpc.{CpcSketch, CpcUnion}

import scala.reflect.ClassTag

// specializing underlying types for speed, and in-case of sum, the output type is
// mostly independent of input type - to protect from overflow
class Sum[I: Numeric] extends SimpleAggregator[I, Long, Long] {
  private val numericImpl = implicitly[Numeric[I]]

  override def outputType: DataType = LongType

  override def irType: DataType = LongType

  override def prepare(input: I): Long = numericImpl.toLong(input)

  override def update(ir: Long, input: I): Long = ir + prepare(input)

  override def merge(ir1: Long, ir2: Long): Long = ir1 + ir2

  override def finalize(ir: Long): Long = ir

  override def delete(ir: Long, input: I): Long = ir - prepare(input)

  override def isDeletable: Boolean = true
}

class Count extends SimpleAggregator[Any, Long, Long] {
  override def outputType: DataType = LongType

  override def irType: DataType = LongType

  override def prepare(input: Any): Long = 1

  override def update(ir: Long, input: Any): Long = ir + 1

  override def merge(ir1: Long, ir2: Long): Long = ir1 + ir2

  override def finalize(ir: Long): Long = ir

  override def delete(ir: Long, input: Any): Long = ir - 1

  override def isDeletable: Boolean = true
}

class UniqueCount[T](inputType: DataType) extends SimpleAggregator[T, util.ArrayList[T], Long] {
  override def outputType: DataType = LongType

  override def irType: DataType = ListType(inputType)

  override def prepare(input: T): util.ArrayList[T] = {
    val result = new util.ArrayList[T]()
    result.add(input)
    result
  }

  override def update(ir: util.ArrayList[T], input: T): util.ArrayList[T] = {
    if (!ir.contains(input)) {
      ir.add(input)
    }
    ir
  }

  override def merge(ir1: util.ArrayList[T], ir2: util.ArrayList[T]): util.ArrayList[T] = {
    val it = ir2.iterator()
    while (it.hasNext) {
      val elem = it.next
      update(ir1, elem)
    }
    ir1
  }

  override def finalize(ir: util.ArrayList[T]): Long = ir.size()

  override def clone(ir: util.ArrayList[T]): util.ArrayList[T] = {
    val cloned = new util.ArrayList[T](ir.size())
    cloned.addAll(ir)
    cloned
  }
}

class Average extends SimpleAggregator[Double, Array[Any], Double] {
  override def outputType: DataType = DoubleType

  override def irType: DataType =
    StructType(
      s"avg_ir",
      List(StructField("sum", DoubleType), StructField("count", IntType))
    )

  override def prepare(input: Double): Array[Any] = Array(input, 1)

  // mutating
  override def update(ir: Array[Any], input: Double): Array[Any] = {
    ir.update(0, ir(0).asInstanceOf[Double] + input)
    ir.update(1, ir(1).asInstanceOf[Int] + 1)
    ir
  }

  // mutating
  override def merge(ir1: Array[Any], ir2: Array[Any]): Array[Any] = {
    ir1.update(0, ir1(0).asInstanceOf[Double] + ir2(0).asInstanceOf[Double])
    ir1.update(1, ir1(1).asInstanceOf[Int] + ir2(1).asInstanceOf[Int])
    ir1
  }

  override def finalize(ir: Array[Any]): Double =
    ir(0).asInstanceOf[Double] / ir(1).asInstanceOf[Int].toDouble

  override def delete(ir: Array[Any], input: Double): Array[Any] = {
    ir.update(0, ir(0).asInstanceOf[Double] - input)
    ir.update(1, ir(1).asInstanceOf[Int] - 1)
    ir
  }

  override def clone(ir: Array[Any]): Array[Any] = {
    val arr = new Array[Any](ir.length)
    ir.copyToArray(arr)
    arr
  }

  override def isDeletable: Boolean = true
}

trait CpcFriendly[Input] {
  def update(sketch: CpcSketch, input: Input): Unit
}

object CpcFriendly {
  implicit val stringIsCpcFriendly: CpcFriendly[String] = { (sketch: CpcSketch, input: String) =>
    sketch.update(input)
  }

  implicit val longIsCpcFriendly: CpcFriendly[Long] = { (sketch: CpcSketch, input: Long) =>
    sketch.update(input)
  }

  implicit val doubleIsCpcFriendly: CpcFriendly[Double] = { (sketch: CpcSketch, input: Double) =>
    sketch.update(input)
  }

  implicit val BinaryIsCpcFriendly: CpcFriendly[Array[Byte]] = { (sketch: CpcSketch, input: Array[Byte]) =>
    sketch.update(input)
  }
}

// Based on CPC sketch (a faster, smaller and more accurate version of HLL)
// See: Back to the future: an even more nearly optimal cardinality estimation algorithm, 2017
// https://arxiv.org/abs/1708.06839
// refer to the chart here to tune your sketch size with lgK
// https://github.com/apache/incubator-datasketches-java/blob/master/src/main/java/org/apache/datasketches/cpc/CpcSketch.java#L180
// default is about 1200 bytes
class ApproxDistinctCount[Input: CpcFriendly](
    lgK: Int = CpcSketch.DEFAULT_LG_K
) extends SimpleAggregator[Input, CpcSketch, Long] {
  override def outputType: DataType = LongType

  override def irType: DataType = BinaryType

  override def prepare(input: Input): CpcSketch = {
    val sketch = new CpcSketch(lgK)
    implicitly[CpcFriendly[Input]].update(sketch, input)
    sketch
  }

  override def update(ir: CpcSketch, input: Input): CpcSketch = {
    implicitly[CpcFriendly[Input]].update(ir, input)
    ir
  }

  override def merge(ir1: CpcSketch, ir2: CpcSketch): CpcSketch = {
    val merger = new CpcUnion(lgK)
    merger.update(ir1)
    merger.update(ir2)
    merger.getResult
  }

  // CPC sketch has a non-public copy() method, which is what we want
  // CPCUnion has access to that copy() method - so we kind of hack that
  // mechanism to get a proper copy without any overhead.
  override def clone(ir: CpcSketch): CpcSketch = {
    val merger = new CpcUnion(lgK)
    merger.update(ir)
    merger.getResult
  }

  override def finalize(ir: CpcSketch): Long = ir.getEstimate.toLong

  override def normalize(ir: CpcSketch): Array[Byte] = ir.toByteArray

  override def denormalize(normalized: Any): CpcSketch =
    CpcSketch.heapify(normalized.asInstanceOf[Array[Byte]])
}

abstract class Order[I](inputType: DataType) extends SimpleAggregator[I, I, I] {
  override def outputType: DataType = inputType

  override def irType: DataType = inputType

  override def prepare(input: I): I = input

  override def finalize(ir: I): I = ir
}

class Max[I: Ordering](inputType: DataType) extends Order[I](inputType) {
  override def update(ir: I, input: I): I =
    implicitly[Ordering[I]].max(ir, input)

  override def merge(ir1: I, ir2: I): I = implicitly[Ordering[I]].max(ir1, ir2)
}

class Min[I: Ordering](inputType: DataType) extends Order[I](inputType) {
  override def update(ir: I, input: I): I =
    implicitly[Ordering[I]].min(ir, input)

  override def merge(ir1: I, ir2: I): I = implicitly[Ordering[I]].min(ir1, ir2)
}

// generalization of topK and bottomK
class OrderByLimit[I: ClassTag](
    inputType: DataType,
    limit: Int,
    ordering: Ordering[I]
) extends SimpleAggregator[I, util.ArrayList[I], util.ArrayList[I]] {
  private val minHeap = new MinHeap[I](limit, ordering)

  override def outputType: DataType = ListType(inputType)

  override def irType: DataType = ListType(inputType)

  type Container = util.ArrayList[I]

  override final def prepare(input: I): Container = {
    val arr = new util.ArrayList[I]()
    arr.add(input)
    arr
  }

  // mutating
  override final def update(state: Container, input: I): Container =
    minHeap.insert(state, input)

  // mutating 1
  override final def merge(state1: Container, state2: Container): Container =
    minHeap.merge(state1, state2)

  override def finalize(state: Container): Container = minHeap.sort(state)

  override def clone(ir: Container): Container = {
    val cloned = new util.ArrayList[I](ir.size())
    cloned.addAll(ir)
    cloned
  }
}

class TopK[T: Ordering: ClassTag](inputType: DataType, k: Int)
    extends OrderByLimit[T](inputType, k, Ordering[T].reverse)

class BottomK[T: Ordering: ClassTag](inputType: DataType, k: Int) extends OrderByLimit[T](inputType, k, Ordering[T])
