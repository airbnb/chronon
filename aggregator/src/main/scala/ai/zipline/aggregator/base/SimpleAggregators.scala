package ai.zipline.aggregator.base

import ai.zipline.api._
import com.yahoo.sketches.cpc.{CpcSketch, CpcUnion}

import java.util
import java.util.PriorityQueue
import scala.reflect.ClassTag

class Sum[I: Numeric](inputType: DataType) extends SimpleAggregator[I, I, I] {
  private val numericImpl = implicitly[Numeric[I]]

  override def outputType: DataType = inputType

  override def irType: DataType = inputType

  override def prepare(input: I): I = input

  override def update(ir: I, input: I): I = numericImpl.plus(ir, input)

  override def merge(ir1: I, ir2: I): I = numericImpl.plus(ir1, ir2)

  override def finalize(ir: I): I = ir

  override def delete(ir: I, input: I): I = numericImpl.minus(ir, input)

  override def isDeletable: Boolean = true

  override def clone(ir: I): I = ir
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

  override def clone(ir: Long): Long = ir
}

class UniqueCount[T](inputType: DataType) extends SimpleAggregator[T, util.HashSet[T], Long] {
  override def outputType: DataType = LongType

  override def irType: DataType = ListType(inputType)

  override def prepare(input: T): util.HashSet[T] = {
    val result = new util.HashSet[T]()
    result.add(input)
    result
  }

  override def update(ir: util.HashSet[T], input: T): util.HashSet[T] = {
    if (!ir.contains(input)) {
      ir.add(input)
    }
    ir
  }

  override def merge(ir1: util.HashSet[T], ir2: util.HashSet[T]): util.HashSet[T] = {
    ir1.addAll(ir2)
    ir1
  }

  override def finalize(ir: util.HashSet[T]): Long = ir.size()

  override def clone(ir: util.HashSet[T]): util.HashSet[T] = {
    val cloned = new util.HashSet[T]()
    cloned.addAll(ir)
    cloned
  }

  override def normalize(ir: util.HashSet[T]): Any = {
    val arr = new util.ArrayList[T](ir.size())
    arr.addAll(ir)
    arr
  }

  override def denormalize(ir: Any): util.HashSet[T] = {
    val set = new util.HashSet[T]()
    set.addAll(ir.asInstanceOf[util.ArrayList[T]])
    set
  }
}

class Average extends SimpleAggregator[Double, Array[Any], Double] {
  override def outputType: DataType = DoubleType

  override def irType: DataType =
    StructType(
      "AvgIr",
      Array(StructField("sum", DoubleType), StructField("count", IntType))
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

class Histogram(k: Int = 0) extends SimpleAggregator[String, util.Map[String, Int], util.Map[String, Int]] {
  type IrMap = util.Map[String, Int]
  override def outputType: DataType = MapType(StringType, IntType)

  override def irType: DataType = outputType

  override def prepare(input: String): IrMap = {
    val result = new util.HashMap[String, Int]()
    result.put(input, 1)
    result
  }

  def incrementInMap(ir: IrMap, input: String, count: Int = 1): IrMap = {
    val old = ir.get(input)
    val newVal = if (old == null) count else old + count
    if (newVal == 0) {
      ir.remove(input)
    } else {
      ir.put(input, newVal)
    }
    ir
  }

  // mutating
  override def update(ir: IrMap, input: String): IrMap = {
    incrementInMap(ir, input)
  }

  // mutating
  override def merge(ir1: IrMap, ir2: IrMap): IrMap = {
    val it = ir2.entrySet().iterator()
    while (it.hasNext) {
      val entry = it.next()
      val key = entry.getKey
      val value = entry.getValue
      incrementInMap(ir1, key, value)
    }
    ir1
  }

  override def finalize(ir: IrMap): IrMap = {
    if (k > 0 && ir.size() > k) { // keep only top k values
      val pq = new MinHeap[Int](k, Ordering[Int].reverse)
      val it = ir.entrySet().iterator()
      val heap = new util.ArrayList[Int]()
      while (it.hasNext) { pq.insert(heap, it.next().getValue) }
      val cutOff = pq.sort(heap).get(k - 1)
      val newResult = new util.HashMap[String, Int]()
      val itNew = ir.entrySet().iterator()
      while (itNew.hasNext && newResult.size() < k) {
        val entry = itNew.next()
        if (entry.getValue >= cutOff) {
          newResult.put(entry.getKey, entry.getValue)
        }
      }
      newResult
    } else {
      ir
    }
  }

  override def delete(ir: IrMap, input: String): IrMap = {
    incrementInMap(ir, input, -1)
  }

  override def clone(ir: IrMap): IrMap = {
    val result = new util.HashMap[String, Int]();
    result.putAll(ir);
    result
  }

  override def isDeletable: Boolean = true
}

trait CpcFriendly[Input] {
  def update(sketch: CpcSketch, input: Input): Unit
}

object CpcFriendly {
  implicit val stringIsCpcFriendly: CpcFriendly[String] = new CpcFriendly[String] {
    override def update(sketch: CpcSketch, input: String): Unit = sketch.update(input)
  }

  implicit val longIsCpcFriendly: CpcFriendly[Long] = new CpcFriendly[Long] {
    override def update(sketch: CpcSketch, input: Long): Unit = sketch.update(input)
  }
  implicit val doubleIsCpcFriendly: CpcFriendly[Double] = new CpcFriendly[Double] {
    override def update(sketch: CpcSketch, input: Double): Unit = sketch.update(input)
  }

  implicit val BinaryIsCpcFriendly: CpcFriendly[Array[Byte]] = new CpcFriendly[Array[Byte]] {
    override def update(sketch: CpcSketch, input: Array[Byte]): Unit = sketch.update(input)
  }
}

// Based on CPC sketch (a faster, smaller and more accurate version of HLL)
// See: Back to the future: an even more nearly optimal cardinality estimation algorithm, 2017
// https://arxiv.org/abs/1708.06839
// refer to the chart here to tune your sketch size with lgK
// https://github.com/apache/incubator-datasketches-java/blob/master/src/main/java/org/apache/datasketches/cpc/CpcSketch.java#L180
// default is about 1200 bytes
class ApproxDistinctCount[Input: CpcFriendly](lgK: Int = 8) extends SimpleAggregator[Input, CpcSketch, Long] {
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

  override def clone(ir: I): I = ir
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
