/*
 *    Copyright (C) 2023 The Chronon Authors.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package ai.chronon.aggregator.base

import ai.chronon.api._
import com.yahoo.memory.Memory
import com.yahoo.sketches.cpc.{CpcSketch, CpcUnion}
import com.yahoo.sketches.frequencies.{ErrorType, ItemsSketch}
import com.yahoo.sketches.kll.KllFloatsSketch
import com.yahoo.sketches.{ArrayOfDoublesSerDe, ArrayOfItemsSerDe, ArrayOfLongsSerDe, ArrayOfStringsSerDe}

import java.{lang, util}
import scala.collection.mutable
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

// Welford algo for computing variance
// Traditional sum of squares based formula has serious numerical stability problems
class WelfordState(ir: Array[Any]) {
  private def count: Int = ir(0).asInstanceOf[Int]
  private def setCount(c: Int): Unit = ir.update(0, c)
  private def mean: Double = ir(1).asInstanceOf[Double]
  private def setMean(m: Double): Unit = ir.update(1, m)
  private def m2: Double = ir(2).asInstanceOf[Double]
  private def setM2(m: Double): Unit = ir.update(2, m)

  private def getCombinedMeanDouble(weightN: Double, an: Double, weightK: Double, ak: Double): Double =
    if (weightN < weightK) getCombinedMeanDouble(weightK, ak, weightN, an)
    else
      (weightN + weightK) match {
        case 0.0                             => 0.0
        case newCount if newCount == weightN => an
        case newCount =>
          val scaling = weightK / newCount
          if (scaling < 0.1) (an + (ak - an) * scaling)
          else (weightN * an + weightK * ak) / newCount
      }

  // See http://en.wikipedia.org/wiki/Algorithms_for_calculating_variance#Parallel_algorithm
  def update(input: Double): Unit = {
    setCount(count + 1)
    val delta = input - mean
    setMean(mean + (delta / count))
    val delta2 = input - mean
    setM2(m2 + (delta * delta2))
  }

  def finalizeImpl(): Double = m2 / count

  // See http://en.wikipedia.org/wiki/Algorithms_for_calculating_variance#Higher-order_statistics
  def merge(other: WelfordState): Unit = {
    val countCombined = count + other.count
    val delta = other.mean - mean
    val delta_n = delta / countCombined
    val meanCombined = getCombinedMeanDouble(count, mean, other.count, other.mean)
    val m2Combined = m2 + other.m2 + (delta * delta_n * count * other.count)
    setCount(countCombined)
    setMean(meanCombined)
    setM2(m2Combined)
  }
}

object WelfordState {
  def init: Array[Any] = Array(0, 0.0, 0.0)
}

class Variance extends SimpleAggregator[Double, Array[Any], Double] {
  override def outputType: DataType = DoubleType

  override def irType: DataType =
    StructType(
      "VarianceIr",
      Array(StructField("count", IntType), StructField("mean", DoubleType), StructField("m2", DoubleType))
    )

  override def prepare(input: Double): Array[Any] = {
    val ir = WelfordState.init
    new WelfordState(ir).update(input)
    ir
  }

  override def update(ir: Array[Any], input: Double): Array[Any] = {
    new WelfordState(ir).update(input)
    ir
  }

  override def merge(ir1: Array[Any], ir2: Array[Any]): Array[Any] = {
    new WelfordState(ir1).merge(new WelfordState(ir2))
    ir1
  }

  override def finalize(ir: Array[Any]): Double =
    new WelfordState(ir).finalizeImpl()

  override def clone(ir: Array[Any]): Array[Any] = {
    val arr = new Array[Any](ir.length)
    ir.copyToArray(arr)
    arr
  }

  override def isDeletable: Boolean = false
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

trait CpcFriendly[Input] extends Serializable {
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

case class ItemsSketchIR[T](sketch: ItemsSketch[T], sketchType: Int)

trait FrequentItemsFriendly[Input] {
  def serializer: ArrayOfItemsSerDe[Input]
  def sketchType: Int
}

object FrequentItemsFriendly {
  val StringItemType: Int = 1
  val LongItemType: Int = 2
  val DoubleItemType: Int = 3

  implicit val stringIsFrequentItemsFriendly: FrequentItemsFriendly[String] = new FrequentItemsFriendly[String] {
    override def serializer: ArrayOfItemsSerDe[String] = new ArrayOfStringsSerDe
    override def sketchType: Int = StringItemType
  }

  implicit val longIsFrequentItemsFriendly: FrequentItemsFriendly[java.lang.Long] =
    new FrequentItemsFriendly[java.lang.Long] {
      override def serializer: ArrayOfItemsSerDe[java.lang.Long] = new ArrayOfLongsSerDe
      override def sketchType: Int = LongItemType
    }

  implicit val doubleIsFrequentItemsFriendly: FrequentItemsFriendly[java.lang.Double] =
    new FrequentItemsFriendly[java.lang.Double] {
      override def serializer: ArrayOfItemsSerDe[java.lang.Double] = new ArrayOfDoublesSerDe
      override def sketchType: Int = DoubleItemType
    }
}

class FrequentItems[T: FrequentItemsFriendly](val mapSize: Int, val errorType: ErrorType = ErrorType.NO_FALSE_POSITIVES)
    extends SimpleAggregator[T, ItemsSketchIR[T], Map[T, Long]] {
  private type Sketch = ItemsSketchIR[T]

  // The ItemsSketch implementation requires a size with a positive power of 2
  // Initialize the sketch with the next closest power of 2
  val sketchSize: Int = if (mapSize > 1) Integer.highestOneBit(mapSize - 1) << 1 else 2

  override def outputType: DataType = MapType(StringType, IntType)

  override def irType: DataType = BinaryType

  override def prepare(input: T): Sketch = {
    val sketch = new ItemsSketch[T](sketchSize)
    val sketchType = implicitly[FrequentItemsFriendly[T]].sketchType
    sketch.update(input)
    ItemsSketchIR(sketch, sketchType)
  }

  override def update(ir: Sketch, input: T): Sketch = {
    ir.sketch.update(input)
    ir
  }
  override def merge(ir1: Sketch, ir2: Sketch): Sketch = {
    ir1.sketch.merge(ir2.sketch)
    ir1
  }

  // ItemsSketch doesn't have a proper copy method. So we serialize and deserialize.
  override def clone(ir: Sketch): Sketch = {
    val serializer = implicitly[FrequentItemsFriendly[T]].serializer
    val bytes = ir.sketch.toByteArray(serializer)
    val clonedSketch = ItemsSketch.getInstance(Memory.wrap(bytes), serializer)
    ItemsSketchIR(clonedSketch, ir.sketchType)
  }

  override def finalize(ir: Sketch): Map[T, Long] = {
    if (mapSize == 0) {
      return Map.empty
    }

    val items = ir.sketch.getFrequentItems(errorType).map(sk => sk.getItem -> sk.getEstimate)
    val heap = mutable.PriorityQueue[(T, Long)]()(Ordering.by(_._2))

    items.foreach({
      case (key, value) =>
        if (heap.size < mapSize) {
          heap.enqueue((key, value))
        } else if (heap.head._2 < value) {
          heap.dequeue()
          heap.enqueue((key, value))
        }
    })

    heap.dequeueAll.toMap
  }

  override def normalize(ir: Sketch): Array[Byte] = {
    val serializer = implicitly[FrequentItemsFriendly[T]].serializer
    (Seq(ir.sketchType.byteValue()) ++ ir.sketch.toByteArray(serializer)).toArray
  }

  override def denormalize(normalized: Any): Sketch = {
    val bytes = normalized.asInstanceOf[Array[Byte]]
    val sketchType = bytes.head
    val serializer = implicitly[FrequentItemsFriendly[T]].serializer
    val sketch = ItemsSketch.getInstance[T](Memory.wrap(bytes.tail), serializer)
    ItemsSketchIR(sketch, sketchType)
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

  override def bulkMerge(irs: Iterator[CpcSketch]): CpcSketch = {
    val merger = new CpcUnion(lgK)
    irs.foreach(merger.update)
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

class ApproxPercentiles(k: Int = 128, percentiles: Array[Double] = Array(0.5))
    extends SimpleAggregator[Float, KllFloatsSketch, Array[Float]] {
  override def outputType: DataType = ListType(FloatType)

  override def irType: DataType = BinaryType

  override def prepare(input: Float): KllFloatsSketch = {
    val sketch = new KllFloatsSketch(k)
    sketch.update(input)
    sketch
  }

  override def update(ir: KllFloatsSketch, input: Float): KllFloatsSketch = {
    ir.update(input)
    ir
  }

  override def merge(ir1: KllFloatsSketch, ir2: KllFloatsSketch): KllFloatsSketch = {
    ir1.merge(ir2)
    ir1
  }

  override def bulkMerge(irs: Iterator[KllFloatsSketch]): KllFloatsSketch = {
    val result = new KllFloatsSketch(k)
    irs.foreach(result.merge)
    result
  }

  // KLLFloatsketch doesn't have a proper copy method. So we serialize and deserialize.
  override def clone(ir: KllFloatsSketch): KllFloatsSketch = {
    KllFloatsSketch.heapify(Memory.wrap(ir.toByteArray))
  }

  // Produce percentile values for the specified percentiles ex: [0.1, 0.5, 0.95]
  override def finalize(ir: KllFloatsSketch): Array[Float] = ir.getQuantiles(percentiles)

  override def normalize(ir: KllFloatsSketch): Array[Byte] = ir.toByteArray

  override def denormalize(normalized: Any): KllFloatsSketch =
    KllFloatsSketch.heapify(Memory.wrap(normalized.asInstanceOf[Array[Byte]]))
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
