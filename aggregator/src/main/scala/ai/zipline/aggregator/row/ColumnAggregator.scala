package ai.zipline.aggregator.row

import ai.zipline.aggregator.base._
import ai.zipline.api.Config.{AggregationPart, AggregationType}

abstract class ColumnAggregator extends Serializable {
  def outputType: DataType

  def irType: DataType

  def update(ir: Array[Any], inputRow: Row): Unit

  // ir1 is mutated, ir2 isn't
  def merge(ir1: Any, ir2: Any): Any

  def finalize(ir: Any): Any

  def delete(ir: Array[Any], inputRow: Row): Unit

  def isDeletable: Boolean

  // convert custom java/scala class types to serializable types for the external system
  def normalize(ir: Any): Any

  def denormalize(ir: Any): Any

  def clone(ir: Any): Any

  // used for constructing windowed aggregates from unwindowed aggregations
  def mergeRedirected(ir1: Array[Any], ir2: Array[Any], ir2Index: Int): Unit
}

abstract class BaseColumnAggregator[Input, IR, Output](agg: BaseAggregator[Input, IR, Output],
                                                       columnIndices: ColumnIndices)
    extends ColumnAggregator {
  override def outputType: DataType = agg.outputType

  override def irType: DataType = agg.irType

  override def merge(ir1: Any, ir2: Any): Any = {
    if (ir2 == null) return ir1
    if (ir1 == null) return agg.clone(ir2.asInstanceOf[IR])
    agg.merge(ir1.asInstanceOf[IR], ir2.asInstanceOf[IR])
  }

  override def mergeRedirected(
      ir1: Array[Any],
      ir2: Array[Any],
      ir2Index: Int
  ): Unit = {
    val irVal1 = ir1(columnIndices.output)
    val irVal2 = ir2(ir2Index)
    if (irVal2 == null) return
    val irVal2Cloned: IR = agg.clone(irVal2.asInstanceOf[IR])
    if (irVal1 == null) {
      ir1.update(columnIndices.output, irVal2Cloned)
      return
    }
    // both are non-null
    ir1.update(
      columnIndices.output,
      agg.merge(irVal1.asInstanceOf[IR], irVal2Cloned)
    )
  }

  override def finalize(ir: Any): Any = guardedApply(agg.finalize, ir)

  override def normalize(ir: Any): Any = guardedApply(agg.normalize, ir)

  override def denormalize(ir: Any): Any = if (ir == null) null else agg.denormalize(ir)

  override def clone(ir: Any): Any = guardedApply(agg.clone, ir)

  private def guardedApply(f: IR => Any, ir: Any): Any = if (ir == null) null else f(ir.asInstanceOf[IR])
}

case class ColumnIndices(input: Int, output: Int)

object ColumnAggregator {
  private def cast[T](any: Any): T = any.asInstanceOf[T]

  // does null checks and up casts types to feed into typed aggregators
  def fromSimple[Input, IR, Output](agg: SimpleAggregator[Input, IR, Output],
                                    columnIndices: ColumnIndices,
                                    toTypedInput: Any => Input = (cast[Input] _)): ColumnAggregator =
    new BaseColumnAggregator(agg, columnIndices) {

      override def update(ir: Array[Any], inputRow: Row): Unit = {
        val inputVal = inputRow.get(columnIndices.input)
        if (inputVal == null) return
        val previousVal = ir(columnIndices.output)
        if (previousVal == null) {
          ir.update(columnIndices.output, agg.prepare(toTypedInput(inputVal)))
          return
        }
        val previous = previousVal.asInstanceOf[IR]
        val input = toTypedInput(inputVal)
        val updated = agg.update(previous, input)
        ir.update(columnIndices.output, updated)
      }

      override def delete(ir: Array[Any], inputRow: Row): Unit = {
        if (!agg.isDeletable) return
        val inputVal = inputRow.get(columnIndices.input)
        if (inputVal == null) return
        val previousVal = ir(columnIndices.output)
        if (previousVal == null) {
          ir.update(columnIndices.output, agg.prepare(toTypedInput(inputVal)))
          return
        }
        val previous = previousVal.asInstanceOf[IR]
        val input = toTypedInput(inputVal)
        val updated = agg.delete(previous, input)
        ir.update(columnIndices.output, updated)
      }

      override def isDeletable: Boolean = agg.isDeletable
    }

  def fromTimed[Input, IR, Output](
      agg: TimedAggregator[Input, IR, Output],
      columnIndices: ColumnIndices
  ): ColumnAggregator =
    new BaseColumnAggregator(agg, columnIndices) {

      override def update(ir: Array[Any], inputRow: Row): Unit = {
        val inputVal = inputRow.get(columnIndices.input)
        if (inputVal == null) return
        val previousVal = ir(columnIndices.output)
        if (previousVal == null) {
          ir.update(
            columnIndices.output,
            agg.prepare(cast[Input](inputVal), inputRow.ts)
          )
          return
        }
        val previous = previousVal.asInstanceOf[IR]
        val input = cast[Input](inputVal)
        val updated = agg.update(previous, input, inputRow.ts)
        ir.update(columnIndices.output, updated)
      }

      // timed aggregators are not deletable - they assume implicit time ordering
      override def delete(ir: Array[Any], inputRow: Row): Unit = {}

      override def isDeletable: Boolean = false
    }

  // to forced numeric widening
  private def toDouble[A: Numeric](inp: Any) = implicitly[Numeric[A]].toDouble(inp.asInstanceOf[A])
  private def toLong[A: Numeric](inp: Any) = implicitly[Numeric[A]].toLong(inp.asInstanceOf[A])

  def construct(inputType: DataType,
                aggregationPart: AggregationPart,
                columnIndices: ColumnIndices): ColumnAggregator = {
    def mismatchException =
      throw new UnsupportedOperationException(s"$inputType is incompatible with ${aggregationPart.`type`}")
    aggregationPart.`type` match {
      case AggregationType.Count => fromSimple(new Count, columnIndices)
      case AggregationType.Sum =>
        inputType match {
          case IntType    => fromSimple(new Sum[Long], columnIndices, toLong[Int])
          case LongType   => fromSimple(new Sum[Long], columnIndices)
          case ShortType  => fromSimple(new Sum[Long], columnIndices, toLong[Short])
          case DoubleType => fromSimple(new Sum[Double], columnIndices)
          case FloatType  => fromSimple(new Sum[Double], columnIndices, toDouble[Float])
          case _          => mismatchException
        }
      case AggregationType.ApproxDistinctCount =>
        inputType match {
          case IntType    => fromSimple(new ApproxDistinctCount[Long], columnIndices, toLong[Int])
          case LongType   => fromSimple(new ApproxDistinctCount[Long], columnIndices)
          case ShortType  => fromSimple(new ApproxDistinctCount[Long], columnIndices, toLong[Short])
          case DoubleType => fromSimple(new ApproxDistinctCount[Double], columnIndices)
          case FloatType  => fromSimple(new ApproxDistinctCount[Double], columnIndices, toDouble[Float])
          case StringType => fromSimple(new ApproxDistinctCount[String], columnIndices)
          case BinaryType => fromSimple(new ApproxDistinctCount[Array[Byte]], columnIndices)
          case _          => mismatchException
        }
      case AggregationType.Average =>
        inputType match {
          case IntType    => fromSimple(new Average, columnIndices, toDouble[Int])
          case LongType   => fromSimple(new Average, columnIndices, toDouble[Long])
          case ShortType  => fromSimple(new Average, columnIndices, toDouble[Short])
          case DoubleType => fromSimple(new Average, columnIndices)
          case FloatType  => fromSimple(new Average, columnIndices, toDouble[Float])
          case _          => mismatchException
        }
      case AggregationType.Min =>
        inputType match {
          case IntType    => fromSimple(new Min[Int](inputType), columnIndices)
          case LongType   => fromSimple(new Min[Long](inputType), columnIndices)
          case ShortType  => fromSimple(new Min[Short](inputType), columnIndices)
          case DoubleType => fromSimple(new Min[Double](inputType), columnIndices)
          case FloatType  => fromSimple(new Min[Float](inputType), columnIndices)
          case StringType => fromSimple(new Min[String](inputType), columnIndices)
          case _          => mismatchException
        }
      case AggregationType.Max =>
        inputType match {
          case IntType    => fromSimple(new Max[Int](inputType), columnIndices)
          case LongType   => fromSimple(new Max[Long](inputType), columnIndices)
          case ShortType  => fromSimple(new Max[Short](inputType), columnIndices)
          case DoubleType => fromSimple(new Max[Double](inputType), columnIndices)
          case FloatType  => fromSimple(new Max[Float](inputType), columnIndices)
          case StringType => fromSimple(new Max[String](inputType), columnIndices)
          case _          => mismatchException
        }
      case AggregationType.TopK =>
        val k = aggregationPart.getInt("k")
        inputType match {
          case IntType    => fromSimple(new TopK[Int](inputType, k), columnIndices)
          case LongType   => fromSimple(new TopK[Long](inputType, k), columnIndices)
          case ShortType  => fromSimple(new TopK[Short](inputType, k), columnIndices)
          case DoubleType => fromSimple(new TopK[Double](inputType, k), columnIndices)
          case FloatType  => fromSimple(new TopK[Float](inputType, k), columnIndices)
          case StringType => fromSimple(new TopK[String](inputType, k), columnIndices)
          case _          => mismatchException
        }
      case AggregationType.BottomK =>
        val k = aggregationPart.getInt("k")
        inputType match {
          case IntType    => fromSimple(new BottomK[Int](inputType, k), columnIndices)
          case LongType   => fromSimple(new BottomK[Long](inputType, k), columnIndices)
          case ShortType  => fromSimple(new BottomK[Short](inputType, k), columnIndices)
          case DoubleType => fromSimple(new BottomK[Double](inputType, k), columnIndices)
          case FloatType  => fromSimple(new BottomK[Float](inputType, k), columnIndices)
          case StringType => fromSimple(new BottomK[String](inputType, k), columnIndices)
          case _          => mismatchException
        }
      case AggregationType.First  => fromTimed(new First(inputType), columnIndices)
      case AggregationType.Last   => fromTimed(new Last(inputType), columnIndices)
      case AggregationType.FirstK => fromTimed(new FirstK(inputType, aggregationPart.getInt("k")), columnIndices)
      case AggregationType.LastK  => fromTimed(new LastK(inputType, aggregationPart.getInt("k")), columnIndices)
    }
  }
}
