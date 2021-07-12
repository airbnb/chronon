package ai.zipline.aggregator.row

import ai.zipline.aggregator.base._
import ai.zipline.api.Extensions._
import ai.zipline.api.{AggregationPart, Operation}

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
}

// implementations to assume nulls have been filtered out before calling
trait Dispatcher[Input, IR] {
  def prepare(inputRow: Row): IR
  def updateColumn(ir: IR, inputRow: Row): IR
  def inversePrepare(inputRow: Row): IR
  def deleteColumn(ir: IR, inputRow: Row): IR
}

class SimpleDispatcher[Input, IR](agg: SimpleAggregator[Input, IR, _],
                                  columnIndices: ColumnIndices,
                                  toTypedInput: Any => Input)
    extends Dispatcher[Input, IR]
    with Serializable {
  override def prepare(inputRow: Row): IR =
    agg.prepare(toTypedInput(inputRow.get(columnIndices.input)))

  override def inversePrepare(inputRow: Row): IR =
    agg.inversePrepare(toTypedInput(inputRow.get(columnIndices.input)))

  override def deleteColumn(ir: IR, inputRow: Row): IR =
    agg.delete(ir, toTypedInput(inputRow.get(columnIndices.input)))

  override def updateColumn(ir: IR, inputRow: Row): IR =
    agg.update(ir, toTypedInput(inputRow.get(columnIndices.input)))
}

class TimedDispatcher[Input, IR](agg: TimedAggregator[Input, IR, _], columnIndices: ColumnIndices)
    extends Dispatcher[Input, IR] {
  override def prepare(inputRow: Row): IR =
    agg.prepare(inputRow.get(columnIndices.input).asInstanceOf[Input], inputRow.ts)

  override def updateColumn(ir: IR, inputRow: Row): IR =
    agg.update(ir, inputRow.get(columnIndices.input).asInstanceOf[Input], inputRow.ts)

  override def inversePrepare(inputRow: Row): IR = ???

  override def deleteColumn(ir: IR, inputRow: Row): IR = ???
}

case class ColumnIndices(input: Int, output: Int)

object ColumnAggregator {
  private def cast[T](any: Any): T = any.asInstanceOf[T]

  // does null checks and up casts types to feed into typed aggregators
  // by the time we call underlying aggregators there should be no nulls left to handle
  def fromSimple[Input, IR, Output](agg: SimpleAggregator[Input, IR, Output],
                                    columnIndices: ColumnIndices,
                                    toTypedInput: Any => Input,
                                    bucketIndex: Option[Int] = None): ColumnAggregator = {
    val dispatcher = new SimpleDispatcher(agg, columnIndices, toTypedInput)
    if (bucketIndex.isEmpty) {
      new DirectColumnAggregator(agg, columnIndices, dispatcher)
    } else {
      new BucketedColumnAggregator(agg, columnIndices, bucketIndex.get, dispatcher)
    }
  }

  def fromTimed[Input, IR, Output](
      agg: TimedAggregator[Input, IR, Output],
      columnIndices: ColumnIndices,
      bucketIndex: Option[Int] = None
  ): ColumnAggregator = {
    val dispatcher = new TimedDispatcher(agg, columnIndices)
    if (bucketIndex.isEmpty) {
      new DirectColumnAggregator(agg, columnIndices, dispatcher)
    } else {
      new BucketedColumnAggregator(agg, columnIndices, bucketIndex.get, dispatcher)
    }
  }

  //force numeric widening
  private def toDouble[A: Numeric](inp: Any) = implicitly[Numeric[A]].toDouble(inp.asInstanceOf[A])
  private def toLong[A: Numeric](inp: Any) = implicitly[Numeric[A]].toLong(inp.asInstanceOf[A])

  def construct(inputType: DataType,
                aggregationPart: AggregationPart,
                columnIndices: ColumnIndices,
                bucketIndex: Option[Int]): ColumnAggregator = {
    def mismatchException =
      throw new UnsupportedOperationException(s"$inputType is incompatible with ${aggregationPart.operation}")

    def simple[Input, IR, Output](agg: SimpleAggregator[Input, IR, Output],
                                  toTypedInput: Any => Input = (cast[Input] _)): ColumnAggregator = {
      fromSimple(agg, columnIndices, toTypedInput, bucketIndex)
    }

    def timed[Input, IR, Output](agg: TimedAggregator[Input, IR, Output]): ColumnAggregator = {
      fromTimed(agg, columnIndices, bucketIndex)
    }

    aggregationPart.operation match {
      case Operation.COUNT => simple(new Count)
      case Operation.SUM =>
        inputType match {
          case IntType    => simple(new Sum[Long], toLong[Int])
          case LongType   => simple(new Sum[Long])
          case ShortType  => simple(new Sum[Long], toLong[Short])
          case DoubleType => simple(new Sum[Double])
          case FloatType  => simple(new Sum[Double], toDouble[Float])
          case _          => mismatchException
        }
      case Operation.UNIQUE_COUNT =>
        inputType match {
          case IntType    => simple(new UniqueCount[Int](inputType))
          case LongType   => simple(new UniqueCount[Long](inputType))
          case ShortType  => simple(new UniqueCount[Short](inputType))
          case DoubleType => simple(new UniqueCount[Double](inputType))
          case FloatType  => simple(new UniqueCount[Float](inputType))
          case StringType => simple(new UniqueCount[String](inputType))
          case BinaryType => simple(new UniqueCount[Array[Byte]](inputType))
          case _          => mismatchException
        }
      case Operation.APPROX_UNIQUE_COUNT =>
        inputType match {
          case IntType    => simple(new ApproxDistinctCount[Long], toLong[Int])
          case LongType   => simple(new ApproxDistinctCount[Long])
          case ShortType  => simple(new ApproxDistinctCount[Long], toLong[Short])
          case DoubleType => simple(new ApproxDistinctCount[Double])
          case FloatType  => simple(new ApproxDistinctCount[Double], toDouble[Float])
          case StringType => simple(new ApproxDistinctCount[String])
          case BinaryType => simple(new ApproxDistinctCount[Array[Byte]])
          case _          => mismatchException
        }
      case Operation.AVERAGE =>
        inputType match {
          case IntType    => simple(new Average, toDouble[Int])
          case LongType   => simple(new Average, toDouble[Long])
          case ShortType  => simple(new Average, toDouble[Short])
          case DoubleType => simple(new Average)
          case FloatType  => simple(new Average, toDouble[Float])
          case _          => mismatchException
        }
      case Operation.MIN =>
        inputType match {
          case IntType    => simple(new Min[Int](inputType))
          case LongType   => simple(new Min[Long](inputType))
          case ShortType  => simple(new Min[Short](inputType))
          case DoubleType => simple(new Min[Double](inputType))
          case FloatType  => simple(new Min[Float](inputType))
          case StringType => simple(new Min[String](inputType))
          case _          => mismatchException
        }
      case Operation.MAX =>
        inputType match {
          case IntType    => simple(new Max[Int](inputType))
          case LongType   => simple(new Max[Long](inputType))
          case ShortType  => simple(new Max[Short](inputType))
          case DoubleType => simple(new Max[Double](inputType))
          case FloatType  => simple(new Max[Float](inputType))
          case StringType => simple(new Max[String](inputType))
          case _          => mismatchException
        }
      case Operation.TOP_K =>
        val k = aggregationPart.getInt("k")
        inputType match {
          case IntType    => simple(new TopK[Int](inputType, k))
          case LongType   => simple(new TopK[Long](inputType, k))
          case ShortType  => simple(new TopK[Short](inputType, k))
          case DoubleType => simple(new TopK[Double](inputType, k))
          case FloatType  => simple(new TopK[Float](inputType, k))
          case StringType => simple(new TopK[String](inputType, k))
          case _          => mismatchException
        }
      case Operation.BOTTOM_K =>
        val k = aggregationPart.getInt("k")
        inputType match {
          case IntType    => simple(new BottomK[Int](inputType, k))
          case LongType   => simple(new BottomK[Long](inputType, k))
          case ShortType  => simple(new BottomK[Short](inputType, k))
          case DoubleType => simple(new BottomK[Double](inputType, k))
          case FloatType  => simple(new BottomK[Float](inputType, k))
          case StringType => simple(new BottomK[String](inputType, k))
          case _          => mismatchException
        }
      case Operation.FIRST   => timed(new First(inputType))
      case Operation.LAST    => timed(new Last(inputType))
      case Operation.FIRST_K => timed(new FirstK(inputType, aggregationPart.getInt("k")))
      case Operation.LAST_K  => timed(new LastK(inputType, aggregationPart.getInt("k")))
    }
  }
}
