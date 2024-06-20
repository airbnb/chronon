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

package ai.chronon.aggregator.row

import ai.chronon.aggregator.base._
import ai.chronon.api.Extensions.{AggregationPartOps, OperationOps}
import ai.chronon.api._
import com.fasterxml.jackson.databind.ObjectMapper

import java.util
import scala.collection.JavaConverters.asScalaIteratorConverter
import scala.util.ScalaJavaConversions.IteratorOps

abstract class ColumnAggregator extends Serializable {
  def outputType: DataType

  def irType: DataType

  def update(ir: Array[Any], inputRow: Row): Unit

  // ir1 is mutated, ir2 isn't
  def merge(ir1: Any, ir2: Any): Any

  def bulkMerge(irs: Iterator[Any]): Any = irs.reduce(merge)

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
    extends Dispatcher[Input, Any]
    with Serializable {
  override def prepare(inputRow: Row): IR =
    agg.prepare(toTypedInput(inputRow.get(columnIndices.input)))

  override def inversePrepare(inputRow: Row): IR =
    agg.inversePrepare(toTypedInput(inputRow.get(columnIndices.input)))

  override def deleteColumn(ir: Any, inputRow: Row): IR =
    agg.delete(ir.asInstanceOf[IR], toTypedInput(inputRow.get(columnIndices.input)))

  override def updateColumn(ir: Any, inputRow: Row): IR =
    agg.update(ir.asInstanceOf[IR], toTypedInput(inputRow.get(columnIndices.input)))
}

class VectorDispatcher[Input, IR](agg: SimpleAggregator[Input, IR, _],
                                  columnIndices: ColumnIndices,
                                  toTypedInput: Any => Input)
    extends Dispatcher[Input, Any]
    with Serializable {

  def toInputIterator(inputRow: Row): Iterator[Input] = {
    val inputVal = inputRow.get(columnIndices.input)
    if (inputVal == null) return null
    val anyIterator = inputVal match {
      case inputSeq: collection.Seq[Any]  => inputSeq.iterator
      case inputList: util.ArrayList[Any] => inputList.iterator().asScala
    }
    anyIterator.filter { _ != null }.map { toTypedInput }
  }

  def guardedApply(inputRow: Row, prepare: Input => IR, update: (IR, Input) => IR, baseIr: Any = null): Any = {
    val it = toInputIterator(inputRow)
    if (it == null) return baseIr
    var result = baseIr
    while (it.hasNext) {
      if (result == null) {
        result = prepare(it.next())
      } else {
        result = update(result.asInstanceOf[IR], it.next())
      }
    }
    result
  }
  override def prepare(inputRow: Row): Any = guardedApply(inputRow, agg.prepare, agg.update)

  override def updateColumn(ir: Any, inputRow: Row): Any = guardedApply(inputRow, agg.prepare, agg.update, ir)

  override def inversePrepare(inputRow: Row): Any = guardedApply(inputRow, agg.inversePrepare, agg.delete)

  override def deleteColumn(ir: Any, inputRow: Row): Any = guardedApply(inputRow, agg.inversePrepare, agg.delete, ir)

}

class TimedDispatcher[Input, IR](agg: TimedAggregator[Input, IR, _], columnIndices: ColumnIndices)
    extends Dispatcher[Input, Any] {
  override def prepare(inputRow: Row): IR =
    agg.prepare(inputRow.get(columnIndices.input).asInstanceOf[Input], inputRow.ts)

  override def updateColumn(ir: Any, inputRow: Row): IR =
    agg.update(ir.asInstanceOf[IR], inputRow.get(columnIndices.input).asInstanceOf[Input], inputRow.ts)

  override def inversePrepare(inputRow: Row): IR = ???

  override def deleteColumn(ir: Any, inputRow: Row): IR = ???
}

case class ColumnIndices(input: Int, output: Int)

object ColumnAggregator {

  def castToLong(value: AnyRef): AnyRef =
    value match {
      case i: java.lang.Integer => new java.lang.Long(i.longValue())
      case i: java.lang.Short   => new java.lang.Long(i.longValue())
      case i: java.lang.Byte    => new java.lang.Long(i.longValue())
      case i: java.lang.Double  => new java.lang.Long(i.longValue())
      case i: java.lang.Float   => new java.lang.Long(i.longValue())
      case i: java.lang.String  => new java.lang.Long(java.lang.Long.parseLong(i))
      case _                    => value
    }

  def castToDouble(value: AnyRef): AnyRef =
    value match {
      case i: java.lang.Integer => new java.lang.Double(i.doubleValue())
      case i: java.lang.Short   => new java.lang.Double(i.doubleValue())
      case i: java.lang.Byte    => new java.lang.Double(i.doubleValue())
      case i: java.lang.Float   => new java.lang.Double(i.doubleValue())
      case i: java.lang.Long    => new java.lang.Double(i.doubleValue())
      case i: java.lang.String  => new java.lang.Double(java.lang.Double.parseDouble(i))
      case _                    => value
    }

  def castTo(value: AnyRef, typ: DataType): AnyRef =
    typ match {
      // TODO this might need more type handling
      case LongType   => castToLong(value)
      case DoubleType => castToDouble(value)
      case _          => value
    }

  private def cast[T](any: Any): T = any.asInstanceOf[T]

  // does null checks and up casts types to feed into typed aggregators
  // by the time we call underlying aggregators there should be no nulls left to handle
  def fromSimple[Input, IR, Output](agg: SimpleAggregator[Input, IR, Output],
                                    columnIndices: ColumnIndices,
                                    toTypedInput: Any => Input,
                                    bucketIndex: Option[Int] = None,
                                    isVector: Boolean = false,
                                    isMap: Boolean = false): ColumnAggregator = {

    assert(!(isVector && isMap), "Input column cannot simultaneously be map or vector")
    val dispatcher = if (isVector) {
      new VectorDispatcher(agg, columnIndices, toTypedInput)
    } else {
      new SimpleDispatcher(agg, columnIndices, toTypedInput)
    }

    // TODO: remove the below assertion and add support
    assert(!(isMap && bucketIndex.isDefined), "Bucketing over map columns is currently unsupported")
    if (isMap) {
      new MapColumnAggregator(agg, columnIndices, toTypedInput)
    } else if (bucketIndex.isDefined) {
      new BucketedColumnAggregator(agg, columnIndices, bucketIndex.get, dispatcher)
    } else {
      new DirectColumnAggregator(agg, columnIndices, dispatcher)
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

  private def toDouble[A: Numeric](inp: Any) = implicitly[Numeric[A]].toDouble(inp.asInstanceOf[A])
  private def toFloat[A: Numeric](inp: Any): Float = implicitly[Numeric[A]].toFloat(inp.asInstanceOf[A])
  private def toLong[A: Numeric](inp: Any) = implicitly[Numeric[A]].toLong(inp.asInstanceOf[A])
  private def boolToLong(inp: Any): Long = if (inp.asInstanceOf[Boolean]) 1 else 0
  private def toJavaLong[A: Numeric](inp: Any) =
    implicitly[Numeric[A]].toLong(inp.asInstanceOf[A]).asInstanceOf[java.lang.Long]
  private def toJavaDouble[A: Numeric](inp: Any) =
    implicitly[Numeric[A]].toDouble(inp.asInstanceOf[A]).asInstanceOf[java.lang.Double]

  def construct(baseInputType: DataType,
                aggregationPart: AggregationPart,
                columnIndices: ColumnIndices,
                bucketIndex: Option[Int]): ColumnAggregator = {
    baseInputType match {
      case DateType | TimestampType =>
        throw new IllegalArgumentException(
          s"Error while aggregating over '${aggregationPart.inputColumn}'. " +
            s"Date type and Timestamp time should not be aggregated over (They don't serialize well in avro either). " +
            s"Please use Query's Select expressions to transform them into Long.")
      case _ =>
    }

    def mismatchException =
      throw new UnsupportedOperationException(s"$baseInputType is incompatible with ${aggregationPart.operation}")

    // to support vector aggregations when input column is an array.
    // avg of [1, 2, 3], [3, 4], [5] = 18 / 6 => 3
    val vectorElementType: Option[DataType] = (aggregationPart.operation.isSimple, baseInputType) match {
      case (true, ListType(elementType)) if DataType.isScalar(elementType) => Some(elementType)
      case _                                                               => None
    }

    val mapElementType: Option[DataType] = (aggregationPart.operation.isSimple, baseInputType) match {
      case (true, MapType(StringType, elementType)) => Some(elementType)
      case _                                        => None
    }
    val inputType = (mapElementType ++ vectorElementType ++ Some(baseInputType)).head

    def simple[Input, IR, Output](agg: SimpleAggregator[Input, IR, Output],
                                  toTypedInput: Any => Input = cast[Input] _): ColumnAggregator = {
      fromSimple(agg,
                 columnIndices,
                 toTypedInput,
                 bucketIndex,
                 isVector = vectorElementType.isDefined,
                 isMap = mapElementType.isDefined)
    }

    def timed[Input, IR, Output](agg: TimedAggregator[Input, IR, Output]): ColumnAggregator = {
      fromTimed(agg, columnIndices, bucketIndex)
    }

    aggregationPart.operation match {
      case Operation.COUNT     => simple(new Count)
      case Operation.HISTOGRAM => simple(new Histogram(aggregationPart.getInt("k", Some(0))))
      case Operation.APPROX_HISTOGRAM_K =>
        val k = aggregationPart.getInt("k", Some(8))
        inputType match {
          case IntType    => simple(new ApproxHistogram[java.lang.Long](k), toJavaLong[Int])
          case LongType   => simple(new ApproxHistogram[java.lang.Long](k))
          case ShortType  => simple(new ApproxHistogram[java.lang.Long](k), toJavaLong[Short])
          case DoubleType => simple(new ApproxHistogram[java.lang.Double](k))
          case FloatType  => simple(new ApproxHistogram[java.lang.Double](k), toJavaDouble[Float])
          case StringType => simple(new ApproxHistogram[String](k))
          case _          => mismatchException
        }
      case Operation.SUM =>
        inputType match {
          case IntType     => simple(new Sum[Long](LongType), toLong[Int])
          case LongType    => simple(new Sum[Long](inputType))
          case ShortType   => simple(new Sum[Long](LongType), toLong[Short])
          case BooleanType => simple(new Sum[Long](LongType), boolToLong)
          case DoubleType  => simple(new Sum[Double](inputType))
          case FloatType   => simple(new Sum[Double](inputType), toDouble[Float])
          case _           => mismatchException
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
          case IntType    => simple(new ApproxDistinctCount[Long](aggregationPart.getInt("k", Some(8))), toLong[Int])
          case LongType   => simple(new ApproxDistinctCount[Long](aggregationPart.getInt("k", Some(8))))
          case ShortType  => simple(new ApproxDistinctCount[Long](aggregationPart.getInt("k", Some(8))), toLong[Short])
          case DoubleType => simple(new ApproxDistinctCount[Double](aggregationPart.getInt("k", Some(8))))
          case FloatType =>
            simple(new ApproxDistinctCount[Double](aggregationPart.getInt("k", Some(8))), toDouble[Float])
          case StringType => simple(new ApproxDistinctCount[String](aggregationPart.getInt("k", Some(8))))
          case BinaryType => simple(new ApproxDistinctCount[Array[Byte]](aggregationPart.getInt("k", Some(8))))
          case _          => mismatchException
        }

      case Operation.APPROX_PERCENTILE =>
        val k = aggregationPart.getInt("k", Some(128))
        val mapper = new ObjectMapper()
        val percentiles =
          mapper.readValue(aggregationPart.argMap.getOrDefault("percentiles", "[0.5]"), classOf[Array[Double]])
        val agg = new ApproxPercentiles(k, percentiles)
        inputType match {
          case IntType    => simple(agg, toFloat[Int])
          case LongType   => simple(agg, toFloat[Long])
          case DoubleType => simple(agg, toFloat[Double])
          case FloatType  => simple(agg)
          case ShortType  => simple(agg, toFloat[Short])
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

      case Operation.VARIANCE =>
        inputType match {
          case IntType    => simple(new Variance, toDouble[Int])
          case LongType   => simple(new Variance, toDouble[Long])
          case ShortType  => simple(new Variance, toDouble[Short])
          case DoubleType => simple(new Variance)
          case FloatType  => simple(new Variance, toDouble[Float])
          case _          => mismatchException
        }

      case Operation.SKEW =>
        inputType match {
          case IntType    => simple(new Skew, toDouble[Int])
          case LongType   => simple(new Skew, toDouble[Long])
          case ShortType  => simple(new Skew, toDouble[Short])
          case DoubleType => simple(new Skew)
          case FloatType  => simple(new Skew, toDouble[Float])
          case _          => mismatchException
        }

      case Operation.KURTOSIS =>
        inputType match {
          case IntType    => simple(new Kurtosis, toDouble[Int])
          case LongType   => simple(new Kurtosis, toDouble[Long])
          case ShortType  => simple(new Kurtosis, toDouble[Short])
          case DoubleType => simple(new Kurtosis)
          case FloatType  => simple(new Kurtosis, toDouble[Float])
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
