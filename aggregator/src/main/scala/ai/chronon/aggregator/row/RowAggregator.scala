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

import ai.chronon.aggregator.base.SimpleAggregator
import ai.chronon.api.Extensions.{AggregationPartOps, WindowOps}
import ai.chronon.api.{AggregationPart, DataType, Row, StringType}

import scala.collection.Seq

// The primary API of the aggregator package.
// the semantics are to mutate values in place for performance reasons
class RowAggregator(val inputSchema: Seq[(String, DataType)], val aggregationParts: Seq[AggregationPart])
    extends Serializable
    with SimpleAggregator[Row, Array[Any], Array[Any]] {

  val length: Int = aggregationParts.size
  val indices: Range = 0 until length
  // has to be array for fast random access
  val columnAggregators: Array[ColumnAggregator] = {
    aggregationParts.zipWithIndex.map {
      case (spec: AggregationPart, aggregatorIndex: Int) =>
        val ((_, inputType), inputIndex) = {
          inputSchema.zipWithIndex.find(_._1._1 == spec.inputColumn).get
        }

        val bucketIndex: Option[Int] = Option(spec.bucket).map { bucketCol =>
          val bIndex = inputSchema.indexWhere(_._1 == bucketCol)
          assert(bIndex != -1, s"bucketing column: $bucketCol is not found in input: ${inputSchema.map(_._1)}")
          val bucketType = inputSchema(bIndex)._2
          assert(bucketType == StringType, s"bucketing column: $bucketCol needs to be a string, but found $bucketType")
          bIndex
        }
        try {
          ColumnAggregator.construct(
            inputType,
            spec,
            ColumnIndices(inputIndex, aggregatorIndex),
            bucketIndex
          )
        } catch {
          case e: Exception =>
            throw new RuntimeException(
              s"Failed to create ${spec.operation} aggregator for ${spec.inputColumn} column of type $inputType",
              e)
        }
    }
  }.toArray

  def apply(index: Int): ColumnAggregator = columnAggregators(index)

  def init: Array[Any] = new Array[Any](aggregationParts.length)

  val irSchema: Array[(String, DataType)] = aggregationParts
    .map(_.outputColumnName)
    .toArray
    .zip(columnAggregators.map(_.irType))

  val outputSchema: Array[(String, DataType)] = aggregationParts
    .map(_.outputColumnName)
    .toArray
    .zip(columnAggregators.map(_.outputType))

  val isNotDeletable: Boolean = columnAggregators.forall(!_.isDeletable)

  // this will mutate in place
  def update(ir: Array[Any], inputRow: Row): Array[Any] = {
    var i = 0
    while (i < columnAggregators.length) {
      columnAggregators(i).update(ir, inputRow)
      i += 1
    }
    ir
  }

  def updateWithReturn(ir: Array[Any], inputRow: Row): Array[Any] = {
    update(ir, inputRow)
    ir
  }

  def updateWindowed(ir: Array[Any], inputRow: Row, endTime: Long): Unit = {
    var i = 0
    while (i < columnAggregators.length) {
      val ts = inputRow.ts
      val windowStart = endTime - aggregationParts(i).window.millis
      if (ts < endTime && ts >= windowStart)
        columnAggregators(i).update(ir, inputRow)
      i += 1
    }
  }

  def merge(ir1: Array[Any], ir2: Array[Any]): Array[Any] = {
    if (ir1 == null) return ir2
    if (ir2 == null) return ir1
    var i = 0
    while (i < columnAggregators.length) {
      ir1.update(i, columnAggregators(i).merge(ir1(i), ir2(i)))
      i += 1
    }
    ir1
  }

  def finalize(ir: Array[Any]): Array[Any] = map(ir, _.finalize)

  override def delete(ir: Array[Any], inputRow: Row): Array[Any] = {
    if (!isNotDeletable) {
      columnAggregators.foreach(_.delete(ir, inputRow))
    }
    ir
  }

  // convert arbitrary java types into types that
  // parquet/spark or any other external storage format understands
  // CPCSketch is the primary use-case now. We will need it for quantile estimation too I think
  override def normalize(ir: Array[Any]): Array[Any] = map(ir, _.normalize)
  // used by hops aggregator, hops have a timestamp in the end - which we want to preserve
  def normalizeInPlace(ir: Array[Any]): Array[Any] = {
    var idx = 0
    while (idx < columnAggregators.length) {
      ir.update(idx, columnAggregators(idx).normalize(ir(idx)))
      idx += 1
    }
    ir
  }

  def denormalize(ir: Array[Any]): Array[Any] = map(ir, _.denormalize)
  def denormalizeInPlace(ir: Array[Any]): Array[Any] = {
    var idx = 0
    while (idx < columnAggregators.length) {
      ir.update(idx, columnAggregators(idx).denormalize(ir(idx)))
      idx += 1
    }
    ir
  }

  // deep copies an IR ai.chronon.aggregator.row
  def clone(ir: Array[Any]): Array[Any] = map(ir, _.clone)

  // deep copies an IR ai.chronon.aggregator.row
  private def map(ir: Array[Any], f: ColumnAggregator => Any => Any): Array[Any] = {
    val result = new Array[Any](length)
    var i = 0
    while (i < columnAggregators.length) {
      result.update(i, f(columnAggregators(i))(ir(i)))
      i += 1
    }
    result
  }

  override def prepare(input: Row): Array[Any] = {
    val ir = new Array[Any](columnAggregators.length)
    update(ir, input)
  }

  override def outputType: DataType = outputType

  override def irType: DataType = irType
}
