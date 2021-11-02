package ai.zipline.aggregator.row

import ai.zipline.api.Extensions._
import ai.zipline.api.{AggregationPart, DataType, StringType}

// The primary API of the aggregator package.
// the semantics are to mutate values in place for performance reasons
class RowAggregator(inputSchema: Seq[(String, DataType)], aggregationParts: Seq[AggregationPart]) extends Serializable {

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

        ColumnAggregator.construct(
          inputType,
          spec,
          ColumnIndices(inputIndex, aggregatorIndex),
          bucketIndex
        )
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

  def update(ir: Array[Any], inputRow: Row): Unit = {
    var i = 0
    while (i < columnAggregators.length) {
      columnAggregators(i).update(ir, inputRow)
      i += 1
    }
  }

  def updateWindowed(ir: Array[Any], inputRow: Row, endTime: Long): Unit = {
    var i = 0
    while (i < columnAggregators.length) {
      val ts = inputRow.ts
      val windowStart = endTime - aggregationParts(i).window.millis - 86400 * 1000
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

  def delete(ir: Array[Any], inputRow: Row): Unit =
    if (!isNotDeletable)
      columnAggregators.foreach(_.delete(ir, inputRow))

  // convert arbitrary java types into types that
  // parquet/spark or any other external storage format understands
  // CPCSketch is the primary use-case now. We will need it for quantile estimation too I think
  def normalize(ir: Array[Any]): Array[Any] = map(ir, _.normalize)
  def denormalize(ir: Array[Any]): Array[Any] = map(ir, _.denormalize)

  def denormalizeInPlace(ir: Array[Any]): Array[Any] = {
    var idx = 0
    while (idx < columnAggregators.length) {
      ir.update(idx, columnAggregators(idx).denormalize(ir(idx)))
      idx += 1
    }
    ir
  }

  // deep copies an IR ai.zipline.aggregator.row
  def clone(ir: Array[Any]): Array[Any] = map(ir, _.clone)

  // deep copies an IR ai.zipline.aggregator.row
  private def map(ir: Array[Any], f: ColumnAggregator => Any => Any): Array[Any] = {
    val result = new Array[Any](length)
    var i = 0
    while (i < columnAggregators.length) {
      result.update(i, f(columnAggregators(i))(ir(i)))
      i += 1
    }
    result
  }
}
