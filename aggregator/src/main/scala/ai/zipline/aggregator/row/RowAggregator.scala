package ai.zipline.aggregator.row

import ai.zipline.aggregator.base.DataType
import ai.zipline.api.AggregationPart
import ai.zipline.api.Extensions._

// The primary API of the aggregator package.
// the semantics are to mutate values in place for performance reasons
class RowAggregator(inputSchema: Seq[(String, DataType)],
                    aggregationParts: Seq[AggregationPart],
                    groupByName: String = null) extends Serializable {

  val length: Int = aggregationParts.size
  val indices: Range = 0 until length
  // has to be array for fast random access
  val columnAggregators: Array[ColumnAggregator] = {
    aggregationParts.zipWithIndex.map {
      case (spec: AggregationPart, aggregatorIndex: Int) =>
        val ((_, inputType), inputIndex) = {
          inputSchema.zipWithIndex.find(_._1._1 == spec.inputColumn).get
        }
        ColumnAggregator.construct(
          inputType,
          spec,
          ColumnIndices(inputIndex, aggregatorIndex)
        )
    }
  }.toArray

  def apply(index: Int): ColumnAggregator = columnAggregators(index)

  def init: Array[Any] = new Array[Any](aggregationParts.length)

  val irSchema: Array[(String, DataType)] = aggregationParts
    .map(_.outputColumnName(groupByName))
    .toArray
    .zip(columnAggregators.map(_.irType))

  val outputSchema: Array[(String, DataType)] = aggregationParts
    .map(_.outputColumnName(groupByName))
    .toArray
    .zip(columnAggregators.map(_.outputType))

  val isNotDeletable: Boolean = columnAggregators.forall(!_.isDeletable)

  def update(ir: Array[Any], inputRow: Row): Unit =
    columnAggregators.foreach(_.update(ir, inputRow))

  def updateWindowed(ir: Array[Any], inputRow: Row, endTime: Long): Unit =
    for (i <- columnAggregators.indices) {
      val ts = inputRow.ts
      val windowStart = endTime - aggregationParts(i).window.millis
      if (ts < endTime && ts >= windowStart)
        columnAggregators(i).update(ir, inputRow)
    }

  def merge(ir1: Array[Any], ir2: Array[Any]): Unit =
    for (i <- columnAggregators.indices)
      ir1.update(i, columnAggregators(i).merge(ir1(i), ir2(i)))

  def finalize(ir: Array[Any]): Array[Any] = map(ir, _.finalize)

  def delete(ir: Array[Any], inputRow: Row): Unit =
    if (!isNotDeletable)
      columnAggregators.foreach(_.delete(ir, inputRow))

  // convert arbitrary java types into types that
  // parquet/spark or any other external storage format understands
  // CPCSketch is the primary use-case now. We will need it for quantile estimation too I think
  def normalize(ir: Array[Any]): Array[Any] = map(ir, _.normalize)
  def denormalize(ir: Array[Any]): Array[Any] = map(ir, _.denormalize)

  // deep copies an IR ai.zipline.aggregator.row
  def clone(ir: Array[Any]): Array[Any] = map(ir, _.clone)

  // deep copies an IR ai.zipline.aggregator.row
  private def map(ir: Array[Any], f: ColumnAggregator => Any => Any): Array[Any] = {
    val result = new Array[Any](length)
    for (i <- columnAggregators.indices) {
      result.update(i, f(columnAggregators(i))(ir(i)))
    }
    result
  }
}
