package ai.zipline.spark.test

import ai.zipline.api.Constants
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{DataType, LongType, StructField, StructType}


/**
 * Simple Aggregator class to generate snapshots from mutations based on the
 * latest mutation Ts.
 */
class SnapshotAggregator(inputSchema: StructType, mutationColumnName: String, keyColumnName: String) extends Serializable {

  val mutationTsIdx = inputSchema.fieldIndex(Constants.MutationTimeColumn)
  val mutationColumnIdx = inputSchema.fieldIndex(mutationColumnName)
  val dsIdx = inputSchema.fieldIndex(Constants.PartitionColumn)
  val tsIdx = inputSchema.fieldIndex(Constants.TimeColumn)
  val keyIdx = inputSchema.fieldIndex(keyColumnName)

  def aggregatorKey(row: Row)  = {
    (row.get(keyIdx), row.getLong(tsIdx), row.getString(dsIdx))
  }

  def init: Array[Any] = new Array[Any](4)

  def update(ir: Array[Any], inputRow: Row): Array[Any] = {
    if (ir == null) Array[Any](inputRow.getLong(mutationTsIdx), inputRow.get(mutationColumnIdx), inputRow.get(tsIdx), inputRow.get(mutationTsIdx))
    val irTs = ir(0).asInstanceOf[Long]
    val rowTs = inputRow.getLong(mutationTsIdx)
    val createdAt = if (irTs > 0) math.min(irTs, rowTs) else rowTs
    val updatedAt = math.max(irTs, rowTs)
    if (irTs < rowTs) {
      Array[Any](rowTs, inputRow.get(mutationColumnIdx), createdAt, updatedAt)
    } else {
      Array[Any](irTs, ir(1), createdAt, updatedAt)
    }
  }

  def merge(ir1: Array[Any], ir2: Array[Any]): Array[Any] = {
    if (ir1 == null) return ir2
    if (ir2 == null) return ir1
    val ir1Ts = ir1(0).asInstanceOf[Long]
    val ir2Ts = ir2(0).asInstanceOf[Long]
    val createdAt = math.min(ir1Ts, ir2Ts)
    val updatedAt = math.max(ir1Ts, ir2Ts)
    if (ir1Ts < ir2Ts) {
      Array[Any](ir2Ts, ir2(1), createdAt, updatedAt)
    } else {
      Array[Any](ir1Ts, ir1(1), createdAt, updatedAt)
    }
  }

  def finalize(ir: Array[Any]): (Any, Any, Any) = {
    (ir(1), ir(2), ir(3))
  }

  def toRow(rddKey: (Any, Any, Any), rddValue: (Any, Any, Any)) = {
    Row(rddKey._1, rddValue._1, rddKey._3, rddValue._2, rddValue._3, rddKey._2)
  }

  val outputSchema = StructType(Array(
    inputSchema.fields(keyIdx),
    inputSchema.fields(mutationColumnIdx),
    inputSchema.fields(dsIdx),
    StructField("created_at", LongType, true),
    StructField("updated_at", LongType, true),
    inputSchema.fields(tsIdx)
  ))
}
