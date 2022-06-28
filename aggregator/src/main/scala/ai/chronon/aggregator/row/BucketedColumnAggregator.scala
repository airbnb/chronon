package ai.chronon.aggregator.row

import ai.chronon.aggregator.base.BaseAggregator
import ai.chronon.api.{DataType, MapType, Row, StringType}

import java.util
import scala.collection.mutable

class BucketedColumnAggregator[Input, IR, Output](agg: BaseAggregator[Input, IR, Output],
                                                  columnIndices: ColumnIndices,
                                                  bucketIndex: Int,
                                                  rowUpdater: Dispatcher[Input, Any])
    extends ColumnAggregator {

  override def outputType: DataType = MapType(StringType, agg.outputType)

  override def irType: DataType = MapType(StringType, agg.irType)

  type IrMap = util.HashMap[String, IR]
  private def castIr(ir: Any): IrMap = ir.asInstanceOf[IrMap]

  override def merge(ir1: Any, ir2: Any): Any = {
    if (ir2 == null) return ir1
    // we need to clone here because the contract is to only mutate ir1
    // ir2 can it self be expected to mutate later - and hence has to retain its value
    val rightMap = castIr(ir2)
    val rightIter = rightMap.entrySet().iterator()

    def clone(ir: IR): IR = if (ir == null) ir else agg.clone(ir)
    if (ir1 == null) {
      val rightClone = new IrMap
      while (rightIter.hasNext) {
        val entry = rightIter.next()
        rightClone.put(entry.getKey, clone(entry.getValue))
      }
      return rightClone
    }

    val leftMap = castIr(ir1)
    while (rightIter.hasNext) {
      val entry = rightIter.next()
      val bucket = entry.getKey
      val rightIr = entry.getValue
      if (rightIr != null) {
        val leftIr = leftMap.get(bucket)
        if (leftIr == null) {
          leftMap.put(bucket, clone(rightIr))
        } else {
          leftMap.put(bucket, agg.merge(leftIr, rightIr))
        }
      }
    }
    leftMap
  }

  override def update(ir: Array[Any], inputRow: Row): Unit = {
    val inputVal = inputRow.get(columnIndices.input)
    if (inputVal == null) return // null inputs are ignored

    val bucketVal = inputRow.get(bucketIndex)
    if (bucketVal == null) return // null buckets are ignored

    val bucket = bucketVal.asInstanceOf[String]
    var previousMap = ir(columnIndices.output)

    if (previousMap == null) { // map is absent
      val prepared = rowUpdater.prepare(inputRow)
      if (prepared != null) {
        val newMap = new IrMap()
        newMap.put(bucket, prepared.asInstanceOf[IR])
        ir.update(columnIndices.output, newMap)
      }
      return
    }

    val map = castIr(previousMap)
    if (!map.containsKey(bucket)) { // bucket is absent - so init
      val ir = rowUpdater.prepare(inputRow)
      if (ir != null) {
        map.put(bucket, ir.asInstanceOf[IR])
      }
      return
    }

    val updated = rowUpdater.updateColumn(map.get(bucket), inputRow)
    if (updated != null)
      map.put(bucket, updated.asInstanceOf[IR])
  }

  override def delete(ir: Array[Any], inputRow: Row): Unit = {
    if (!agg.isDeletable) return

    val inputVal = inputRow.get(columnIndices.input)
    if (inputVal == null) return // null inputs are ignored

    val bucketVal = inputRow.get(bucketIndex)
    if (bucketVal == null) return // null buckets are ignored

    val bucket = bucketVal.asInstanceOf[String]
    val previousMap = ir(columnIndices.output)
    lazy val prepared = rowUpdater.inversePrepare(inputRow)
    if (previousMap == null) { // map is absent
      if (prepared != null) {
        val map = mutable.HashMap[String, IR](bucket -> prepared.asInstanceOf[IR])
        ir.update(columnIndices.output, map)
      }
      return
    }

    val map = castIr(previousMap)
    if (!map.containsKey(bucket)) { // bucket is absent - so init
      if (prepared != null)
        map.put(bucket, prepared.asInstanceOf[IR])
      return
    }

    val updated = rowUpdater.deleteColumn(map.get(bucket), inputRow)
    if (updated != null)
      map.put(bucket, updated.asInstanceOf[IR])
  }

  override def finalize(ir: Any): Any = guardedApply(agg.finalize, ir)

  override def normalize(ir: Any): Any = guardedApply(agg.normalize, ir)

  override def denormalize(ir: Any): Any = guardedApply(agg.denormalize, ir)

  override def clone(ir: Any): Any = guardedApply(agg.clone, ir)

  private def guardedApply[ValueType, NewValueType](f: ValueType => NewValueType, ir: Any): Any = {
    if (ir == null) return null
    val iter = ir.asInstanceOf[util.HashMap[String, ValueType]].entrySet().iterator()
    val result = new util.HashMap[String, NewValueType]()
    while (iter.hasNext) {
      val entry = iter.next()
      result.put(entry.getKey, f(entry.getValue))
    }
    result
  }

  override def isDeletable: Boolean = agg.isDeletable
}
