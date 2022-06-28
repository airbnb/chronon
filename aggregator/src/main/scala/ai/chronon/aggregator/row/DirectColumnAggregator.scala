package ai.chronon.aggregator.row

import ai.chronon.aggregator.base.BaseAggregator
import ai.chronon.api.{DataType, Row}

class DirectColumnAggregator[Input, IR, Output](agg: BaseAggregator[Input, IR, Output],
                                                columnIndices: ColumnIndices,
                                                dispatcher: Dispatcher[Input, Any])
    extends ColumnAggregator {
  override def outputType: DataType = agg.outputType
  override def irType: DataType = agg.irType

  override def merge(ir1: Any, ir2: Any): Any = {
    if (ir2 == null) return ir1
    // we need to clone here because the contract is to only mutate ir1
    // ir2 can it self be expected to mutate later - and hence has to retain it's value
    // this is a critical assumption of the rest of the code
    if (ir1 == null) return agg.clone(ir2.asInstanceOf[IR])
    agg.merge(ir1.asInstanceOf[IR], ir2.asInstanceOf[IR])
  }

  // non bucketed update
  override def update(ir: Array[Any], inputRow: Row): Unit = {
    val inputVal = inputRow.get(columnIndices.input)
    if (inputVal == null) return // null inputs are ignored
    val previousVal = ir(columnIndices.output)
    if (previousVal == null) { // value is absent - so init
      ir.update(columnIndices.output, dispatcher.prepare(inputRow))
      return
    }
    val previous = previousVal.asInstanceOf[IR]
    val updated = dispatcher.updateColumn(previous, inputRow)
    ir.update(columnIndices.output, updated)
  }

  override def delete(ir: Array[Any], inputRow: Row): Unit = {
    if (!agg.isDeletable) return
    val inputVal = inputRow.get(columnIndices.input)
    if (inputVal == null) return
    val previousVal = ir(columnIndices.output)
    if (previousVal == null) {
      ir.update(columnIndices.output, dispatcher.inversePrepare(inputRow))
      return
    }
    val previous = previousVal.asInstanceOf[IR]
    val deleted = dispatcher.deleteColumn(previous, inputRow)
    ir.update(columnIndices.output, deleted)
  }

  override def finalize(ir: Any): Any = guardedApply(agg.finalize, ir)
  override def normalize(ir: Any): Any = guardedApply(agg.normalize, ir)
  override def denormalize(ir: Any): Any = if (ir == null) null else agg.denormalize(ir)
  override def clone(ir: Any): Any = guardedApply(agg.clone, ir)
  private def guardedApply[ValueType, NewValueType](f: ValueType => NewValueType, ir: Any): Any = {
    if (ir == null) null else f(ir.asInstanceOf[ValueType])
  }

  override def isDeletable: Boolean = agg.isDeletable
}
