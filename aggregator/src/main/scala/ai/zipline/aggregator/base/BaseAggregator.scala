package ai.zipline.aggregator.base

import ai.zipline.api.DataType

abstract class BaseAggregator[Input, IR, Output] extends Serializable {
  def outputType: DataType
  def irType: DataType

  def merge(ir1: IR, ir2: IR): IR
  def finalize(ir: IR): Output

  // we allow mutating the IR
  // in cases where we need to work on the same IR twice (eg., Sawtooth::cumulate)
  // we will need to preserve a copy before mutating the IR
  def clone(ir: IR): IR

  // we work on RDDs, and use custom java types for IRs - like CPCSketch etc,
  // But when it comes time to serialize it, we will need to convert IR into types
  // that the (de)serializer can understand.
  def normalize(ir: IR): Any = ir
  def denormalize(ir: Any): IR = ir.asInstanceOf[IR]

  def isDeletable: Boolean = false
}

// sum, count, min, max, avg, approx_unique, topK, bottomK
abstract class SimpleAggregator[Input, IR, Output] extends BaseAggregator[Input, IR, Output] {

  def prepare(input: Input): IR
  def update(ir: IR, input: Input): IR

  def inversePrepare(input: Input): IR = {
    // we don't have `empty()` method or a `inverse(input)` method,
    // we only have `prepare(input)` and `delete(ir, input)`
    // so we call `prepare(input)` followed by two `deletes(ir, input)`
    val init = prepare(input)
    val zero = delete(init, input)
    delete(zero, input)
  }

  def delete(ir: IR, input: Input): IR =
    throw new UnsupportedOperationException("Operation is not deletable")
}

// time aggregators are by default not deletable - or we haven't seen one
// Eg., first, last, lastK, firstK
abstract class TimedAggregator[Input, IR, Output] extends BaseAggregator[Input, IR, Output] {
  def prepare(input: Input, ts: Long): IR
  def update(ir: IR, input: Input, ts: Long): IR
}
