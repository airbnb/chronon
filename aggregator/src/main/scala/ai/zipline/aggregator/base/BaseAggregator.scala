package ai.zipline.aggregator.base

abstract class BaseAggregator[Input, IR, Output] extends Serializable {
  def outputType: DataType

  def irType: DataType

  def merge(ir1: IR, ir2: IR): IR

  // we allow mutating the IR
  // in cases where we need to work on the same IR twice (eg., Sawtooth::cumulate)
  // we will need to preserve a copy before mutating the IR
  def clone(ir: IR): IR = ir

  def finalize(ir: IR): Output

  // we work on RDDs, and use custom java types for IRs - like CPCSketch etc,
  // But when it comes time to serialize it, we will need to convert IR into types
  // that the (de)serializer can understand.
  def normalize(ir: IR): Any = ir
  def denormalize(ir: Any): IR = ir.asInstanceOf[IR]
}

// sum, count, min, max, avg, approx_unique, topK, bottomK
abstract class SimpleAggregator[Input, IR, Output] extends BaseAggregator[Input, IR, Output] {

  def prepare(input: Input): IR

  def update(ir: IR, input: Input): IR

  def delete(ir: IR, input: Input): IR =
    throw new UnsupportedOperationException("Operation is not deletable")

  def isDeletable: Boolean = false
}

// time aggregators are by default not deletable - or we haven't seen one
// Eg., first, last, lastK, firstK
abstract class TimedAggregator[Input, IR, Output] extends BaseAggregator[Input, IR, Output] {
  def prepare(input: Input, ts: Long): IR

  def update(ir: IR, input: Input, ts: Long): IR
}
