package ai.zipline.aggregator.base

abstract class BaseAggregator[Input, IR, Output] extends Serializable {
  def outputType: DataType

  def irType: DataType

  def merge(ir1: IR, ir2: IR): IR

  // numeric types don't need to be cloned - but arrays etc need to be
  def clone(ir: IR): IR = ir

  def finalize(ir: IR): Output

  def normalize(ir: IR): Any = ir

  def denormalize(ir: Any): IR = ir.asInstanceOf[IR]
}

// sum, count, min, max, avg, approx_unique, topK, bottomK
abstract class SimpleAggregator[Input, IR, Output]
    extends BaseAggregator[Input, IR, Output] {

  def prepare(input: Input): IR

  def update(ir: IR, input: Input): IR

  def delete(ir: IR, input: Input): IR =
    throw new UnsupportedOperationException("Operation is not deletable")

  def isDeletable: Boolean = false
}

// time aggregators are by default not deletable - or we haven't seen one
// Eg., first, last, lastK, firstK
abstract class TimedAggregator[Input, IR, Output]
    extends BaseAggregator[Input, IR, Output] {
  def prepare(input: Input, ts: Long): IR

  def update(ir: IR, input: Input, ts: Long): IR
}
