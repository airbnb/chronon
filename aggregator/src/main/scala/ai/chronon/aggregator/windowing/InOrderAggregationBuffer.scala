package ai.chronon.aggregator.windowing

import ai.chronon.aggregator.base.SimpleAggregator

case class InOrderAggregationEntry[IR](var value: IR, ts: Long)

trait InOrderAggregationBuffer[Input, IR >: Null, Output >: Null] {

  def peekBack(): InOrderAggregationEntry[IR]
  def pop(): InOrderAggregationEntry[IR]
  def push(input: Input, ts: Long): Unit
  def query(): IR
  def reset(): Unit
}

object InOrderSlidingAggregationBuffer {
  def createTwoStackLiteBuffer[Input, IR >: Null, Output >: Null](aggregator: SimpleAggregator[Input, IR, Output]) =
    new TwoStackLiteBuffer[Input, IR, Output](aggregator)

  def createDABALiteBuffer[Input, IR >: Null, Output >: Null](
      aggregator: SimpleAggregator[Input, IR, Output]
  ) = new DABALiteBuffer[Input, IR, Output](aggregator)
}
