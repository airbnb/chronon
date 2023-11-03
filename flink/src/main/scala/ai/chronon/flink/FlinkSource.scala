package ai.chronon.flink

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

abstract class FlinkSource[T] extends Serializable {

  /**
   * Return an instrumented DataStream for the given topic and feature group.
   */
  def getDataStream(topic: String, groupName: String)(
    env: StreamExecutionEnvironment,
    parallelism: Int
  ): DataStream[T]
}
