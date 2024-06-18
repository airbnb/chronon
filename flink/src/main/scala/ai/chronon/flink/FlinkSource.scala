package ai.chronon.flink

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

abstract class FlinkSource[T] extends Serializable {

  /**
    * Return a Flink DataStream for the given topic and feature group.
    *
    * When implementing a source, you should also make a conscious decision about your allowed lateness strategy.
    */
  def getDataStream(topic: String, groupName: String)(
      env: StreamExecutionEnvironment,
      parallelism: Int
  ): DataStream[T]
}
