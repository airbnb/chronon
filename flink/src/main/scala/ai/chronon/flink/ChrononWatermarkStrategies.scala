package ai.chronon.flink

import ai.chronon.api.Constants
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}

import java.time.Duration

object ChrononWatermarkStrategies {
  def sparkExpressionEvalWatermarkStrategy(
      allowedOutOfOrderness: Duration,
      idlenessTimeout: Duration): WatermarkStrategy[Map[String, Any]] =
    WatermarkStrategy
      .forBoundedOutOfOrderness[Map[String, Any]](allowedOutOfOrderness)
      .withIdleness(idlenessTimeout)
      .withTimestampAssigner(new SerializableTimestampAssigner[Map[String, Any]] {
        override def extractTimestamp(element: Map[String, Any], recordTimestamp: Long): Long =
          chrononTimeColumnMillis(element)
      })

  private[flink] def chrononTimeColumnMillis(element: Map[String, Any]): Long =
    element(Constants.TimeColumn).asInstanceOf[Number].longValue()
}
