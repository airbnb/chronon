package ai.chronon.aggregator.windowing
import org.apache.spark.sql.Encoder
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder

import java.sql.Timestamp
import scala.collection.mutable
import scala.collection.mutable.Map
case class ValueWithTime(var timestamp: Timestamp, var value: Double)

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.Aggregator

class TestAggregator extends Aggregator[Row, mutable.Map[String, ValueWithTime], mutable.Map[String, Double]] {
  override def zero: mutable.Map[String, ValueWithTime] = mutable.Map[String, ValueWithTime]()

  override def reduce(buffer: mutable.Map[String, ValueWithTime], newValue: Row): mutable.Map[String, ValueWithTime] = {
    val newValueTime = newValue.getAs[Timestamp](newValue.fieldIndex("timestamp"))
    val newValueReadings = newValue.getAs[Row](newValue.fieldIndex("readings"))

    println(s"*" * 100)
    println(s"** Reduce call before \nnewValue: ${newValue} \nbuffer: ${buffer}")

    val resValue = newValueReadings
      .getValuesMap[Any](List("temperature", "air_quality", "humidity", "light_intensity"))
      .foldLeft(buffer)((runningBuffer, newReading) => {
        if (!runningBuffer.contains(newReading._1) && newReading._2 != null) {
          runningBuffer += (newReading._1 -> ValueWithTime(timestamp = newValueTime, value = newReading._2.asInstanceOf[Number].doubleValue()))
          runningBuffer
        }
        else if (runningBuffer.contains(newReading._1) && runningBuffer(newReading._1).timestamp.before(newValueTime) && newReading._2 != null) {
          runningBuffer(newReading._1).timestamp = newValueTime
          runningBuffer(newReading._1).value = newReading._2.asInstanceOf[Number].doubleValue()
          runningBuffer
        } else {
          runningBuffer
        }
      })

    println(s"** Reduce call after \nresValue: ${resValue}")
    println(s"*" * 100)
    resValue
  }

  override def merge(b1: mutable.Map[String, ValueWithTime], b2: mutable.Map[String, ValueWithTime]): mutable.Map[String, ValueWithTime] = {
    println("*" * 100)
    println(s"merge call:\nb1: ${b1}\nb2: ${b2}")

    b2.foreach(b2Item => {
      if (!b1.contains(b2Item._1)) {
        b1 += (b2Item._1 -> ValueWithTime(b2Item._2.timestamp, b2Item._2.value))
      }
      if (b1.contains(b2Item._1) && b1(b2Item._1).timestamp.before(b2Item._2.timestamp)) {
        b1(b2Item._1).timestamp = b2Item._2.timestamp
        b1(b2Item._1).value = b2Item._2.value
        b1
      } else {
        b1
      }
    })

    println(s"merge after:\nb1: ${b1}")
    println("*" * 100)
    b1
  }

  override def finish(reduction: mutable.Map[String, ValueWithTime]): mutable.Map[String, Double] = {
    return null
  }

  override def bufferEncoder: Encoder[mutable.Map[String, ValueWithTime]] = ExpressionEncoder()

  override def outputEncoder: Encoder[mutable.Map[String, Double]] = ExpressionEncoder()
}
