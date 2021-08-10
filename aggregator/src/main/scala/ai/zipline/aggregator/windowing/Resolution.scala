package ai.zipline.aggregator.windowing

import ai.zipline.api.Extensions._
import ai.zipline.api.{TimeUnit, Window}

trait Resolution extends Serializable {
  // For a given window what is the resolution of the tail
  // The tail hops with the window size as represented by the return value
  def calculateTailHop(window: Window): Long

  // What are the hops that we will use to tile the full window
  // 1. Need to be sorted in descending order, and
  // 2. Every element needs to be a multiple of the next one
  // 3. calculateTailHop needs to return values only from this.
  val hopSizes: Array[Long]
}

object FiveMinuteResolution extends Resolution {
  def calculateTailHop(window: Window): Long =
    window.millis match {
      case x if x >= new Window(12, TimeUnit.DAYS).millis  => WindowUtils.Day.millis
      case x if x >= new Window(12, TimeUnit.HOURS).millis => WindowUtils.Hour.millis
      case _                                               => WindowUtils.FiveMinutes
    }

  val hopSizes: Array[Long] =
    Array(WindowUtils.Day.millis, WindowUtils.Hour.millis, WindowUtils.FiveMinutes)
}

object DailyResolution extends Resolution {

  def calculateTailHop(window: Window): Long =
    window.timeUnit match {
      case TimeUnit.DAYS => WindowUtils.Day.millis
      case _ =>
        throw new IllegalArgumentException(
          s"Invalid request for window $window for daily aggregation. " +
            s"Window can only be multiples of 1d or the operation needs to be un-windowed."
        )
    }

  val hopSizes: Array[Long] = Array(WindowUtils.Day.millis)
}
