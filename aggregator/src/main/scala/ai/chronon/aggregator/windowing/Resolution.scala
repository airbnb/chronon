/*
 *    Copyright (C) 2023 The Chronon Authors.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package ai.chronon.aggregator.windowing

import ai.chronon.api.Extensions.{WindowOps, WindowUtils}
import ai.chronon.api.{GroupBy, TimeUnit, Window}

import scala.util.ScalaJavaConversions.ListOps
import scala.util.ScalaVersionSpecificCollectionsConverter.convertJavaListToScala

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

object ResolutionUtils {

  /**
    * Find the smallest tail window resolution in a GroupBy. Returns None if the GroupBy does not define any windows.
    * The window resolutions are: 5 min for a GroupBy a window < 12 hrs, 1 hr for < 12 days, 1 day for > 12 days.
    * */
  def getSmallestWindowResolutionInMillis(groupBy: GroupBy): Option[Long] =
    Option(
      groupBy.aggregations.toScala.toArray
        .flatMap(aggregation =>
          if (aggregation.windows != null) aggregation.windows.toScala
          else None)
        .map(FiveMinuteResolution.calculateTailHop)
    ).filter(_.nonEmpty).map(_.min)
}
