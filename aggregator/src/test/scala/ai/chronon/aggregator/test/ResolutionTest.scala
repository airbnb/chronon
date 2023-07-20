package ai.chronon.aggregator.test

import ai.chronon.aggregator.windowing.ResolutionUtils
import ai.chronon.api.Extensions.{TimeUnitOps, WindowUtils}
import ai.chronon.api.{Builders, GroupBy, Operation, TimeUnit, Window}
import org.junit.Assert._
import org.junit.Test

class ResolutionTest {
  @Test
  def determinesCorrectSmallestWindowResolution(): Unit = {
    def buildGroupByWithWindows(windows: Seq[Window] = null): GroupBy = Builders.GroupBy(
      aggregations = Seq(
        Builders.Aggregation(
          operation = Operation.SUM,
          inputColumn = "amount_in_usd",
          windows = windows
        )
      )
    )

    assert(
      ResolutionUtils
        .getSmallestWindowResolutionInMillis(
          buildGroupByWithWindows(
            Seq(
              new Window(4, TimeUnit.HOURS), // smallest window should determine tile size
              new Window(4, TimeUnit.DAYS),
              new Window(30, TimeUnit.DAYS)
            )
          )
        )
        .get == WindowUtils.FiveMinutes
    )
    assert(
      ResolutionUtils
        .getSmallestWindowResolutionInMillis(
          buildGroupByWithWindows(
            Seq(
              new Window(4, TimeUnit.DAYS),
              new Window(30, TimeUnit.DAYS)
            )
          )
        )
        .get == TimeUnit.HOURS.millis
    )
    assert(
      ResolutionUtils
        .getSmallestWindowResolutionInMillis(
          buildGroupByWithWindows(
            Seq(
              new Window(30, TimeUnit.DAYS)
            )
          )
        )
        .get == TimeUnit.DAYS.millis
    )
    // No errors if a window is not defined
    assert(
      ResolutionUtils
        .getSmallestWindowResolutionInMillis(
          buildGroupByWithWindows()
        )
        .isEmpty
    )
  }
}
