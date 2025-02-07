package ai.chronon.flink.window

import ai.chronon.aggregator.windowing.FiveMinuteResolution.getWindowResolutionMillis
import ai.chronon.api.Extensions.{WindowOps, WindowUtils}

import scala.collection.mutable.{Map => MutableMap}

/**
 * This object contains utility methods for determining what Flink window sizes Chronon will use for a given GroupBy.
 */
object WindowSizeUtils {

  /**
   * Given a list of GroupBy Windows sizes in millis, calculate the smallest window
   * resolution as defined by Chronon. Returns None if the list of windows is empty.
   *
   * For example, if the windows are [30d, 7d, 1d, 1h], the resolution are
   * [1d, 1h, 1h, 5m], and the smallest resolution is 5m.
   */
  def getSmallestWindowResolutionInMillis(windowSizesMillis: Seq[Long]): Option[Long] =
    Option(windowSizesMillis.map(windowSizeMillis => getWindowResolutionMillis(windowSizeMillis)))
      .map(_.min)
  /**
   * Given a list of GroupBy Windows, determine what window sizes should be used in Flink.
   * Each window size is accompanied by a unique ID (String) that is used to build the UID
   * of the Flink operators. All Flink window sizes are also TileSizes.
   *
   * This public method exists so different parts of Chronon can use the same logic to determine
   * what tile sizes are available. The Chronon KV store can use this method to determine what tiles
   * sizes it can fetch.
   */
  def getFlinkWindowSizesAndWindowIds(
                                       windowSizesMillis: Seq[Long],
                                       shouldEnableAdditionalTwentyMinAndSixHourWindows: () => Boolean
                                     ): Map[TileSize, String] = {
    // Create a map of window sizes to ID. The IDs are used to build the UID of the Flink operators.
    val flinkWindowSizes: MutableMap[TileSize, String] = MutableMap()

    // All GroupBys have a Flink window equal to its smallest window resolution, e.g. a GB with a
    // 10-hour window will have a 5-minute Flink window/tile.
    val smallestWindowResolutionInMillis: Long =
      getSmallestWindowResolutionInMillis(windowSizesMillis) match {
        case Some(smallestTileSize) => smallestTileSize
        // Some GroupBys don't have windows defined, so we default to 1 day.
        case None =>
          WindowUtils.Day.millis
      }
    val smallestTileSize: TileSize = TileSize.fromMillis(smallestWindowResolutionInMillis)
    flinkWindowSizes += (smallestTileSize -> "02") // These ID strings are arbitrarily chosen.

    // Optimization (Tile Layering): all GroupBys with 5-minute tiles should also have 1-hour tiles.
    val use1HourWindow = smallestWindowResolutionInMillis == WindowUtils.FiveMinutes
    if (use1HourWindow) {
      flinkWindowSizes += (TileSize.OneHour -> "12")
    }

    flinkWindowSizes.toMap
  }
}
