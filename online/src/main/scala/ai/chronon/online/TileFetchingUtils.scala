package ai.chronon.online

import ai.chronon.aggregator.windowing.FiveMinuteResolution.getWindowResolutionMillis
import ai.chronon.flink.window.TileSize

/**
 * When using the Tiled Architecture, Chronon stores streaming data in the Key-Value
 * store as tiles that contain the intermediate results for a given time range.
 *
 * This utility contains the code that calculates what tiles are need to serve a certain
 * GroupBy, at a specific time.
 *
 * Related: "The Tiled Architecture" https://chronon.ai/Tiled_Architecture.html
 */
object TileFetchingUtils {

  /**
   * Given a GroupBy Window and the current time in millis, return the start of the saw-tooth window in millis.
   *
   * Example:
   *  current time: 14:33:24, window size: 01:00:00, resolution: 00:05:00 => window start rounded down: 13:30:00.
   */
  private[kvstore] def getSawToothWindowStartMillis(
                                                     windowSizeMillis: Long,
                                                     currentTimeMillis: Long,
                                                     resolutionMillis: Long
                                                   ) = {
    // It does not make sense for any of these parameters to be zero.
    require(
      (windowSizeMillis > 0 && currentTimeMillis > 0 && resolutionMillis > 0),
      "Window size, current time, and resolution must be non-zero. Got " +
        s"windowSizeMillis: $windowSizeMillis, " +
        s"currentTimeMillis: $currentTimeMillis, " +
        s"resolutionMillis: $resolutionMillis)"
    )

    (currentTimeMillis - windowSizeMillis) - ((currentTimeMillis - windowSizeMillis) % resolutionMillis)
  }

  /**
   * Given a list of windows in a GroupBy, the current time in millis, and the tile sizes available, return the tiles
   * needed to be fetched from the KV store to "fill" or compute the windows as a set of (tile start: millis,
   * tile size: millis) tuples.
   *
   * The algorithm will only select tiles after `startTsMillis`, even if the windows provided start before that time.
   */
  def getTilesForWindows(
                          windowSizesMillis: Seq[Long],
                          startTsMillis: Long,
                          currentTimeMillis: Long,
                          tileSizesAvailable: IndexedSeq[TileSize]
                        ): Set[(Long, Long)] = {
    if (startTsMillis > currentTimeMillis)
      throw new IllegalStateException(
        s"Invalid range: startTsMillis ($startTsMillis) must be less than currentTimeMillis ($currentTimeMillis)."
      )

    // The algorithm works by fetching the largest tiles first, so we sort the tile sizes in descending order.
    val tileSizesAvailableSorted = tileSizesAvailable.sortBy(-_.millis)

    // For each window, get the set of tiles necessary. We use a set to prevent requesting duplicate tiles.
    windowSizesMillis.flatMap { windowSizeMillis =>
      getTilesForWindow(
        windowSizeMillis,
        currentTimeMillis,
        startTsMillis,
        tileSizesAvailableSorted
      )
    }.toSet
  }

  /**
   * Given a GroupBy Window, the current time in millis, and the tile sizes available, return the list of tiles needed
   * to "fill" or compute the Window. The tiles are returned as a set of (tile start: millis, tile size: millis) tuples.
   *
   * Example: We want to fetch tiles for a GroupBy Window of 6 hours. The tile sizes available are 6 hours, 1 hour,
   * 20 minutes, and 5 minutes. The current time is 08:38:00
   *
   * Execution: getTilesForWindow(6hr, [6hr, 1hr, 20m, 5m], 08:38:00) is invoked and calls getTilesForRangeInWindow
   * - First invocation of getTilesForRangeInWindow:
   *      getTilesForRangeInWindow([2:35, 8:38], 6hr)
   *      range to fill: [2:35, 8:38]
   *      algorithm allocates one tile: [6:00, 12:00).
   * - Second, recursive invocation:
   *      getTilesForRangeInWindow([2:35, 6:00], 1hr)
   *      range to fill: [2:35, 6:00]
   *      algorithm allocates three tiles: [3:00, 4:00), [4:00, 5:00), [5:00, 6:00).
   * - Third, recursive invocation:
   *      getTilesForRangeInWindow([2:35, 3:00], 20m)
   *      range to fill: [2:35, 3:00]
   *      algorithm allocates one tile: [2:40, 3:00)
   * - Fourth, recursive invocation:
   *      getTilesForRangeInWindow([2:35, 2:40], 5m)
   *      range to fill: [2:35, 2:40]
   *      algorithm allocates one tile: [2:35, 2:40)
   *
   *  Result: the following tiles are needed:
   *      [6:00, 12:00), [3:00, 4:00), [4:00, 5:00), [5:00, 6:00), [2:40, 3:00), [2:35, 2:40)
   */
  private def getTilesForWindow(
                                 windowSizeMillis: Long,
                                 currentTimeMillis: Long,
                                 startTsMillis: Long,
                                 tileSizesAvailableForThisChrononWindowSortedDescending: IndexedSeq[TileSize]
                               ): Set[(Long, Long)] = {
    val windowResolution = getWindowResolutionMillis(windowSizeMillis)

    // We only need to fetch tiles that are after the startTsMillis. (It's assumed that the tiles before this time
    // are already available in the offline/batch data.)
    val sawToothWindowStartMillis = Math.max(
      startTsMillis,
      getSawToothWindowStartMillis(windowSizeMillis, currentTimeMillis, windowResolution)
    )

    // The tile fetching algorithm must start with the largest tile size
    val largestTileSizeMillis = tileSizesAvailableForThisChrononWindowSortedDescending.head

    // Get the tiles that need to be fetched.
    getTilesForRangeInWindow(
      sawToothWindowStartMillis,
      currentTimeMillis + 1, // The end of the range is exclusive, so we add 1 millisecond to the current time to include it
      largestTileSizeMillis,
      tileSizesAvailableForThisChrononWindowSortedDescending,
      canExceedRangeEnd = true
    ).map(tile => (tile._1, tile._2.millis))
  }

  /**
   * Given a range return the Epoch-aligned tiles that need to be fetched.
   *
   * @param canExceedRangeEnd If true, the algorithm can fetch tiles that exceed the range end. For example, if the
   *                          range is [14:35, 19:30) and tile size is 06:00, it's ok for us to fetch the
   *                          partially-complete [18:00, 00:00) tile, so canExceedRangeEnd should be True.
   */
  private def getTilesForRangeInWindow(
                                        rangeStartInclusiveMillis: Long,
                                        rangeEndExclusiveMillis: Long,
                                        currentTileSize: TileSize,
                                        tileSizesAvailableForThisChrononWindowSortedDescending: IndexedSeq[TileSize],
                                        canExceedRangeEnd: Boolean = false
                                      ): Set[(Long, TileSize)] = {
    // Find the first possible tile start within the range.
    // Example: range: [14:35, 16:00), tile size: 00:20 => first possible tile start = 14:40.
    val firstPossibleTileStartAlignedWithEpoch = rangeStartInclusiveMillis +
      ((currentTileSize.millis - (rangeStartInclusiveMillis % currentTileSize.millis)) % currentTileSize.millis)

    // If the first possible tile start is after the end of the range, we can't use the current tile size, so we try
    // the next smaller one.
    // Example: range: [14:35, 16:00), tile size: 06:00 => first possible tile start = 18:00.
    if (firstPossibleTileStartAlignedWithEpoch >= rangeEndExclusiveMillis) {
      // If tile size is the smallest available, we can't fill the window, something is very wrong with the tiling setup.
      if (currentTileSize == tileSizesAvailableForThisChrononWindowSortedDescending.last) {
        throw new IllegalStateException(
          s"Could not fill a Chronon Window defined in a GroupBy with the tile sizes currently available from " +
            s"Flink. Smallest tile size: $currentTileSize Range: [$rangeStartInclusiveMillis, $rangeEndExclusiveMillis)."
        )
      }

      getTilesForRangeInWindow(
        rangeStartInclusiveMillis,
        rangeEndExclusiveMillis,
        tileSizesAvailableForThisChrononWindowSortedDescending(
          tileSizesAvailableForThisChrononWindowSortedDescending.indexOf(currentTileSize) + 1
        ),
        tileSizesAvailableForThisChrononWindowSortedDescending,
        canExceedRangeEnd // If firstPossibleTileStartAlignedWithEpoch is AFTER the end of the range, that means that,
        // at this point in the algorithm's execution, we haven't fetched a single tile. (This is true because the
        // algorithm works by fetching the largest tiles first.) So, pass canExceedRangeEnd forward.
      )
    } else {
      // Find the last possible tile end within the range.
      val lastPossibleTileEndAlignedWithEpoch =
        if (!canExceedRangeEnd) {
          // This is the default case.
          // Example: range: [14:35, 16:00), tile size: 00:20 => last possible tile end = 16:00.
          rangeEndExclusiveMillis - (rangeEndExclusiveMillis % currentTileSize.millis)
        } else {
          // There's one exception. If canExceedRangeEnd is true, that means that we're at a point in the algorithm where
          // we can fetch data past the end of the range an get the current, partially-complete tile.
          // Example: range: [14:35, 19:30), tile size: 06:00, it's ok for us to fetch the (partially-complete)
          // [18:00, 00:00) tile. So we move the end of the range to 00:00.
          rangeEndExclusiveMillis - (rangeEndExclusiveMillis % currentTileSize.millis) + currentTileSize.millis
        }

      // Calculate the number of tiles that can fit within the range.
      // Example: range: [14:35, 16:00), tile size: 00:20, first tile start: 14:40, last tile end: 16:00
      //  => number of tiles = 4.
      val numTilesThatCanFitInTheRange =
        ((lastPossibleTileEndAlignedWithEpoch - firstPossibleTileStartAlignedWithEpoch) / currentTileSize.millis).toInt

      // Generate the tiles that need to be fetched.
      // Example: range: [14:35, 16:00), tile size: 00:20, first tile start: 14:40, last tile end: 16:00
      //  => tiles = [(14:40, TileSize.TwentyMinutes), (...), (...), (15:40, TileSize.TwentyMinutes)].
      val tilesBuilder: collection.mutable.Builder[(Long, TileSize), Set[(Long, TileSize)]] =
        Set.newBuilder[(Long, TileSize)]
      for (i <- 0 until numTilesThatCanFitInTheRange) {
        val tileStart = firstPossibleTileStartAlignedWithEpoch + i * currentTileSize.millis
        tilesBuilder += ((tileStart, currentTileSize))
      }
      var tiles: Set[(Long, TileSize)] = tilesBuilder.result()

      // Figure out what part of the range is not filled by the tiles on the LEFT.
      // Example: range: [14:35, 16:00), tile size: 00:20, first tile start: 14:40, last tile end: 16:00
      // => unfilled range = [14:35, 14:40).
      val unfilledRangeStartLeft = rangeStartInclusiveMillis
      val unfilledRangeEndLeft = firstPossibleTileStartAlignedWithEpoch
      val isThereUnfilledRangeOnTheLeft = unfilledRangeStartLeft < unfilledRangeEndLeft

      // Figure out what part of the range is not filled by the tiles on the RIGHT.
      // Example: range: [0:30, 1:20), tile size: 00:15, first tile start: 0:30, last tile end: 1:15.
      // => unfilled range = [1:15, 1:20).
      val unfilledRangeStartRight = lastPossibleTileEndAlignedWithEpoch
      val unfilledRangeEndRight = rangeEndExclusiveMillis
      val isThereUnfilledRangeOnTheRight = unfilledRangeStartRight < unfilledRangeEndRight

      // if tile size is the smallest available, and we haven't filled the entire range, something is very wrong.
      if (currentTileSize == tileSizesAvailableForThisChrononWindowSortedDescending.last
        && (isThereUnfilledRangeOnTheLeft || isThereUnfilledRangeOnTheRight)) {
        throw new IllegalStateException(
          s"Could not fill a Chronon Window defined in a GroupBy with the tile sizes currently available from " +
            s"Flink. Smallest tile size: $currentTileSize Range: [$rangeStartInclusiveMillis, $rangeEndExclusiveMillis)."
        )
      }

      // If there is an unfilled range on the LEFT, we need to use smaller tiles to fill it.
      if (isThereUnfilledRangeOnTheLeft) {
        tiles = tiles ++ getTilesForRangeInWindow(
          unfilledRangeStartLeft,
          unfilledRangeEndLeft,
          tileSizesAvailableForThisChrononWindowSortedDescending(
            tileSizesAvailableForThisChrononWindowSortedDescending.indexOf(currentTileSize) + 1
          ),
          tileSizesAvailableForThisChrononWindowSortedDescending
        )
      }

      // If there is an unfilled range on the RIGHT, we need to use smaller tiles to fill it.
      if (isThereUnfilledRangeOnTheRight) {
        tiles = tiles ++ getTilesForRangeInWindow(
          unfilledRangeStartRight,
          unfilledRangeEndRight,
          tileSizesAvailableForThisChrononWindowSortedDescending(
            tileSizesAvailableForThisChrononWindowSortedDescending.indexOf(currentTileSize) + 1
          ),
          tileSizesAvailableForThisChrononWindowSortedDescending
        )
      }

      tiles
    }
  }
}
