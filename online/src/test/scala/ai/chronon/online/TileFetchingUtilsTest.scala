package ai.chronon.online

import org.junit.Test
import org.mockito.Mockito.{mock, when}
import org.junit.Assert._
import ai.chronon.aggregator.windowing.FiveMinuteResolution.getWindowResolutionMillis
import ai.chronon.api.Extensions.WindowOps
import ai.chronon.api.{TimeUnit, Window}

class TileFetchingUtilsTest {

  @Test
  def testGetSawToothWindowStartMillisForWindowGreaterThan12Days(): Unit = {
    val window = new Window(12, TimeUnit.DAYS)
    val windowSizeMillis = window.millis
    val resolutionMillis = getWindowResolutionMillis(windowSizeMillis)
    val currentTimeMillis = 1719755742000L // 2024-06-30 13:55:42.000

    val expectedSawToothWindowStartMillis = 1718668800000L // 2024-06-18 00:00:00.000

    assertEquals(
      expectedSawToothWindowStartMillis,
      TileFetchingUtils.getSawToothWindowStartMillis(
        windowSizeMillis,
        currentTimeMillis,
        resolutionMillis
      )
    )
  }

  @Test
  def testGetSawToothWindowStartMillisForWindowBetween12HoursAnd12Days(): Unit = {
    val window = new Window(12, TimeUnit.HOURS)
    val windowSizeMillis = window.millis
    val resolutionMillis = getWindowResolutionMillis(windowSizeMillis)
    val currentTimeMillis = 1719755742000L // 2024-06-30 13:55:42.000

    val expectedSawToothWindowStartMillis = 1719709200000L // 2024-06-30 01:00:00.000

    assertEquals(
      expectedSawToothWindowStartMillis,
      TileFetchingUtils.getSawToothWindowStartMillis(
        windowSizeMillis,
        currentTimeMillis,
        resolutionMillis
      )
    )
  }

  @Test
  def testGetSawToothWindowStartMillisForWindowLessThan12Hours(): Unit = {
    val window = new Window(1, TimeUnit.HOURS)
    val windowSizeMillis = window.millis
    val resolutionMillis = getWindowResolutionMillis(windowSizeMillis)
    val currentTimeMillis = 1719755742000L // 2024-06-30 13:55:42.000

    val expectedSawToothWindowStartMillis = 1719752100000L // 2024-06-30 12:55:00.000

    assertEquals(
      expectedSawToothWindowStartMillis,
      TileFetchingUtils.getSawToothWindowStartMillis(
        windowSizeMillis,
        currentTimeMillis,
        resolutionMillis
      )
    )
  }
  @Test
  def testGetTilesForWindowWithSingleWindowAndSingleTileSize(): Unit = {
    val windowSizeMillis = List(2 * 60 * 60 * 1000L) // 2 hours
    val afterTsMillis = 1706832000000L // February 2, 2024 0:00:00
    val currentTsMillis = 1706844420000L // February 2, 2024 3:27:00
    val tileSizes = Vector(TileSize.FiveMinutes)

    val actualTiles = TileFetchingUtils.getTilesForWindows(
      windowSizeMillis,
      afterTsMillis,
      currentTsMillis,
      tileSizes
    )

    // range from February 2, 2024 1:25:00 to February 2, 2024 3:25:00
    val expectedTiles = (1706837100000L to 1706844300000L by TileSize.FiveMinutes.millis)
      .map((_, TileSize.FiveMinutes.millis))
      .toSet

    assertEquals(expectedTiles, actualTiles)
  }

  @Test
  def testGetTilesForWindowWithSingleWindowAndTwoTileSizes(): Unit = {
    val windowSizeMillis = List(2 * 60 * 60 * 1000L) // 2 hours
    val afterTsMillis = 1706832000000L // Friday, February 2, 2024 0:00:00
    val currentTsMillis = 1706844420000L // Friday, February 2, 2024 3:27:00
    val tileSizes = Vector(TileSize.FiveMinutes, TileSize.OneHour)

    val actualTiles = TileFetchingUtils.getTilesForWindows(
      windowSizeMillis,
      afterTsMillis,
      currentTsMillis,
      tileSizes
    )

    val expectedTiles = Set(
      (1706837100000L, TileSize.FiveMinutes.millis), // Friday, February 2, 2024 1:25:00
      (1706837400000L, TileSize.FiveMinutes.millis), // Friday, February 2, 2024 1:30:00
      (1706837700000L, TileSize.FiveMinutes.millis), // Friday, February 2, 2024 1:35:00
      (1706838000000L, TileSize.FiveMinutes.millis), // Friday, February 2, 2024 1:40:00
      (1706838300000L, TileSize.FiveMinutes.millis), // Friday, February 2, 2024 1:45:00
      (1706838600000L, TileSize.FiveMinutes.millis), // Friday, February 2, 2024 1:50:00
      (1706838900000L, TileSize.FiveMinutes.millis), // Friday, February 2, 2024 1:55:00
      (1706839200000L, TileSize.OneHour.millis), // Friday, February 2, 2024 2:00:00
      (1706842800000L, TileSize.OneHour.millis), // Friday, February 2, 2024 3:00:00
    )

    assertEquals(expectedTiles, actualTiles)
  }

  @Test
  def testGetTilesForWindowWithTwoWindowsAndSingleTileSize(): Unit = {
    val windowSizeMillis = List(2 * 60 * 60 * 1000L, 6 * 60 * 60 * 1000L) // 2 hours and 6 hours
    val afterTsMillis = 1706832000000L // February 2, 2024 0:00:00
    val currentTsMillis = 1706844420000L // February 2, 2024 3:27:00
    val tileSizes = Vector(TileSize.FiveMinutes)

    val actualTiles = TileFetchingUtils.getTilesForWindows(
      windowSizeMillis,
      afterTsMillis,
      currentTsMillis,
      tileSizes
    )

    // range from February 2, 2024 0:00:00 to February 2, 2024 3:25:00
    val expectedTiles = (1706832000000L to 1706844300000L by TileSize.FiveMinutes.millis)
      .map((_, TileSize.FiveMinutes.millis))
      .toSet

    assertEquals(expectedTiles, actualTiles)
  }

  @Test
  def testGetTilesForWindowWithTwoWindowsAndTwoTileSizes(): Unit = {
    val windowSizeMillis = List(2 * 60 * 60 * 1000L, 6 * 60 * 60 * 1000L) // 2 hours and 6 hours
    val afterTsMillis = 1706832000000L // February 2, 2024 0:00:00
    val currentTsMillis = 1706844420000L // February 2, 2024 3:27:00
    val tileSizes = Vector(TileSize.FiveMinutes, TileSize.OneHour)

    val actualTiles = TileFetchingUtils.getTilesForWindows(
      windowSizeMillis,
      afterTsMillis,
      currentTsMillis,
      tileSizes
    )

    val expectedTiles = Set(
      (1706832000000L, TileSize.OneHour.millis), // Friday, February 2, 2024 0:00:00
      (1706835600000L, TileSize.OneHour.millis), // Friday, February 2, 2024 1:00:00
      (1706837100000L, TileSize.FiveMinutes.millis), // Friday, February 2, 2024 1:25:00
      (1706837400000L, TileSize.FiveMinutes.millis), // Friday, February 2, 2024 1:30:00
      (1706837700000L, TileSize.FiveMinutes.millis), // Friday, February 2, 2024 1:35:00
      (1706838000000L, TileSize.FiveMinutes.millis), // Friday, February 2, 2024 1:40:00
      (1706838300000L, TileSize.FiveMinutes.millis), // Friday, February 2, 2024 1:45:00
      (1706838600000L, TileSize.FiveMinutes.millis), // Friday, February 2, 2024 1:50:00
      (1706838900000L, TileSize.FiveMinutes.millis), // Friday, February 2, 2024 1:55:00
      (1706839200000L, TileSize.OneHour.millis), // Friday, February 2, 2024 2:00:00
      (1706842800000L, TileSize.OneHour.millis), // Friday, February 2, 2024 3:00:00
    )

    assertEquals(expectedTiles, actualTiles)
  }

  @Test
  def testGetTilesForWindowWithLongWindow(): Unit = {
    val windowSizeMillis = List(12 * 24 * 60 * 60 * 1000L) // 12 days
    val afterTsMillis = 1706832000000L // February 2, 2024 0:00:00
    val currentTsMillis = 1706844420000L // February 2, 2024 3:27:00
    val tileSizes = Vector(
      TileSize.FiveMinutes,
      TileSize.OneHour,
      TileSize.OneDay
    )

    val actualTiles = TileFetchingUtils.getTilesForWindows(
      windowSizeMillis,
      afterTsMillis,
      currentTsMillis,
      tileSizes
    )

    val expectedTiles = Set(
      (1706832000000L, TileSize.OneDay.millis) // Friday, February 2, 2024 0:00:00
    )

    assertEquals(expectedTiles, actualTiles)
  }

  @Test
  def testGetTilesForWindowWithShortWindow(): Unit = {
    val windowSizeMillis = List(10 * 60 * 1000L) // 10 min
    val afterTsMillis = 1706832000000L // February 2, 2024 0:00:00
    val currentTsMillis = 1706844420000L // February 2, 2024 3:27:00
    val tileSizes = Vector(
      TileSize.FiveMinutes,
      TileSize.OneHour,
      TileSize.OneDay
    )

    val actualTiles = TileFetchingUtils.getTilesForWindows(
      windowSizeMillis,
      afterTsMillis,
      currentTsMillis,
      tileSizes
    )

    val expectedTiles = Set(
      (1706843700000L, TileSize.FiveMinutes.millis), // Friday, February 2, 2024 3:15:00
      (1706844000000L, TileSize.FiveMinutes.millis), // Friday, February 2, 2024 3:20:00
      (1706844300000L, TileSize.FiveMinutes.millis), // Friday, February 2, 2024 3:25:00
    )

    assertEquals(expectedTiles, actualTiles)
  }

  @Test
  def testGetTilesForWindowWithWindowStartAfterAfterTsSeconds(): Unit = {
    val windowSizeMillis = List(1 * 60 * 60 * 1000L) // 1 hour
    val afterTsMillis = 1706832000000L // February 2, 2024 0:00:00
    val currentTsMillis = 1706844420000L // February 2, 2024 3:27:00
    val tileSizes = Vector(
      TileSize.FiveMinutes,
      TileSize.OneHour,
      TileSize.OneDay
    )

    val actualTiles = TileFetchingUtils.getTilesForWindows(
      windowSizeMillis,
      afterTsMillis,
      currentTsMillis,
      tileSizes
    )

    val expectedTiles = Set(
      (1706840700000L, TileSize.FiveMinutes.millis), // Friday, February 2, 2024 2:25:00
      (1706841000000L, TileSize.FiveMinutes.millis), // Friday, February 2, 2024 2:30:00
      (1706841300000L, TileSize.FiveMinutes.millis), // Friday, February 2, 2024 2:35:00
      (1706841600000L, TileSize.FiveMinutes.millis), // Friday, February 2, 2024 2:40:00
      (1706841900000L, TileSize.FiveMinutes.millis), // Friday, February 2, 2024 2:45:00
      (1706842200000L, TileSize.FiveMinutes.millis), // Friday, February 2, 2024 2:50:00
      (1706842500000L, TileSize.FiveMinutes.millis), // Friday, February 2, 2024 2:55:00
      (1706842800000L, TileSize.OneHour.millis) // Friday, February 2, 2024 3:00:00
    )

    assertEquals(expectedTiles, actualTiles)
  }

  @Test
  def testGetTilesForWindowWithRangeToFillOnRight(): Unit = {
    val windowSizeMillis = List(2 * 60 * 60 * 1000L) // 2 hours
    val afterTsMillis = 1706832000000L // February 2, 2024 0:00:00
    val currentTsMillis = 1706843400000L // February 2, 2024 3:10:00

    // Custom tile sizes to force range filling on the right
    val customEightyMinuteTileSize = mock(classOf[TileSize])
    when(customEightyMinuteTileSize.seconds).thenReturn(80 * 60)
    when(customEightyMinuteTileSize.millis).thenReturn(80 * 60 * 1000)
    when(customEightyMinuteTileSize.keyString).thenReturn("80m")

    val customThreeHourTileSize = mock(classOf[TileSize])
    when(customThreeHourTileSize.seconds).thenReturn(3 * 60 * 60)
    when(customThreeHourTileSize.millis).thenReturn(3 * 60 * 60 * 1000)
    when(customThreeHourTileSize.keyString).thenReturn("3h")

    val tileSizes = Vector(
      customEightyMinuteTileSize,
      customThreeHourTileSize,
      TileSize.FiveMinutes
    )

    val actualTiles = TileFetchingUtils.getTilesForWindows(
      windowSizeMillis,
      afterTsMillis,
      currentTsMillis,
      tileSizes
    )

    val expectedTiles = Set(
      (1706842800000L, customThreeHourTileSize.millis), // Friday, February 2, 2024 3:00:00
      (1706836800000L, customEightyMinuteTileSize.millis), // Friday, February 2, 2024 3:05:00
      (1706836200000L, TileSize.FiveMinutes.millis), // Friday, February 2, 2024 1:10:00
      (1706836500000L, TileSize.FiveMinutes.millis), // Friday, February 2, 2024 1:15:00
      (1706841600000L, TileSize.FiveMinutes.millis), // Friday, February 2, 2024 2:40:00
      (1706841900000L, TileSize.FiveMinutes.millis), // Friday, February 2, 2024 2:45:00
      (1706842200000L, TileSize.FiveMinutes.millis), // Friday, February 2, 2024 2:50:00
      (1706842500000L, TileSize.FiveMinutes.millis) // Friday, February 2, 2024 2:55:00
    )

    assertEquals(expectedTiles, actualTiles)
  }
  @Test
  def testGetTilesForWindowWhenAfterTsMillisEqualsCurrentTimeMillis(): Unit = {
    val windowSizeMillis = List(20 * 60 * 1000L, 2 * 60 * 60 * 1000L, 6 * 60 * 60 * 1000L)
    val afterTsMillis = 1706832000000L // February 2, 2024 0:00:00
    val currentTsMillis = 1706832000000L // February 2, 2024 0:00:00
    val tileSizes = Vector(TileSize.FiveMinutes)

    val actualTiles = TileFetchingUtils.getTilesForWindows(
      windowSizeMillis,
      afterTsMillis,
      currentTsMillis,
      tileSizes
    )

    val expectedTiles = Set(
      (1706832000000L, TileSize.FiveMinutes.millis) // Friday, February 2, 2024 0:00:00
    )

    assertEquals(expectedTiles, actualTiles)
  }

  @Test(expected = classOf[IllegalStateException])
  def testGetTilesForWindowWhenCurrentTimeMillisBeforeAfterTsMillis(): Unit = {
    val windowSizeMillis = List(60 * 60 * 1000L)
    val afterTsMillis = 1706832000000L // February 2, 2024 0:00:00
    val currentTsMillis = 1706831400000L // February 1, 2024 23:50:00
    val tileSizes = Vector(TileSize.OneHour, TileSize.FiveMinutes)

    TileFetchingUtils.getTilesForWindows(
      windowSizeMillis,
      afterTsMillis,
      currentTsMillis,
      tileSizes
    )
  }

  @Test
  def testGetTilesForWindowWhenCurrentTimeMillisExactlyEqualsNextTileStart(): Unit = {
    val windowSizeMillis = List(60 * 60 * 1000L)
    val afterTsMillis = 1706832000000L // February 2, 2024 0:00:00
    val currentTsMillis = 1706835600000L // February 2, 2024 1:00:00
    val tileSizes = Vector(TileSize.OneHour, TileSize.FiveMinutes)

    val actualTiles = TileFetchingUtils.getTilesForWindows(
      windowSizeMillis,
      afterTsMillis,
      currentTsMillis,
      tileSizes
    )

    val expectedTiles = Set(
      (1706832000000L, TileSize.OneHour.millis), // Friday, February 2, 2024 0:00:00
      (1706835600000L, TileSize.OneHour.millis) // Friday, February 2, 2024 1:00:00
    )

    assertEquals(expectedTiles, actualTiles)
  }

  /**
   * Helper function to assert there are no gaps between tiles.
   * Tests the tile fetching algorithm at different timestamps and verifies tiles returned never have gaps.
   */
  private def assertThereAreNoGapsBetweenTiles(
                                                windowSizesMillis: List[Long],
                                                afterTsMillis: Long,
                                                tileSizesAvailable: IndexedSeq[TileSize]
                                              ): Unit = {
    // Test all window sizes separately
    windowSizesMillis.foreach { selectedWindowSizeMillis =>
      // Iterate through every 1-minute timestamp from February 2, 2024 0:01:00 to February 3, 2024 23:59:00
      (afterTsMillis + 60 * 1000 until afterTsMillis + 60 * 1000 + 60 * 60 * 24 * 2 * 1000 by 60 * 1000)
        .map { currentTsMillis =>
          // Add jitter, check the ms before the minute, the exact minute itself, and the ms after
          (-1 to 1).map { j =>
            val currentTsMillisWithJitter = currentTsMillis + j
            val sortedTiles = TileFetchingUtils
              .getTilesForWindows(
                Seq(selectedWindowSizeMillis),
                afterTsMillis,
                currentTsMillisWithJitter,
                tileSizesAvailable
              )
              .toSeq
              .sortBy(_._1)

            // First tile is at or after February 2, 2024 0:00:00
            assertTrue(sortedTiles.head._1 >= afterTsMillis)

            // Last tile is at or after current time
            assertTrue(sortedTiles.last._1 + sortedTiles.last._2 >= currentTsMillisWithJitter)

            // Check if there are no gaps between tiles
            if (sortedTiles.size > 1) {
              sortedTiles.sliding(2).foreach { pair =>
                // First time start + first tile size = second time start
                assertEquals(pair.head._1 + pair.head._2, pair.last._1)
              }
            }
          }
        }
    }
  }

  @Test
  def testNoGapsBetweenTilesWithOneHourAndFiveMinuteTiles(): Unit = {
    val windowSizeMillis = List(
      10 * 60 * 1000L,
      60 * 60 * 1000L,
      24 * 60 * 60 * 1000L,
      7 * 24 * 60 * 60 * 1000L
    )
    val afterTsMillis = 1706832000000L // February 2, 2024 0:00:00
    val tileSizes = Vector(
      TileSize.FiveMinutes,
      TileSize.OneHour
    )

    assertThereAreNoGapsBetweenTiles(windowSizeMillis, afterTsMillis, tileSizes)
  }
}
