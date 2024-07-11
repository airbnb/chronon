package ai.chronon.aggregator.test

import ai.chronon.aggregator.row.RowAggregator
import ai.chronon.aggregator.windowing.{SawtoothMutationAggregator, TiledIr}
import ai.chronon.api.Extensions.AggregationOps
import ai.chronon.api.{Aggregation, Builders, DataType, IntType, LongType, Operation, StringType, TimeUnit, Window}
import junit.framework.TestCase
import org.junit.Assert.{assertEquals, assertNull}

class SawtoothMutationAggregatorTest extends TestCase {

  val FiveMinuteTileSize = 5 * 60 * 1000L
  val OneHourTileSize = 60 * 60 * 1000L
  val OneDayTileSize = 24 * 60 * 60 * 1000L

  def constructTileIr(aggregations: Seq[Aggregation],
                      inputSchema: Seq[(String, DataType)],
                      events: Seq[TestRow]): Array[Any] = {
    val aggregator = new RowAggregator(inputSchema, aggregations.flatMap(_.unpack))
    val aggIr = aggregator.init

    events.map { e =>
      aggregator.update(aggIr, e)
    }

    aggIr
  }

  def testUpdateIrTiled(): Unit = {
    val aggregations = Seq(
      Builders.Aggregation(
        operation = Operation.SUM,
        inputColumn = "rating",
        windows = Seq(new Window(6, TimeUnit.HOURS))
      ),
      Builders.Aggregation(
        operation = Operation.AVERAGE,
        inputColumn = "rating",
        windows = Seq(new Window(1, TimeUnit.DAYS))
      )
    )
    val inputSchema: Seq[(String, DataType)] = Seq(
      ("ts_millis", LongType),
      ("listing_id", StringType),
      ("rating", IntType)
    )

    val queryTs = 1707167971000L // Monday, February 5, 2024 9:19:31 PM
    val batchEndTs = 1707091200000L // Monday, February 5, 2024 12:00:00 AM

    // The aggregator should include tile1 and tile4 and skip tiles 2 and 3 since
    // they fall into the same hour block as tile 1. Tile0 should be ignored as it
    // comes before batchEndTs.

    val tile0 = TiledIr(
      1707087600000L, // [11:00 PM , 12:00 AM)
      constructTileIr(
        aggregations,
        inputSchema,
        Seq(
          TestRow(1707087600000L, "listing_1", 10)
        )
      ),
      OneHourTileSize
    )

    val tile1 = TiledIr(
      1707091200000L, // [12:00 AM , 1:00 AM)
      constructTileIr(
        aggregations,
        inputSchema,
        Seq(
          TestRow(1707091210000L, "listing_1", 4),
          TestRow(1707091220000L, "listing_1", 5),
          TestRow(1707091510000L, "listing_1", 6)
        )
      ),
      OneHourTileSize
    )

    val tile2 = TiledIr(
      1707091200000L, // [12:00 AM, 12:05 AM)
      constructTileIr(
        aggregations,
        inputSchema,
        Seq(
          TestRow(1707091210000L, "listing_1", 4),
          TestRow(1707091220000L, "listing_1", 5)
        )
      ),
      FiveMinuteTileSize
    )

    val tile3 = TiledIr(
      1707091500000L, // [12:05 AM, 12:10 AM)
      constructTileIr(
        aggregations,
        inputSchema,
        Seq(
          TestRow(1707091510000L, "listing_1", 6)
        )
      ),
      FiveMinuteTileSize
    )

    val tile4 = TiledIr(
      1707094800000L, // [1:00 AM, 1:05 AM)
      constructTileIr(
        aggregations,
        inputSchema,
        Seq(
          TestRow(1707094800000L, "listing_1", 10)
        )
      ),
      FiveMinuteTileSize
    )

    val streamingTiledIrs: Seq[TiledIr] = Seq(
      tile0,
      tile1,
      tile2,
      tile3,
      tile4
    )

    val sawtoothMutationAggregator = new SawtoothMutationAggregator(aggregations, inputSchema)
    val ir = sawtoothMutationAggregator.init

    sawtoothMutationAggregator.updateIrTiledWithTileLayering(
      ir.collapsed,
      streamingTiledIrs,
      queryTs,
      batchEndTs
    )

    // assert ir now contains the right values
    assertEquals(2, ir.collapsed.length)

    // 6 hour window is null since there were no events within the time frame
    assertNull(ir.collapsed(0))
    // 1 day window has items from tile1 and tile4
    assertEquals(25.0, ir.collapsed(1).asInstanceOf[Array[Any]](0))
    assertEquals(4, ir.collapsed(1).asInstanceOf[Array[Any]](1))
  }

  def testUpdateIrTiledMidHourWindowStart(): Unit = {
    val aggregations = Seq(
      Builders.Aggregation(
        operation = Operation.AVERAGE,
        inputColumn = "rating",
        windows = Seq(new Window(6, TimeUnit.HOURS))
      )
    )
    val inputSchema: Seq[(String, DataType)] = Seq(
      ("ts_millis", LongType),
      ("listing_id", StringType),
      ("rating", IntType)
    )

    val queryTs = 1707167971000L // Monday, February 5, 2024 9:19:31 PM
    val batchEndTs = 1707091200000L // Monday, February 5, 2024 12:00:00 AM

    // Window start will be 3:19 PM.
    // The aggregator should include tile3 and tile4 and skip tiles 1 and 2 since
    // they are timestamped before the start of the window rounded down to the hop size for the aggregator.

    val tile1 = TiledIr(
      1707145200000L, // [3:00 PM , 4:00 PM)
      constructTileIr(
        aggregations,
        inputSchema,
        Seq(
          TestRow(1707145210000L, "listing_1", 4),
          TestRow(1707145220000L, "listing_1", 5),
          TestRow(1707146100000L, "listing_1", 6),
          TestRow(1707146400000L, "listing_1", 10)
        )
      ),
      OneHourTileSize
    )

    val tile2 = TiledIr(
      1707145200000L, // [3:00 PM, 3:05 PM)
      constructTileIr(
        aggregations,
        inputSchema,
        Seq(
          TestRow(1707145210000L, "listing_1", 4),
          TestRow(1707145220000L, "listing_1", 5)
        )
      ),
      FiveMinuteTileSize
    )

    val tile3 = TiledIr(
      1707146100000L, // [3:15 PM, 3:20 PM)
      constructTileIr(
        aggregations,
        inputSchema,
        Seq(
          TestRow(1707146100000L, "listing_1", 6)
        )
      ),
      FiveMinuteTileSize
    )

    val tile4 = TiledIr(
      1707146400000L, // [3:20 PM, 3:25 PM)
      constructTileIr(
        aggregations,
        inputSchema,
        Seq(
          TestRow(1707146400000L, "listing_1", 10)
        )
      ),
      FiveMinuteTileSize
    )

    val streamingTiledIrs: Seq[TiledIr] = Seq(
      tile1,
      tile2,
      tile3,
      tile4
    )

    val sawtoothMutationAggregator = new SawtoothMutationAggregator(aggregations, inputSchema)
    val ir = sawtoothMutationAggregator.init

    sawtoothMutationAggregator.updateIrTiledWithTileLayering(
      ir.collapsed,
      streamingTiledIrs,
      queryTs,
      batchEndTs
    )

    assertEquals(1, ir.collapsed.length)

    assertEquals(16.0, ir.collapsed(0).asInstanceOf[Array[Any]](0))
    assertEquals(2, ir.collapsed(0).asInstanceOf[Array[Any]](1))
  }

  def testUpdateIrTiledHandlesUnorderedTiles(): Unit = {
    val aggregations = Seq(
      Builders.Aggregation(
        operation = Operation.SUM,
        inputColumn = "rating",
        windows = Seq(new Window(6, TimeUnit.HOURS))
      ),
      Builders.Aggregation(
        operation = Operation.AVERAGE,
        inputColumn = "rating",
        windows = Seq(new Window(1, TimeUnit.DAYS))
      )
    )
    val inputSchema: Seq[(String, DataType)] = Seq(
      ("ts_millis", LongType),
      ("listing_id", StringType),
      ("rating", IntType)
    )

    val queryTs = 1707167971000L // Monday, February 5, 2024 9:19:31 PM
    val batchEndTs = 1707091200000L // Monday, February 5, 2024 12:00:00 AM

    // The aggregator should include tile1 and tile4 and skip tiles 2 and 3 since
    // they fall into the same hour block as tile 1.

    val tile1 = TiledIr(
      1707091200000L, // [12:00 AM , 1:00 AM)
      constructTileIr(
        aggregations,
        inputSchema,
        Seq(
          TestRow(1707091210000L, "listing_1", 4),
          TestRow(1707091220000L, "listing_1", 5),
          TestRow(1707091510000L, "listing_1", 6)
        )
      ),
      OneHourTileSize
    )

    val tile2 = TiledIr(
      1707091200000L, // [12:00 AM, 12:05 AM)
      constructTileIr(
        aggregations,
        inputSchema,
        Seq(
          TestRow(1707091210000L, "listing_1", 4),
          TestRow(1707091220000L, "listing_1", 5)
        )
      ),
      FiveMinuteTileSize
    )

    val tile3 = TiledIr(
      1707091500000L, // [12:05 AM, 12:10 AM)
      constructTileIr(
        aggregations,
        inputSchema,
        Seq(
          TestRow(1707091510000L, "listing_1", 6)
        )
      ),
      FiveMinuteTileSize
    )

    val tile4 = TiledIr(
      1707094800000L, // [1:00 AM, 1:05 AM)
      constructTileIr(
        aggregations,
        inputSchema,
        Seq(
          TestRow(1707094800000L, "listing_1", 10)
        )
      ),
      FiveMinuteTileSize
    )

    // The tiles are out of order, with two five minute tiles before the one hour tile.
    // Tiles 1 and 2 have the same start time but tile 1 is larger so it should be taken first
    // and tile 2 should be ignored.
    val streamingTiledIrs: Seq[TiledIr] = Seq(
      tile3,
      tile2,
      tile1,
      tile4
    )

    val sawtoothMutationAggregator = new SawtoothMutationAggregator(aggregations, inputSchema)
    val ir = sawtoothMutationAggregator.init

    sawtoothMutationAggregator.updateIrTiledWithTileLayering(
      ir.collapsed,
      streamingTiledIrs,
      queryTs,
      batchEndTs
    )

    // assert ir now contains the right values
    assertEquals(2, ir.collapsed.length)

    // 6 hour window is null since there were no events within the time frame
    assertNull(ir.collapsed(0))
    // 1 day window has items from tile1 and tile4
    assertEquals(25.0, ir.collapsed(1).asInstanceOf[Array[Any]](0))
    assertEquals(4, ir.collapsed(1).asInstanceOf[Array[Any]](1))
  }

  def testUpdateIrTiledMultipleShortWindows(): Unit = {
    val aggregations = Seq(
      Builders.Aggregation(
        operation = Operation.AVERAGE,
        inputColumn = "rating",
        windows = Seq(new Window(6, TimeUnit.HOURS), new Window(12, TimeUnit.HOURS))
      )
    )
    val inputSchema: Seq[(String, DataType)] = Seq(
      ("ts_millis", LongType),
      ("listing_id", StringType),
      ("rating", IntType)
    )

    val queryTs = 1707167971000L // Monday, February 5, 2024 9:19:31 PM
    val batchEndTs = 1707091200000L // Monday, February 5, 2024 12:00:00 AM

    // Window start will be 3:19 PM for the 6 hour window and 9:19 AM for the 12 hour window.
    // The aggregator should include tiles 1, 2, and 3 for the 12 hour window and tiles
    // 5, 6 for the 6 hour window.

    val tile1 = TiledIr(
      1707124500000L, // [9:15 AM, 9:20 AM)
      constructTileIr(
        aggregations,
        inputSchema,
        Seq(
          TestRow(1707124500000L, "listing_1", 1),
          TestRow(1707124500000L, "listing_1", 2)
        )
      ),
      FiveMinuteTileSize
    )

    val tile2 = TiledIr(
      1707124800000L, // [9:20 AM, 9:25 AM)
      constructTileIr(
        aggregations,
        inputSchema,
        Seq(
          TestRow(1707124800000L, "listing_1", 3)
        )
      ),
      FiveMinuteTileSize
    )

    val tile3 = TiledIr(
      1707145200000L, // [3:00 PM , 4:00 PM)
      constructTileIr(
        aggregations,
        inputSchema,
        Seq(
          TestRow(1707145210000L, "listing_1", 4),
          TestRow(1707145220000L, "listing_1", 5),
          TestRow(1707146100000L, "listing_1", 6),
          TestRow(1707146400000L, "listing_1", 10)
        )
      ),
      OneHourTileSize
    )

    val tile4 = TiledIr(
      1707145200000L, // [3:00 PM, 3:05 PM)
      constructTileIr(
        aggregations,
        inputSchema,
        Seq(
          TestRow(1707145210000L, "listing_1", 4),
          TestRow(1707145220000L, "listing_1", 5)
        )
      ),
      FiveMinuteTileSize
    )

    val tile5 = TiledIr(
      1707146100000L, // [3:15 PM, 3:20 PM)
      constructTileIr(
        aggregations,
        inputSchema,
        Seq(
          TestRow(1707146100000L, "listing_1", 6)
        )
      ),
      FiveMinuteTileSize
    )

    val tile6 = TiledIr(
      1707146400000L, // [3:20 PM, 3:25 PM)
      constructTileIr(
        aggregations,
        inputSchema,
        Seq(
          TestRow(1707146400000L, "listing_1", 10)
        )
      ),
      FiveMinuteTileSize
    )

    val streamingTiledIrs: Seq[TiledIr] = Seq(
      tile1,
      tile2,
      tile3,
      tile4,
      tile5,
      tile6
    )

    val sawtoothMutationAggregator = new SawtoothMutationAggregator(aggregations, inputSchema)
    val ir = sawtoothMutationAggregator.init

    sawtoothMutationAggregator.updateIrTiledWithTileLayering(
      ir.collapsed,
      streamingTiledIrs,
      queryTs,
      batchEndTs
    )

    assertEquals(2, ir.collapsed.length)

    assertEquals(16.0, ir.collapsed(0).asInstanceOf[Array[Any]](0))
    assertEquals(2, ir.collapsed(0).asInstanceOf[Array[Any]](1))

    assertEquals(31.0, ir.collapsed(1).asInstanceOf[Array[Any]](0))
    assertEquals(7, ir.collapsed(1).asInstanceOf[Array[Any]](1))
  }

  def testUpdateIrTiledArbitraryTileSize(): Unit = {
    val aggregations = Seq(
      Builders.Aggregation(
        operation = Operation.AVERAGE,
        inputColumn = "rating",
        windows = Seq(new Window(3, TimeUnit.HOURS))
      )
    )
    val inputSchema: Seq[(String, DataType)] = Seq(
      ("ts_millis", LongType),
      ("listing_id", StringType),
      ("rating", IntType)
    )

    val queryTs = 1707103140000L // Monday, February 5, 2024 3:19:00 AM
    val batchEndTs = 1707091200000L // Monday, February 5, 2024 12:00:00 AM

    // Window has a 12:19 start.
    // The aggregator should include tile3 and ignore tiles 1 and 2.

    val tile1 = TiledIr(
      1707091200000L, // [12:00 AM, 12:08 AM)
      constructTileIr(
        aggregations,
        inputSchema,
        Seq(
          TestRow(1707091210000L, "listing_1", 4),
          TestRow(1707091220000L, "listing_1", 5)
        )
      ),
      8 * 60 * 1000L // 8 minutes
    )

    val tile2 = TiledIr(
      1707091680000L, // [12:08 AM, 12:16 AM)
      constructTileIr(
        aggregations,
        inputSchema,
        Seq(
          TestRow(1707092160000L, "listing_1", 6)
        )
      ),
      8 * 60 * 1000L // 8 minutes
    )

    val tile3 = TiledIr(
      1707092160000L, // [12:16 AM, 12:24 AM)
      constructTileIr(
        aggregations,
        inputSchema,
        Seq(
          TestRow(1707092160000L, "listing_1", 10)
        )
      ),
      8 * 60 * 1000L // 8 minutes
    )

    val streamingTiledIrs: Seq[TiledIr] = Seq(
      tile1,
      tile2,
      tile3
    )

    val sawtoothMutationAggregator = new SawtoothMutationAggregator(aggregations, inputSchema)
    val ir = sawtoothMutationAggregator.init

    sawtoothMutationAggregator.updateIrTiledWithTileLayering(
      ir.collapsed,
      streamingTiledIrs,
      queryTs,
      batchEndTs
    )

    assertEquals(1, ir.collapsed.length)

    assertEquals(10.0, ir.collapsed(0).asInstanceOf[Array[Any]](0))
    assertEquals(1, ir.collapsed(0).asInstanceOf[Array[Any]](1))
  }

  def testUpdateIrTiledDayTiles(): Unit = {
    val aggregations = Seq(
      Builders.Aggregation(
        operation = Operation.AVERAGE,
        inputColumn = "rating",
        windows = Seq(new Window(12, TimeUnit.DAYS))
      )
    )
    val inputSchema: Seq[(String, DataType)] = Seq(
      ("ts_millis", LongType),
      ("listing_id", StringType),
      ("rating", IntType)
    )

    val queryTs = 1707167971000L // Monday, February 5, 2024 9:19:31 PM
    val batchEndTs = 1707004800000L // Monday, February 4, 2024 12:00:00 AM

    // The aggregator should include tile0 and tile1 and exclude the rest.
    val tile0 = TiledIr(
      1707004800000L, // [12:00 AM , 12:00 AM)
      constructTileIr(
        aggregations,
        inputSchema,
        Seq(
          TestRow(1707004800000L, "listing_1", 1)
        )
      ),
      OneDayTileSize
    )

    val tile1 = TiledIr(
      1707087600000L, // [11:00 PM , 12:00 AM)
      constructTileIr(
        aggregations,
        inputSchema,
        Seq(
          TestRow(1707087600000L, "listing_1", 10)
        )
      ),
      OneHourTileSize
    )

    val tile2 = TiledIr(
      1707091200000L, // [12:00 AM , 12:00 AM)
      constructTileIr(
        aggregations,
        inputSchema,
        Seq(
          TestRow(1707091210000L, "listing_1", 4),
          TestRow(1707091220000L, "listing_1", 5),
          TestRow(1707091510000L, "listing_1", 6),
          TestRow(1707094800000L, "listing_1", 10)
        )
      ),
      OneDayTileSize
    )

    val tile3 = TiledIr(
      1707091200000L, // [12:00 AM, 12:05 AM)
      constructTileIr(
        aggregations,
        inputSchema,
        Seq(
          TestRow(1707091210000L, "listing_1", 4),
          TestRow(1707091220000L, "listing_1", 5)
        )
      ),
      FiveMinuteTileSize
    )

    val tile4 = TiledIr(
      1707091500000L, // [12:05 AM, 12:10 AM)
      constructTileIr(
        aggregations,
        inputSchema,
        Seq(
          TestRow(1707091510000L, "listing_1", 6)
        )
      ),
      FiveMinuteTileSize
    )

    val tile5 = TiledIr(
      1707094800000L, // [1:00 AM, 1:05 AM)
      constructTileIr(
        aggregations,
        inputSchema,
        Seq(
          TestRow(1707094800000L, "listing_1", 10)
        )
      ),
      FiveMinuteTileSize
    )

    val streamingTiledIrs: Seq[TiledIr] = Seq(
      tile0,
      tile1,
      tile2,
      tile3,
      tile4,
      tile5
    )

    val sawtoothMutationAggregator = new SawtoothMutationAggregator(aggregations, inputSchema)
    val ir = sawtoothMutationAggregator.init

    sawtoothMutationAggregator.updateIrTiledWithTileLayering(
      ir.collapsed,
      streamingTiledIrs,
      queryTs,
      batchEndTs
    )

    assertEquals(1, ir.collapsed.length)

    assertEquals(26.0, ir.collapsed(0).asInstanceOf[Array[Any]](0))
    assertEquals(5, ir.collapsed(0).asInstanceOf[Array[Any]](1))
  }

  def testManyWindowSizesManyTileSizes(): Unit = {
    val aggregations = Seq(
      Builders.Aggregation(
        operation = Operation.SUM,
        inputColumn = "rating",
        windows = Seq(
          new Window(30, TimeUnit.DAYS),
          new Window(7, TimeUnit.DAYS),
          new Window(1, TimeUnit.DAYS),
          new Window(1, TimeUnit.HOURS),
          new Window(10, TimeUnit.MINUTES)
        )
      )
    )
    val inputSchema: Seq[(String, DataType)] = Seq(
      ("ts_millis", LongType),
      ("listing_id", StringType),
      ("rating", IntType)
    )

    val queryTs = 1707095520000L // Monday, February 5, 2024 1:12:00 UTC
    val batchEndTs = 1707004800000L // Sunday, February 4, 2024 0:00:00 UTC (25h12m before queryTs)

    // Create some new custom tile sizes
    val TwentyMinuteTileSize = 20 * 60 * 1000L
    val SixHourTileSize = 6 * 60 * 60 * 1000L

    def getTile(ts: Long, rows: Seq[TestRow], size: Long): TiledIr = {
      TiledIr(
        ts,
        constructTileIr(
          aggregations,
          inputSchema,
          rows
        ),
        size
      )
    }

    // Prepare some tiles. The actual values in them (1, 10, 100, 1000) aren't realistic but that's fine, we're just
    // making sure the correct tiles are being aggregated.
    val tiles = Seq(
      // first day
      getTile(1707004800000L, Seq(TestRow(1707004800000L, "listing_1", 1000)), SixHourTileSize), // [00:00, 6:00)
      getTile(1707008400000L, Seq(TestRow(1707008400000L, "listing_1", 100)), OneHourTileSize), // [1:00, 2:00)
      getTile(1707012000000L, Seq(TestRow(1707012000000L, "listing_1", 100)), OneHourTileSize), // [2:00, 3:00)
      getTile(1707015600000L, Seq(TestRow(1707015600000L, "listing_1", 100)), OneHourTileSize), // [3:00, 4:00)
      getTile(1707019200000L, Seq(TestRow(1707019200000L, "listing_1", 100)), OneHourTileSize), // [4:00, 5:00)
      getTile(1707022800000L, Seq(TestRow(1707022800000L, "listing_1", 100)), OneHourTileSize), // [5:00, 6:00)
      getTile(1707026400000L, Seq(TestRow(1707026400000L, "listing_1", 1000)), SixHourTileSize), // [6:00, 12:00)
      getTile(1707048000000L, Seq(TestRow(1707048000000L, "listing_1", 1000)), SixHourTileSize), // [12:00, 18:00)
      getTile(1707069600000L, Seq(TestRow(1707069600000L, "listing_1", 1000)), SixHourTileSize), // [18:00, 24:00)
      // next day
      getTile(1707091200000L, Seq(TestRow(1707091200000L, "listing_1", 1000)), SixHourTileSize), // [00:00, 6:00)
      getTile(1707091800000L, Seq(TestRow(1707091800000L, "listing_1", 1)), FiveMinuteTileSize), // [0:10, 0:15)
      getTile(1707092100000L, Seq(TestRow(1707092100000L, "listing_1", 1)), FiveMinuteTileSize), // [0:15, 0:20)
      getTile(1707092400000L, Seq(TestRow(1707092400000L, "listing_1", 10)), TwentyMinuteTileSize), // [0:20, 0:40)
      getTile(1707093600000L, Seq(TestRow(1707093600000L, "listing_1", 10)), TwentyMinuteTileSize), // [0:40, 1:00)
      getTile(1707094800000L, Seq(TestRow(1707094800000L, "listing_1", 100)), OneHourTileSize) // [1:00, 2:00)
    )

    val sawtoothMutationAggregator = new SawtoothMutationAggregator(aggregations, inputSchema)
    val ir = sawtoothMutationAggregator.init

    sawtoothMutationAggregator.updateIrTiledWithTileLayering(
      ir.collapsed,
      tiles,
      queryTs,
      batchEndTs
    )

    // Assert IR now contains the right values
    assertEquals(5, ir.collapsed.length)

    // 30-day window: sum of all 6hr tiles
    assertEquals(5000L, ir.collapsed(0))

    // 7day window: sum of all 6hr tiles
    assertEquals(5000L, ir.collapsed(1))

    // 1-day window:
    //  four 6-hour tiles [6:00, 12:00), [12:00, 18:00), [18:00, 24:00), [00:00, 6:00),
    //  five 1-hour tiles [1:00, 2:00), [2:00, 3:00), [3:00, 4:00), [4:00, 5:00), [5:00, 6:00)
    assertEquals(4500L, ir.collapsed(2))

    // 1-hour window:
    //  one 1-hour tile [1:00, 2:00),
    //  two 20-min tiles [0:20, 0:40), [0:40, 1:00),
    //  two 5-min tiles [0:10, 0:15), [0:15, 0:20)
    assertEquals(122L, ir.collapsed(3))

    // 10-min window: one 1-hour tile [1:00, 2:00)
    assertEquals(100L, ir.collapsed(4))
  }
}
