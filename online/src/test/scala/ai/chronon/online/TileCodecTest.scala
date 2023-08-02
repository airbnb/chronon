package ai.chronon.online

import ai.chronon.api.{Aggregation, Builders, FloatType, GroupBy, IntType, ListType, LongType, Operation, Row, StringType, TimeUnit, Window}
import org.junit.Assert.{assertEquals, assertTrue, assertFalse}
import org.junit.Test

import scala.collection.JavaConverters._

class TileCodecTest {
  private val histogram = Map[String, Int]("A" -> 3, "B" -> 2).asJava

  private val aggregationsAndExpected: Array[(Aggregation, Any)] = Array(
    Builders.Aggregation(Operation.AVERAGE, "views", Seq(new Window(1, TimeUnit.DAYS))) -> 16.0,
    Builders.Aggregation(Operation.AVERAGE, "rating", Seq(new Window(1, TimeUnit.DAYS))) -> 4.0,

    Builders.Aggregation(Operation.SUM, "rating", Seq(new Window(1, TimeUnit.DAYS))) -> 12.0f,
    Builders.Aggregation(Operation.SUM, "rating", Seq(new Window(7, TimeUnit.DAYS))) -> 12.0f,

    Builders.Aggregation(Operation.UNIQUE_COUNT, "title", Seq(new Window(1, TimeUnit.DAYS))) -> 3L,
    Builders.Aggregation(Operation.UNIQUE_COUNT, "title", Seq(new Window(7, TimeUnit.DAYS))) -> 3L,

    Builders.Aggregation(Operation.LAST, "title", Seq(new Window(1, TimeUnit.DAYS))) -> "C",
    Builders.Aggregation(Operation.LAST, "title", Seq(new Window(7, TimeUnit.DAYS))) -> "C",

    Builders.Aggregation(Operation.LAST_K, "title", Seq(new Window(1, TimeUnit.DAYS)), argMap = Map("k" -> "2")) -> List("C", "B").asJava,
    Builders.Aggregation(Operation.LAST_K, "title", Seq(new Window(7, TimeUnit.DAYS)), argMap = Map("k" -> "2")) -> List("C", "B").asJava,

    Builders.Aggregation(Operation.TOP_K, "title", Seq(new Window(1, TimeUnit.DAYS)), argMap = Map("k" -> "1")) -> List("C").asJava,
    Builders.Aggregation(Operation.TOP_K, "title", Seq(new Window(7, TimeUnit.DAYS)), argMap = Map("k" -> "1")) -> List("C").asJava,

    Builders.Aggregation(Operation.MIN, "title", Seq(new Window(1, TimeUnit.DAYS))) -> "A",
    Builders.Aggregation(Operation.MIN, "title", Seq(new Window(7, TimeUnit.DAYS))) -> "A",

    Builders.Aggregation(Operation.APPROX_UNIQUE_COUNT, "title", Seq(new Window(1, TimeUnit.DAYS))) -> 3L,
    Builders.Aggregation(Operation.APPROX_UNIQUE_COUNT, "title", Seq(new Window(7, TimeUnit.DAYS))) -> 3L,

    Builders.Aggregation(Operation.HISTOGRAM, "hist_input", Seq(new Window(1, TimeUnit.DAYS)), argMap = Map("k" -> "2")) -> histogram,
    Builders.Aggregation(Operation.HISTOGRAM, "hist_input", Seq(new Window(7, TimeUnit.DAYS)), argMap = Map("k" -> "2")) -> histogram
  )

  private val schema = List(
    "created" -> LongType,
    "views" -> IntType,
    "rating" -> FloatType,
    "title" -> StringType,
    "hist_input" -> ListType(StringType)
  )

  def createRow(ts: Long, views: Int, rating: Float, title: String, histInput: Seq[String]): Row = {
    val values: Array[(String, Any)] = Array(
      "created" -> ts,
      "views" -> views,
      "rating" -> rating,
      "title" -> title,
      "hist_input" -> histInput
    )
    new ArrayRow(values.map(_._2), ts)
  }

  @Test
  def testTileCodecIrSerRoundTrip(): Unit = {
    val groupByMetadata = Builders.MetaData(name = "my_group_by")
    val (aggregations, expectedVals) = aggregationsAndExpected.unzip
    val groupBy = Builders.GroupBy(metaData = groupByMetadata, aggregations = aggregations)
    val rowAggregator = TileCodec.buildRowAggregator(groupBy, schema)
    val rowIR = rowAggregator.init
    val tileCodec = new TileCodec(rowAggregator, groupBy)

    val originalIsComplete = true
    val rows = Seq(
      createRow(1519862399984L, 4, 4.0f, "A", Seq("D", "A", "B", "A")),
      createRow(1519862399984L, 40, 5.0f, "B", Seq()),
      createRow(1519862399988L, 4, 3.0f, "C", Seq("A", "B", "C"))
    )
    rows.foreach(row => rowAggregator.update(rowIR, row))
    val bytes = tileCodec.makeTileIr(rowIR, originalIsComplete)
    assert(bytes.length > 0)

    val (deserPayload, isComplete) = tileCodec.decodeTileIr(bytes)
    assert(isComplete == originalIsComplete)

    // lets finalize the payload intermediate results and verify things
    val finalResults = rowAggregator.finalize(deserPayload)
    expectedVals.zip(finalResults).zip(rowAggregator.outputSchema.map(_._1)).foreach {
      case ((expected, actual), name) =>
        println(s"Checking: $name")
        assertEquals(expected, actual)
    }
  }

  @Test
  def correctlyDeterminesTilingIsEnabled(): Unit = {
    def buildGroupByWithCustomJson(customJson: String = null): GroupBy =
      Builders.GroupBy(
        metaData = Builders.MetaData(name = "featureGroupName", customJson = customJson)
      )

    // customJson not set defaults to false
    assertFalse(TileCodec.isTilingEnabled(buildGroupByWithCustomJson()))
    assertFalse(TileCodec.isTilingEnabled(buildGroupByWithCustomJson("{}")))

    assertTrue(
      TileCodec
        .isTilingEnabled(buildGroupByWithCustomJson("{\"enable_tiling\": true}"))
    )

    assertFalse(
      TileCodec
        .isTilingEnabled(buildGroupByWithCustomJson("{\"enable_tiling\": false}"))
    )

    assertFalse(
      TileCodec
      .isTilingEnabled(
        buildGroupByWithCustomJson("{\"enable_tiling\": \"string instead of bool\"}")
      )
    )
  }
}
