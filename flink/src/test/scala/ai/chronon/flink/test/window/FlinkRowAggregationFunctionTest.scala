package ai.chronon.flink.test.window

import ai.chronon.api._
import ai.chronon.flink.window.FlinkRowAggregationFunction
import ai.chronon.online.TileCodec
import org.junit.Assert.fail
import org.junit.Test

import scala.util.{Failure, Try}

class FlinkRowAggregationFunctionTest {
  private val aggregations: Seq[Aggregation] = Seq(
    Builders.Aggregation(
      Operation.AVERAGE,
      "views",
      Seq(
        new Window(1, TimeUnit.DAYS),
        new Window(1, TimeUnit.HOURS),
        new Window(30, TimeUnit.DAYS)
      )
    ),
    Builders.Aggregation(
      Operation.AVERAGE,
      "rating",
      Seq(
        new Window(1, TimeUnit.DAYS),
        new Window(1, TimeUnit.HOURS)
      )
    ),
    Builders.Aggregation(
      Operation.MAX,
      "title",
      Seq(
        new Window(1, TimeUnit.DAYS)
      )
    ),
    Builders.Aggregation(
      Operation.LAST,
      "title",
      Seq(
        new Window(1, TimeUnit.DAYS)
      )
    )
  )

  private val schema = List(
    Constants.TimeColumn -> LongType,
    "views" -> IntType,
    "rating" -> FloatType,
    "title" -> StringType
  )

  @Test
  def testFlinkAggregatorProducesCorrectResults(): Unit = {
    val groupByMetadata = Builders.MetaData(name = "my_group_by")
    val groupBy = Builders.GroupBy(metaData = groupByMetadata, aggregations = aggregations)
    val aggregateFunc = new FlinkRowAggregationFunction(groupBy, schema)

    var acc = aggregateFunc.createAccumulator()
    val rows = Seq(
      createRow(1519862399984L, 4, 4.0f, "A"),
      createRow(1519862399984L, 40, 5.0f, "B"),
      createRow(1519862399988L, 3, 3.0f, "C"),
      createRow(1519862399988L, 5, 4.0f, "D"),
      createRow(1519862399994L, 4, 4.0f, "A"),
      createRow(1519862399999L, 10, 4.0f, "A")
    )
    rows.foreach(row => acc = aggregateFunc.add(row, acc))
    val result = aggregateFunc.getResult(acc)

    // we sanity check the final result of the accumulator
    // to do so, we must first expand / decompress the windowed tile IR into a full tile
    // then we can finalize the tile and get the final result
    val tileCodec = new TileCodec(groupBy, schema)
    val expandedIr = tileCodec.expandWindowedTileIr(result.ir)
    val finalResult = tileCodec.windowedRowAggregator.finalize(expandedIr)

    // expect 7 columns as we have 3 view avg time windows, 2 rating avg and 1 max title, 1 last title
    assert(finalResult.length == 7)
    val expectedAvgViews = 11.0f
    val expectedAvgRating = 4.0f
    val expectedMax = "D"
    val expectedLast = "A"
    val expectedResult = Array(
      expectedAvgViews,
      expectedAvgViews,
      expectedAvgViews,
      expectedAvgRating,
      expectedAvgRating,
      expectedMax,
      expectedLast
    )
    assert(finalResult sameElements expectedResult)
  }

  @Test
  def testFlinkAggregatorResultsCanBeMergedWithOtherPreAggregates(): Unit = {
    val groupByMetadata = Builders.MetaData(name = "my_group_by")
    val groupBy = Builders.GroupBy(metaData = groupByMetadata, aggregations = aggregations)
    val aggregateFunc = new FlinkRowAggregationFunction(groupBy, schema)

    // create partial aggregate 1
    var acc1 = aggregateFunc.createAccumulator()
    val rows1 = Seq(
      createRow(1519862399984L, 4, 4.0f, "A"),
      createRow(1519862399984L, 40, 5.0f, "B")
    )
    rows1.foreach(row => acc1 = aggregateFunc.add(row, acc1))
    val partialResult1 = aggregateFunc.getResult(acc1)

    // create partial aggregate 2
    var acc2 = aggregateFunc.createAccumulator()
    val rows2 = Seq(
      createRow(1519862399988L, 3, 3.0f, "C"),
      createRow(1519862399988L, 5, 4.0f, "D")
    )
    rows2.foreach(row => acc2 = aggregateFunc.add(row, acc2))
    val partialResult2 = aggregateFunc.getResult(acc2)

    // create partial aggregate 3
    var acc3 = aggregateFunc.createAccumulator()
    val rows3 = Seq(
      createRow(1519862399994L, 4, 4.0f, "A"),
      createRow(1519862399999L, 10, 4.0f, "A")
    )
    rows3.foreach(row => acc3 = aggregateFunc.add(row, acc3))
    val partialResult3 = aggregateFunc.getResult(acc3)

    // lets merge the partial results together and check
    val mergedPartialAggregates = aggregateFunc.rowAggregator
      .merge(
        aggregateFunc.rowAggregator.merge(partialResult1.ir, partialResult2.ir),
        partialResult3.ir
      )

    // we sanity check the final result of the accumulator
    // to do so, we must first expand / decompress the windowed tile IR into a full tile
    // then we can finalize the tile and get the final result
    val tileCodec = new TileCodec(groupBy, schema)
    val expandedIr = tileCodec.expandWindowedTileIr(mergedPartialAggregates)
    val finalResult = tileCodec.windowedRowAggregator.finalize(expandedIr)

    // expect 7 columns as we have 3 view avg time windows, 2 rating avg and 1 max title, 1 last title
    assert(finalResult.length == 7)
    val expectedAvgViews = 11.0f
    val expectedAvgRating = 4.0f
    val expectedMax = "D"
    val expectedLast = "A"
    val expectedResult = Array(
      expectedAvgViews,
      expectedAvgViews,
      expectedAvgViews,
      expectedAvgRating,
      expectedAvgRating,
      expectedMax,
      expectedLast
    )
    assert(finalResult sameElements expectedResult)
  }

  @Test
  def testFlinkAggregatorProducesCorrectResultsIfInputIsInIncorrectOrder(): Unit = {
    val groupByMetadata = Builders.MetaData(name = "my_group_by")
    val groupBy = Builders.GroupBy(metaData = groupByMetadata, aggregations = aggregations)
    val aggregateFunc = new FlinkRowAggregationFunction(groupBy, schema)

    var acc = aggregateFunc.createAccumulator()

    // Create a map where the entries are not in the same order as `schema`.
    val outOfOrderRow = Map[String, Any](
      "rating" -> 4.0f,
      Constants.TimeColumn -> 1519862399999L,
      "title" -> "A",
      "views" -> 10
    )

    // If the aggregator fails to fix the order, we'll get a ClassCastException
    Try {
      acc = aggregateFunc.add(outOfOrderRow, acc)
    } match {
      case Failure(e) => {
        fail(
          s"An exception was thrown by the aggregator when it should not have been. " +
            s"The aggregator should fix the order without failing. $e")
      }
      case _ =>
    }

    val result = aggregateFunc.getResult(acc)

    // we sanity check the final result of the accumulator
    // to do so, we must first expand / decompress the windowed tile IR into a full tile
    // then we can finalize the tile and get the final result
    val tileCodec = new TileCodec(groupBy, schema)
    val expandedIr = tileCodec.expandWindowedTileIr(result.ir)
    val finalResult = tileCodec.windowedRowAggregator.finalize(expandedIr)
    assert(finalResult.length == 7)

    val expectedResult = Array(
      outOfOrderRow("views"),
      outOfOrderRow("views"),
      outOfOrderRow("views"),
      outOfOrderRow("rating"),
      outOfOrderRow("rating"),
      outOfOrderRow("title"),
      outOfOrderRow("title")
    )
    assert(finalResult sameElements expectedResult)
  }

  def createRow(ts: Long, views: Int, rating: Float, title: String): Map[String, Any] =
    Map(
      Constants.TimeColumn -> ts,
      "views" -> views,
      "rating" -> rating,
      "title" -> title
    )
}
