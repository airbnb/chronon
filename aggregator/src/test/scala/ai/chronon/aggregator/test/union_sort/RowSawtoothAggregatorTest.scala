package ai.chronon.aggregator.test.union_sort

import ai.chronon.aggregator.base.Sum
import ai.chronon.aggregator.row.RowAggregator
import ai.chronon.aggregator.test.TestRow
import ai.chronon.aggregator.windowing.union_sort.WindowAlignedRowSawtoothAggregator
import ai.chronon.api.Extensions.{AggregationOps, AggregationsOps, WindowOps, WindowUtils}
import ai.chronon.api._
import junit.framework.TestCase
import org.junit.Assert._

import java.util
import scala.collection.JavaConverters._

class RowSawtoothAggregatorTest extends TestCase {
  def naiveSawtooth(inputs: List[Row], queries: List[Long], windowLength: Long, hopLength: Long, rowAgg: RowAggregator): List[(Long, Array[Any])] =
    queries.map { queryTs: Long =>
      val hopTs = queryTs / hopLength * hopLength
      val sawtoothSumIr = inputs.filter { row =>
        row.ts >= (hopTs - windowLength) && row.ts <= queryTs
      }
        .sortBy(_.ts)
        .foldLeft(rowAgg.init) { (ir, row) =>
          rowAgg.updateWithReturn(ir, row)
        }
      (queryTs, rowAgg.finalize(sawtoothSumIr))
    }

  def testWindowAlignedRowSawtooth(): Unit = {
    val millisPerMin = 60L * 1000L

    val rows = List(
      TestRow(1L * millisPerMin, 4, 5.0f, "A"),
      TestRow(60L * millisPerMin, 3, 4.0f, "B"),
      TestRow(70L * millisPerMin, 5, 7.0f, "D"),
      TestRow(120L * millisPerMin, 7, 1.0f, "A"),
      TestRow(125L * millisPerMin, 3, 1.0f, "B"),
      TestRow(150L * millisPerMin, 2, 1.0f, "B")
    )

    val schema = List(
      "ts" -> LongType,
      "views" -> IntType,
      "rating" -> FloatType,
      "title" -> StringType,
    )

    val specs = List(
      Builders.AggregationPart(Operation.AVERAGE, "views", WindowUtils.Hour),
      Builders.AggregationPart(Operation.SUM, "rating", WindowUtils.Hour),
      Builders.AggregationPart(Operation.LAST_K, "title", WindowUtils.Hour, argMap = Map("k" -> "2")),
      Builders.AggregationPart(Operation.APPROX_UNIQUE_COUNT, "title", WindowUtils.Hour),
      Builders.AggregationPart(Operation.TOP_K, "title", WindowUtils.Hour, argMap = Map("k" -> "2"))
    )

    val rowSawtoothAgg = new WindowAlignedRowSawtoothAggregator(schema, specs)
    val expected = naiveSawtooth(rows, rows.map(_.ts), rowSawtoothAgg.window.millis, rowSawtoothAgg.hopLength, rowSawtoothAgg.internalRowAggregator)
    val actual = rows.map { row =>
      rowSawtoothAgg.update(row)
      (row.ts, rowSawtoothAgg.query(row.ts))
    }

    expected.zip(actual).foreach {
      case (expected, actual) =>
        val expectedList = (expected._1, expected._2.toList)
        val actualList = (actual._1, actual._2.toList)
        assert(expectedList == actualList, s"Mismatched: $expectedList, $actualList")
    }
  }

  // TODO: add test on generated data, either using scalacheck Gen or Chronon's DataGen utility
}
