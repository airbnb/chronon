package ai.zipline.aggregator.test

import java.util

import ai.zipline.aggregator.base.{FloatType, IntType, LongType, StringType}
import ai.zipline.aggregator.row.{Row, RowAggregator}
import ai.zipline.api.Config.{AggregationPart, Operation}
import junit.framework.TestCase

import scala.collection.JavaConverters._

class TestRow(fieldsSeq: Any*) extends Row {
  val fields: util.List[Any] = new java.util.ArrayList[Any](fieldsSeq.asJava)
  override val length: Int = fields.size()

  override def get(index: Int): Any = fields.get(index)

  // just a convention for testing
  lazy val timeStamp: Long = getAs[Long](0)

  override def ts: Long = timeStamp

  def print(): Unit = println(fieldsSeq)
}

object TestRow {
  def apply(inputsArray: Any*): TestRow = new TestRow(inputsArray: _*)
}

class RowAggregatorTest extends TestCase {
  def testUpdate(): Unit = {
    val rows = List(
      TestRow(1L, 4, 5.0f, "A"),
      TestRow(2L, 3, 4.0f, "B"),
      TestRow(3L, 5, 7.0f, "D"),
      TestRow(4L, 7, 1.0f, "A"),
      TestRow(5L, 3, 1.0f, "B")
    )

    val rowsToDelete = List(
      TestRow(4L, 2, 1.0f, "A"),
      TestRow(5L, 1, 2.0f, "H")
    )

    val schema = List(
      "ts" -> LongType,
      "views" -> IntType,
      "rating" -> FloatType,
      "title" -> StringType
    )

    val specsAndExpected: Array[(AggregationPart, Any)] = Array(
      AggregationPart(Operation.Average, "views") -> 19.0 / 3,
      AggregationPart(Operation.Sum, "rating") -> 15.0,
      AggregationPart(Operation.Last, "title") -> "B",
      AggregationPart(Operation.LastK, "title", args = Map("k" -> "2")) -> List("B", "A").asJava,
      AggregationPart(Operation.FirstK, "title", args = Map("k" -> "2")) -> List("A", "B").asJava,
      AggregationPart(Operation.First, "title") -> "A",
      AggregationPart(Operation.TopK, "title", args = Map("k" -> "2")) -> List("D", "B").asJava,
      AggregationPart(Operation.BottomK, "title", args = Map("k" -> "2")) -> List("A", "A").asJava,
      AggregationPart(Operation.Max, "title") -> "D",
      AggregationPart(Operation.Min, "title") -> "A",
      AggregationPart(Operation.ApproxDistinctCount, "title") -> 3L
    )

    val (specs, expectedVals) = specsAndExpected.unzip

    val rowAggregator = new RowAggregator(schema, specs)

    val (firstRows, secondRows) = rows.splitAt(3)

    val firstResult = firstRows.foldLeft(rowAggregator.init) {
      case (merged, input) =>
        rowAggregator.update(merged, input)
        merged
    }

    val secondResult = secondRows.foldLeft(rowAggregator.init) {
      case (merged, input) =>
        rowAggregator.update(merged, input)
        merged
    }

    rowAggregator.merge(firstResult, secondResult)

    val forDeletion = firstResult.clone()

    rowsToDelete.foldLeft(forDeletion) {
      case (ir, inp) =>
        rowAggregator.delete(ir, inp)
        ir
    }
    val finalized = rowAggregator.finalize(forDeletion)

    assert(expectedVals.zip(finalized).forall {
      case (expected: Any, actual: Any) => expected == actual
    })
  }
}
