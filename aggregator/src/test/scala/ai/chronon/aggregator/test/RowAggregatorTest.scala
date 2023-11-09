package ai.chronon.aggregator.test

import ai.chronon.aggregator.row.RowAggregator
import ai.chronon.api._
import junit.framework.TestCase
import org.junit.Assert._

import java.util
import scala.collection.JavaConverters._
import scala.collection.mutable

class TestRow(val fieldsSeq: Any*)(tsIndex: Int = 0) extends Row {
  val fields: util.List[Any] = new java.util.ArrayList[Any](fieldsSeq.asJava)
  override val length: Int = fields.size()

  override def get(index: Int): Any = fields.get(index)

  // just a convention for testing
  lazy val timeStamp: Long = getAs[Long](tsIndex)

  override def ts: Long = timeStamp

  override def isBefore: Boolean = false

  override def mutationTs: Long = timeStamp

  def print(): Unit = println(fieldsSeq)

  def set(index: Int, any: Any): Unit = fields.set(index, any)
}

object TestRow {
  def apply(inputsArray: Any*): TestRow = new TestRow(inputsArray: _*)()
}

class RowAggregatorTest extends TestCase {

  var rowAggregator: RowAggregator = _
  var rows: List[TestRow] = _
  var rowsToDelete: List[TestRow] = _
  var expectedVals: Array[Any] = _
  var specs: Seq[AggregationPart] = _

  override def setUp(): Unit = {
    rows = List(
      TestRow(1L, 4, 5.0f, "A", Seq(5, 3, 4), Seq("D", "A", "B", "A"), Map("A" -> 1, "B" -> 2)),
      TestRow(2L, 3, 4.0f, "B", Seq(6, null), Seq(), null),
      TestRow(3L, 5, 7.0f, "D", null, null, Map("A" -> null, "B" -> 1)),
      TestRow(4L, 7, 1.0f, "A", Seq(), Seq("B", "A", "D"), Map("A" -> 5, "B" -> 1, (null, 2))),
      TestRow(5L, 3, 1.0f, "B", Seq(null), Seq("A", "B", "C"), Map.empty[String, Int])
    )

    rowsToDelete = List(
      TestRow(4L, 2, 1.0f, "A", Seq(1, null), Seq("B", "C", "D", "H"), Map("B" -> 1, "A" -> 3)),
      TestRow(5L, 1, 2.0f, "H", Seq(1), Seq(), Map("B" -> 2, "D" -> 3))
    )

    val schema = List(
      "ts" -> LongType,
      "views" -> IntType,
      "rating" -> FloatType,
      "title" -> StringType,
      "session_lengths" -> ListType(IntType),
      "hist_input" -> ListType(StringType),
      "hist_map" -> MapType(StringType, IntType)
    )

    val sessionLengthAvgByTitle = new java.util.HashMap[String, Double]()
    sessionLengthAvgByTitle.put("A", 5.5)
    sessionLengthAvgByTitle.put("B", 6)
    sessionLengthAvgByTitle.put("H", 1) // 0-1 / 0-1

    val histogram = new java.util.HashMap[String, Int]()
    histogram.put("A", 4)
    histogram.put("B", 2)

    val mapAvg = new java.util.HashMap[String, Double]()
    mapAvg.put("A", 3.0)
    mapAvg.put("B", 1.0)
    mapAvg.put("D", 3.0)  // sum = -3 / count = -1
    mapAvg.put(null, 2.0)

    val specsAndExpected: Array[(AggregationPart, Any)] = Array(
      Builders.AggregationPart(Operation.AVERAGE, "views") -> 19.0 / 3,
      Builders.AggregationPart(Operation.COUNT, "views") -> 3L,
      Builders.AggregationPart(Operation.SUM, "rating") -> 15.0,
      Builders.AggregationPart(Operation.LAST, "title") -> "B",
      Builders.AggregationPart(Operation.LAST_K, "title", argMap = Map("k" -> "2")) -> List("B", "A").asJava,
      Builders.AggregationPart(Operation.FIRST_K, "title", argMap = Map("k" -> "2")) -> List("A", "B").asJava,
      Builders.AggregationPart(Operation.FIRST, "title") -> "A",
      Builders.AggregationPart(Operation.TOP_K, "title", argMap = Map("k" -> "2")) -> List("D", "B").asJava,
      Builders.AggregationPart(Operation.BOTTOM_K, "title", argMap = Map("k" -> "2")) -> List("A", "A").asJava,
      Builders.AggregationPart(Operation.MAX, "title") -> "D",
      Builders.AggregationPart(Operation.MIN, "title") -> "A",
      Builders.AggregationPart(Operation.APPROX_UNIQUE_COUNT, "title") -> 3L,
      Builders.AggregationPart(Operation.APPROX_UNIQUE_COUNT, "title", argMap = Map("k" -> "10")) -> 3L,
      Builders.AggregationPart(Operation.UNIQUE_COUNT, "title") -> 3L,
      Builders.AggregationPart(Operation.AVERAGE, "session_lengths") -> 8.0,
      Builders.AggregationPart(Operation.AVERAGE, "session_lengths", bucket = "title") -> sessionLengthAvgByTitle,
      Builders.AggregationPart(Operation.HISTOGRAM, "hist_input", argMap = Map("k" -> "2")) -> histogram,
      Builders.AggregationPart(Operation.AVERAGE, "hist_map") -> mapAvg
    )

    val unzippedSpecsAndExpected = specsAndExpected.unzip
    specs = unzippedSpecsAndExpected._1
    expectedVals = unzippedSpecsAndExpected._2

    rowAggregator = new RowAggregator(schema, specs)
  }

  def testUpdateWithMerge(): Unit = {
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

    expectedVals.zip(finalized).zip(rowAggregator.outputSchema.map(_._1)).foreach {
      case ((expected, actual), name) => assertEquals(expected, actual)
    }
  }

  def testUpdateWithBulkMerge(): Unit = {
    val (firstRows, remainingRows) = rows.splitAt(1)
    val (secondRows, thirdRows) = remainingRows.splitAt(2)

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

    val thirdResult = thirdRows.foldLeft(rowAggregator.init) {
      case (merged, input) =>
        rowAggregator.update(merged, input)
        merged
    }

    rowAggregator.bulkMerge(mutable.ArrayBuffer(firstResult, secondResult, thirdResult))

    val forDeletion = firstResult.clone()

    rowsToDelete.foldLeft(forDeletion) {
      case (ir, inp) =>
        rowAggregator.delete(ir, inp)
        ir
    }
    val finalized = rowAggregator.finalize(forDeletion)

    expectedVals.zip(finalized).zip(rowAggregator.outputSchema.map(_._1)).foreach {
      case ((expected, actual), name) => assertEquals(expected, actual)
    }
  }
}
