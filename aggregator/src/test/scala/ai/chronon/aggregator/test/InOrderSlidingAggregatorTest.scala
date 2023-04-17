package ai.chronon.aggregator.test

import java.util

import ai.chronon.aggregator.base.{SimpleAggregator, TopK}
import ai.chronon.aggregator.test.SawtoothAggregatorTest.sawtoothAggregate
import ai.chronon.aggregator.windowing.{
  DABALiteBuffer,
  FiveMinuteResolution,
  InOrderAggregationBuffer,
  InOrderSlidingAggregationBuffer,
  InOrderSlidingAggregator,
  SawtoothAggregator,
  TwoStackLiteBuffer
}
import ai.chronon.api.{
  Aggregation,
  Builders,
  IntType,
  LongType,
  Operation,
  Row,
  StructField,
  StructType,
  TimeUnit,
  Window
}
import junit.framework.TestCase
import org.junit.Assert._
import com.google.gson.Gson
import scala.collection.Seq

import ai.chronon.aggregator.test.InOrderSlidingAggregatorTest.{benchmarkTesting, testBuffer}
import ai.chronon.api.Extensions.AggregationOps

class InOrderSlidingAggregatorTest extends TestCase {
  def testBufferWithTopK(): Unit = {
    val topK = new TopK[Int](IntType, 2)

    val twoStackBuffer = new TwoStackLiteBuffer(topK)
    testBuffer(twoStackBuffer)

    val dabaBuffer = new DABALiteBuffer(topK)
    testBuffer(dabaBuffer)
  }

  def testAgainstSawtoothWithAvg(): Unit = {
    /* Rough timings below will vary by processor (results are measured using M1 MAX mac pro)
    | aggregation |                            avg                                  |
    | events      |       10k         |        100k         |         1m            |
    | query       | 1k  | 10k  | 100k | 1k   | 10k   | 100k | 1k    | 10k   | 100k  |
    |-------------|-----|------|------|------|-------|------|-------|-------|-------|
    | sorting     | 9   | 10   | 10   | 93   | 82    | 86   | 746   | 740   | 709   |
    | sawtooth    | 160 | 182  | 221  | 250  | 263   | 284  | 407   | 499   | 520   |
    | naive       | 369 | 5872 | N/A  | 3214 | 28351 | N/A  | 28971 | N/A   | N/A   |
    | daba        | 23  | 28   | 65   | 63   | 86    | 121  | 365   | 386   | 398   |
    | two stack   | 6   | 11   | 72   | 76   | 66    | 156  | 393   | 433   | 639   |
     */
    val aggregations: Seq[Aggregation] = Seq(
      Builders.Aggregation(
        Operation.AVERAGE,
        "num",
        Seq(new Window(1, TimeUnit.DAYS), new Window(1, TimeUnit.HOURS), new Window(30, TimeUnit.DAYS))),
      Builders.Aggregation(Operation.AVERAGE, "num")
    )
    benchmarkTesting(aggregations, 1000, 10000, runNaive = false)
  }

  def testAgainstSawtoothWithTopK(): Unit = {
    /* Rough timings below will vary by processor (results are measured using M1 MAX mac pro)
    | aggregation |                           top 300                               |
    | events      |       10k         |        100k         |         1m            |
    | query       | 1k  | 10k  | 100k | 1k   | 10k   | 100k | 1k    | 10k   | 100k  |
    |-------------|-----|------|------|------|-------|------|-------|-------|-------|
    | sorting     | 10  | 10   | 12   | 89   | 71    | 85   | 787   | 710   | 746   |
    | sawtooth    | 198 | 417  | 726  | 333  | 589   | 979  | 621   | 879   | 1430  |
    | naive       | 470 | 7531 | N/A  | N/A  | N/A   | N/A  | N/A   | N/A   | N/A   |
    | daba        | 240 | 564  | 4310 | 1832 | 2600  | 7806 | 14764 | 16024 | 22397 |
    | two stack   | 64  | 398  | 4462 | 229  | 788   | 7107 | 1534  | 2185  | 9288  |
     */
    val aggregations: Seq[Aggregation] = Seq(
      Builders.Aggregation(
        Operation.TOP_K,
        "num",
        Seq(new Window(1, TimeUnit.DAYS), new Window(1, TimeUnit.HOURS), new Window(30, TimeUnit.DAYS)),
        argMap = Map("k" -> "300")),
      Builders.Aggregation(Operation.TOP_K, "num", argMap = Map("k" -> "300"))
    )
    benchmarkTesting(aggregations, 10000, 100000, runNaive = false)
  }
}

object InOrderSlidingAggregatorTest {

  def testBuffer(buffer: InOrderAggregationBuffer[Int, util.ArrayList[Int], util.ArrayList[Int]]): Unit = {
    assertEquals(null, buffer.query()) // null
    Seq(7, 8, 9).map(x => new Integer(x)).foreach(i => buffer.push(i, i * 1000))

    def assertBufferEquals(a: Seq[Int], b: util.ArrayList[Int]): Unit = {
      if (a == null || b == null) {
        assertEquals(a, b)
      } else {
        assertArrayEquals(Option(a).map(_.map(x => new Integer(x).asInstanceOf[AnyRef]).toArray).orNull,
          Option(b).map(_.toArray).orNull)
      }
    }

    assertBufferEquals(Seq(8, 9), buffer.query())
    buffer.pop()
    assertBufferEquals(Seq(8, 9), buffer.query())
    buffer.pop()
    assertBufferEquals(Seq(9), buffer.query())
    buffer.pop()
    assertBufferEquals(null, buffer.query())
    buffer.push(new Integer(10), 10 * 1000)
    assertBufferEquals(Seq(10), buffer.query())
  }

  def benchmarkTesting(aggregations: Seq[Aggregation], queryCount: Int, eventCount: Int, runNaive: Boolean): Unit = {
    val timer = new Timer
    val queries = CStream.genTimestamps(new Window(30, TimeUnit.DAYS), queryCount, 5 * 60 * 1000)

    val columns = Seq(Column("ts", LongType, 180), Column("num", LongType, 1000))
    val events = CStream.gen(columns, eventCount).rows
    val schema = columns.map(_.schema)

    timer.publish("setup")

    val sortedQueries = queries.sorted
    val sortedEvents = events.sortBy(_.ts)

    timer.publish("sorting")

    val sawtoothAggregator =
      new SawtoothAggregator(aggregations, schema, FiveMinuteResolution)

    def runSawtooth(): Array[Array[Any]] = {
      sawtoothAggregate(events, queries, aggregations, schema)
        .map(sawtoothAggregator.windowedAggregator.finalize)
    }

    val sawtoothIrs = runSawtooth()
    timer.publish("sawtooth")

    if (runNaive) {
      val windows = aggregations.flatMap(_.unpack.map(_.window)).toArray
      val tailHops = windows.map(FiveMinuteResolution.calculateTailHop)
      val naiveAggregator = new NaiveAggregator(
        sawtoothAggregator.windowedAggregator,
        windows,
        tailHops
      )
      naiveAggregator.aggregate(events, queries).map(sawtoothAggregator.windowedAggregator.finalize)
      timer.publish("naive")
    }

    val dabaAggregator = new InOrderSlidingAggregator(
      StructType("", columns.map(c => StructField(c.name, c.`type`)).toArray),
      aggregations,
      bufferFactory = InOrderSlidingAggregationBuffer.createDABALiteBuffer)

    def runDABA(): Array[Array[Any]] = {
      // will finalize by default
      dabaAggregator.slidingSawtoothWindow(sortedQueries.iterator, sortedEvents.iterator).toArray
    }

    val dabaIrs = runDABA() // for easier profiling
    timer.publish("daba lite")

    val twoStackAggregator = new InOrderSlidingAggregator(
      StructType("", columns.map(c => StructField(c.name, c.`type`)).toArray),
      aggregations,
      bufferFactory = InOrderSlidingAggregationBuffer.createTwoStackLiteBuffer)

    def runTwoStack(): Array[Array[Any]] = {
      // will finalize by default
      twoStackAggregator.slidingSawtoothWindow(sortedQueries.iterator, sortedEvents.iterator).toArray
    }

    val twoStackIrs = runTwoStack() // for easier profiling
    timer.publish("two stack lite")

    val gson = new Gson()
    dabaIrs.zip(sawtoothIrs).foreach {
      case (daba, sawtooth) =>
        assertEquals(gson.toJson(sawtooth), gson.toJson(daba))
    }

    twoStackIrs.zip(sawtoothIrs).foreach {
      case (twoStack, sawtooth) =>
        assertEquals(gson.toJson(sawtooth), gson.toJson(twoStack))
    }
  }
}