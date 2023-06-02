package ai.chronon.aggregator.test

import java.util

import ai.chronon.aggregator.base.TopK
import ai.chronon.aggregator.test.SawtoothAggregatorTest.sawtoothAggregate
import ai.chronon.aggregator.windowing.{FiveMinuteResolution, SawtoothAggregator, TwoStackLiteAggregationBuffer, TwoStackLiteAggregator, TwoStackLiteHopAggregator}
import ai.chronon.api.{Aggregation, Builders, IntType, LongType, Operation, StructField, StructType, TimeUnit, Window}
import junit.framework.TestCase
import org.junit.Assert._
import com.google.gson.Gson
import scala.collection.Seq

import ai.chronon.aggregator.test.TwoStackLiteAggregatorTest.{benchmarkTesting, multiRunTest, testBuffer}
import ai.chronon.api.Extensions.AggregationOps

class TwoStackLiteAggregatorTest extends TestCase {
  def testBufferWithTopK(): Unit = {
    val topK = new TopK[Int](IntType, 2)

    val twoStackBuffer = new TwoStackLiteAggregationBuffer(topK, 5)
    testBuffer(twoStackBuffer)
  }

  def testAgainstSawtoothWithAvg(): Unit = {
    val aggregations: Seq[Aggregation] = Seq(
      Builders.Aggregation(
        Operation.AVERAGE,
        "num",
        Seq(new Window(1, TimeUnit.DAYS), new Window(1, TimeUnit.HOURS), new Window(30, TimeUnit.DAYS))),
      Builders.Aggregation(Operation.AVERAGE, "num")
    )

    val queryCount = 100000
    val eventCount = 1000000
    benchmarkTesting(aggregations, queryCount, eventCount, runNaive = false)
    println("\n >>>>>> warm up done. <<<<<< \n\n")
    benchmarkTesting(aggregations, queryCount, eventCount, runNaive = false)
    println("\n >>>>>> final run. <<<<<< \n\n")
    benchmarkTesting(aggregations, queryCount, eventCount, runNaive = false)
  }

  def testAgainstSawtoothWithAvgWithIdependentData(): Unit = {
    val aggregations: Seq[Aggregation] = Seq(
      Builders.Aggregation(
        Operation.AVERAGE,
        "num",
        Seq(new Window(1, TimeUnit.DAYS), new Window(1, TimeUnit.HOURS), new Window(30, TimeUnit
          .DAYS))),
      Builders.Aggregation(Operation.AVERAGE, "num")
    )

    val queryCount = 10000
    val eventCount = 100000
    multiRunTest(aggregations, queryCount, eventCount)

    println("\n >>>>>> warm up done. <<<<<< \n\n")

    multiRunTest(aggregations, queryCount, eventCount)

    println("\n >>>>>> final run. <<<<<< \n\n")

    multiRunTest(aggregations, queryCount, eventCount)
  }

  def testAgainstSawtoothWithTopK(): Unit = {
    val aggregations: Seq[Aggregation] = Seq(
      Builders.Aggregation(
        Operation.TOP_K,
        "num",
        Seq(new Window(1, TimeUnit.DAYS), new Window(1, TimeUnit.HOURS), new Window(30, TimeUnit.DAYS)),
        argMap = Map("k" -> "300")),
      Builders.Aggregation(Operation.TOP_K, "num", argMap = Map("k" -> "300"))
    )
    val queryCount = 100000
    val eventCount = 1000000
    benchmarkTesting(aggregations, queryCount, eventCount, runNaive = false)
    println("\n >>>>>> warm up done. <<<<<< \n\n")
    benchmarkTesting(aggregations, queryCount, eventCount, runNaive = false)
    println("\n >>>>>> final run. <<<<<< \n\n")
    benchmarkTesting(aggregations, queryCount, eventCount, runNaive = false)
  }

  def testAgainstSawtoothWithTopKWithIdependentData(): Unit = {
    val aggregations: Seq[Aggregation] = Seq(
      Builders.Aggregation(
        Operation.TOP_K,
        "num",
        Seq(new Window(1, TimeUnit.DAYS), new Window(1, TimeUnit.HOURS), new Window(30, TimeUnit
          .DAYS)),
        argMap = Map("k" -> "300")),
      Builders.Aggregation(Operation.TOP_K, "num", argMap = Map("k" -> "300"))
    )
    multiRunTest(aggregations, 100000, 1000000)

    println("\n >>>>>> warm up done. <<<<<< \n")

    multiRunTest(aggregations, 10000, 100000)
  }
}

object TwoStackLiteAggregatorTest {

  def testBuffer(buffer: TwoStackLiteAggregationBuffer[Int, util.ArrayList[Int], util.ArrayList[Int]]): Unit = {
    assertEquals(null, buffer.query) // null
    Seq(7, 8, 9).map(x => new Integer(x)).foreach(i => buffer.push(i, i * 1000))

    def assertBufferEquals(a: Seq[Int], b: util.ArrayList[Int]): Unit = {
      if (a == null || b == null) {
        assertEquals(a, b)
      } else {
        assertArrayEquals(Option(a).map(_.map(x => new Integer(x).asInstanceOf[AnyRef]).toArray).orNull,
          Option(b).map(_.toArray).orNull)
      }
    }

    assertBufferEquals(Seq(8, 9), buffer.query)
    buffer.pop()
    assertBufferEquals(Seq(8, 9), buffer.query)
    buffer.pop()
    assertBufferEquals(Seq(9), buffer.query)
    buffer.pop()
    assertBufferEquals(null, buffer.query)
    buffer.push(new Integer(10), 10 * 1000)
    assertBufferEquals(Seq(10), buffer.query)
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

    def runSawtoothTwoStack(): Array[Array[Any]] = {
      val sawtoothAggregatorTwoStack =
        new SawtoothAggregator(aggregations, schema, FiveMinuteResolution, useTwoStack = true)
      sawtoothAggregate(events, queries, aggregations, schema, useTwoStack = true)
        .map(sawtoothAggregatorTwoStack.windowedAggregator.finalize)
    }

    val sawtoothIrs = runSawtooth()
    timer.publish("sawtooth")

    val sawtoothTwoStackIrs = runSawtoothTwoStack()
    timer.publish("sawtooth two stack")

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

    val twoStackLiteHopAggregator = new TwoStackLiteHopAggregator(
      StructType("", columns.map(c => StructField(c.name, c.`type`)).toArray),
      aggregations)

    def runTwoStackHop(): Array[Array[Any]] = {
      // will finalize by default
      twoStackLiteHopAggregator.slidingSawtoothWindow(sortedQueries.iterator, sortedEvents.iterator).toArray
    }

    val twoStackHopIrs = runTwoStackHop() // for easier profiling
    timer.publish("two stack hop lite")

    val twoStackAggregator = new TwoStackLiteAggregator(
      StructType("", columns.map(c => StructField(c.name, c.`type`)).toArray),
      aggregations)

    def runTwoStack(): Array[Array[Any]] = {
      // will finalize by default
      twoStackAggregator.slidingSawtoothWindow(sortedQueries.iterator, sortedEvents.iterator).toArray
    }

    val twoStackIrs = runTwoStack() // for easier profiling
    timer.publish("two stack lite")

    val gson = new Gson()

    sawtoothTwoStackIrs.zip(sawtoothIrs).foreach {
      case (daba, sawtooth) =>
        assertEquals(gson.toJson(sawtooth), gson.toJson(daba))
    }

    twoStackHopIrs.zip(sawtoothIrs).foreach {
      case (daba, sawtooth) =>
        assertEquals(gson.toJson(sawtooth), gson.toJson(daba))
    }

    twoStackIrs.zip(sawtoothIrs).foreach {
      case (twoStack, sawtooth) =>
        assertEquals(gson.toJson(sawtooth), gson.toJson(twoStack))
    }
  }

  def multiRunTest(aggregations: Seq[Aggregation], queryCount: Int, eventCount: Int): Unit = {
    genAndRun(queryCount, eventCount, (queries, events, cols, timer) => {
      val schema = cols.map(_.schema)
      val sawtoothAggregator =
        new SawtoothAggregator(aggregations, schema, FiveMinuteResolution)

      sawtoothAggregate(events, queries, aggregations, schema)
        .map(sawtoothAggregator.windowedAggregator.finalize)

      timer.publish(">>>> sawtooth")
    })

    println()

    genAndRun(queryCount, eventCount, (queries, events, cols, timer) => {
      val schema = cols.map(_.schema)

      val sawtoothAggregatorTwoStack =
        new SawtoothAggregator(aggregations, schema, FiveMinuteResolution, useTwoStack = true)
      sawtoothAggregate(events, queries, aggregations, schema, useTwoStack = true)
        .map(sawtoothAggregatorTwoStack.windowedAggregator.finalize)

      timer.publish(">>>> sawtooth two stack")
    })

    println()

    genAndRun(queryCount, eventCount, (queries, events, cols, timer) => {
      val twoStackLiteHopAggregator = new TwoStackLiteHopAggregator(
        StructType("", cols.map(c => StructField(c.name, c.`type`)).toArray),
        aggregations)

      val sortedQueries = queries.sorted
      val sortedEvents = events.sortBy(_.ts)

      timer.publish("sorting")

      twoStackLiteHopAggregator
        .slidingSawtoothWindow(sortedQueries.iterator, sortedEvents.iterator).toArray

      timer.publish(">>>> two stack hop")
    })

    println()

    genAndRun(queryCount, eventCount, (queries, events, cols, timer) => {
      val twoStackAggregator = new TwoStackLiteAggregator(
        StructType("", cols.map(c => StructField(c.name, c.`type`)).toArray),
        aggregations)

      val sortedQueries = queries.sorted
      val sortedEvents = events.sortBy(_.ts)

      timer.publish("sorting")

      twoStackAggregator.slidingSawtoothWindow(sortedQueries.iterator, sortedEvents.iterator).toArray

      timer.publish(">>>> two stack")
    })
  }

  def genAndRun(
      queryCount: Int,
      eventCount: Int,
      runner: (Array[Long], Array[TestRow], Seq[Column], Timer) => Unit): Unit = {
    val timer = new Timer

    val queries = CStream.genTimestamps(new Window(30, TimeUnit.DAYS), queryCount, 5 * 60 * 1000)

    val columns = Seq(Column("ts", LongType, 180), Column("num", LongType, 1000))
    val events = CStream.gen(columns, eventCount).rows

    timer.publish("setup")

    runner(queries, events, columns, timer)
  }
}