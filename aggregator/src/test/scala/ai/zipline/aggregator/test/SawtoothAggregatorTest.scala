package ai.zipline.aggregator.test

import java.util

import ai.zipline.api.Config.{Aggregation, Operation, TimeUnit, Window}
import ai.zipline.aggregator.base.{DataType, LongType}
import ai.zipline.aggregator.row.RowAggregator
import ai.zipline.aggregator.test.TestDataUtils.{genNums, genTimestamps}
import ai.zipline.aggregator.windowing._
import com.google.gson.Gson
import junit.framework.TestCase
import org.junit.Assert._

import scala.collection.mutable

class Timer {

  var ts: Long = System.currentTimeMillis()

  def start(): Unit = { ts = System.currentTimeMillis() }

  // TODO: Write this out into a file checked into git
  // or incorporate proper benchmarks
  def publish(name: String, reset: Boolean = true): Unit = {
    println(s"${name.padTo(25, ' ')} ${System.currentTimeMillis() - ts} ms")
    if (reset) ts = System.currentTimeMillis()
  }
}

object TestDataUtils {
  def genNums(min: Long, max: Long, count: Int): Array[Long] = {
    val result = new Array[Long](count)
    var i = 0
    while (i < count) {
      val candidate: Long = min + (Math.random() * (max - min)).toLong
      result.update(i, candidate)
      i += 1
    }
    result
  }

  def genTimestamps(
      roundMillis: Long,
      count: Int,
      timeWindow: Window
  ): Array[Long] = {
    val end = System.currentTimeMillis()
    val start = end - timeWindow.millis
    genNums(start, end, count).map { i => (i / roundMillis) * roundMillis }
  }
}

class SawtoothAggregatorTest extends TestCase {

  def testTailAccuracy(): Unit = {
    val timer = new Timer
    val queries =
      genTimestamps(5 * 60 * 1000, 1000, Window(30, TimeUnit.Days)).sorted
    val events = {
      val eventCount = 100000
      val eventTimes = genTimestamps(1, eventCount, Window(180, TimeUnit.Days))
      // max is 1M to avoid overflow when summing
      val eventValues = genNums(0, 1000, eventCount)
      eventTimes.zip(eventValues).map {
        case (time, value) => TestRow(time, value)
      }
    }

    val schema = List(
      "ts" -> LongType,
      "num" -> LongType
    )

    val aggregations: Seq[Aggregation] = Seq(
      Aggregation(Operation.Average,
                  "num",
                  Seq(Window(1, TimeUnit.Days), Window(1, TimeUnit.Hours), Window(30, TimeUnit.Days))))
    timer.publish("setup")

    val sawtoothAggregator =
      new SawtoothAggregator(aggregations, schema, FiveMinuteResolution)
    val hopsAggregator = new HopsAggregator(
      queries.min,
      aggregations,
      schema,
      FiveMinuteResolution
    )
    var hops1 = hopsAggregator.init()
    var hops2 = hopsAggregator.init()
    var hopsAll = hopsAggregator.init()

    for (i <- 0 until events.length / 2) {
      hops1 = hopsAggregator.update(hops1, events(i))
      hopsAll = hopsAggregator.update(hopsAll, events(i))
    }

    for (i <- events.length / 2 until events.length) {
      hops2 = hopsAggregator.update(hops2, events(i))
      hopsAll = hopsAggregator.update(hopsAll, events(i))
    }
    timer.publish("hops/Update")

    val hopsMerged = hopsAggregator.merge(hops1, hops2)
    val mergedHops = hopsAggregator.toTimeSortedArray(hopsMerged)
    val rawHops = hopsAggregator.toTimeSortedArray(hopsAll)
    timer.publish("hops/Sort")

    val gson = new Gson
    val mergedStr = gson.toJson(mergedHops)
    val rawStr = gson.toJson(rawHops)
    assertEquals(mergedStr, rawStr)

    timer.start()
    val sawtoothIrs = sawtoothAggregator.computeWindows(rawHops, queries)
    timer.publish("sawtooth/ComputeWindows")

    val windows = aggregations.flatMap(_.unpack.map(_.window)).toArray
    val tailHops = windows.map(FiveMinuteResolution.calculateTailHop)
    val naiveAggregator = new NaiveAggregator(
      sawtoothAggregator.windowedAggregator,
      windows,
      tailHops
    )
    val naiveIrs = naiveAggregator.aggregate(events, queries)
    timer.publish("naive/Aggregate")

    assertEquals(naiveIrs.length, queries.length)
    assertEquals(sawtoothIrs.length, queries.length)
    for (i <- queries.indices) {
      val naiveStr = gson.toJson(naiveIrs(i))
      val sawtoothStr = gson.toJson(sawtoothIrs(i))
      assertEquals(naiveStr, sawtoothStr)
    }
  }

  // the result is irs in sorted order of queries
  // with head real-time accuracy and tail hop accuracy
  // NOTE: This provides a sketch for a distributed topology
  def sawtoothAggregate(
      events: Array[TestRow],
      queries: Array[Long],
      specs: Seq[Aggregation],
      schema: Seq[(String, DataType)],
      resolution: Resolution = FiveMinuteResolution
  ): Array[Array[Any]] = {

    // STEP-1. build hops
    val hopsAggregator =
      new HopsAggregator(queries.min, specs, schema, resolution)
    val sawtoothAggregator =
      new SawtoothAggregator(specs, schema, resolution)
    var hopMaps = hopsAggregator.init()
    for (i <- events.indices)
      hopMaps = hopsAggregator.update(hopMaps, events(i))
    val hops = hopsAggregator.toTimeSortedArray(hopMaps)

    val minResolution = resolution.hopSizes.min
    // STEP-2. group events and queries by tailEnd - 5minute round down of ts
    val groupedQueries: Map[Long, Array[Long]] =
      queries.groupBy(TsUtils.round(_, minResolution))
    val groupedEvents: Map[Long, Array[TestRow]] = events.groupBy { row =>
      TsUtils.round(row.ts, minResolution)
    }

    // STEP-3. compute windows based on tailEnds
    // aggregates will have at-most 5 minute staleness
    val headStartTimes = groupedQueries.keys.toArray
    util.Arrays.sort(headStartTimes)
    // compute windows up-to 5min accuracy for the queries
    val tailIrs = sawtoothAggregator.computeWindows(hops, headStartTimes)

    val result = mutable.ArrayBuffer.empty[Array[Any]]
    // STEP-4. join tailAccurate - Irs with headTimeStamps and headEvents
    // to achieve realtime accuracy
    for (i <- headStartTimes.indices) {
      val tailEnd = headStartTimes(i)
      val tailIr = tailIrs(i)

      // join events and queries on tailEndTimes
      val endTimes: Array[Long] = groupedQueries.getOrElse(tailEnd, null)
      val headEvents = groupedEvents.getOrElse(tailEnd, null)
      if (endTimes != null && headEvents != null) {
        util.Arrays.sort(endTimes)
      }

      result ++= sawtoothAggregator.cumulateUnsorted(
        Option(headEvents).map(_.iterator).orNull,
        endTimes,
        tailIr
      )
    }
    result.toArray
  }

  def testRealTimeAccuracy(): Unit = {
    val timer = new Timer
    val queries = genTimestamps(1, 10000, Window(1, TimeUnit.Days)).sorted
    val events = {
      val eventCount = 100000
      val eventTimes = genTimestamps(1, eventCount, Window(180, TimeUnit.Days))
      // max is 1M to avoid overflow when summing
      val eventValues = genNums(0, 1000, eventCount)
      eventTimes.zip(eventValues).map {
        case (time, value) => TestRow(time, value)
      }
    }

    val schema = List(
      "ts" -> LongType,
      "num" -> LongType
    )

    val aggregations: Seq[Aggregation] = Seq(
      Aggregation(Operation.Average,
                  "num",
                  Seq(
                    Window(1, TimeUnit.Days),
                    Window(1, TimeUnit.Hours),
                    Window(30, TimeUnit.Days)
                  )))
    timer.publish("setup")
    val sawtoothIrs = sawtoothAggregate(events, queries, aggregations, schema)
    timer.publish("sawtoothAggregate")

    val windows = aggregations.flatMap(_.unpack.map(_.window)).toArray
    val tailHops = windows.map(FiveMinuteResolution.calculateTailHop)
    val naiveAggregator = new NaiveAggregator(
      new RowAggregator(schema, aggregations.flatMap(_.unpack)),
      windows,
      tailHops
    )
    val naiveIrs = naiveAggregator.aggregate(events, queries)
    timer.publish("naiveAggregate")

    assertEquals(naiveIrs.length, queries.length)
    assertEquals(sawtoothIrs.length, queries.length)
    val gson = new Gson
    for (i <- queries.indices) {
      val naiveStr = gson.toJson(naiveIrs(i))
      val sawtoothStr = gson.toJson(sawtoothIrs(i))
      assertEquals(naiveStr, sawtoothStr)
    }
    timer.publish("comparison")
  }

}
