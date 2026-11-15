/*
 *    Copyright (C) 2023 The Chronon Authors.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package ai.chronon.aggregator.test

import org.slf4j.LoggerFactory
import ai.chronon.aggregator.row.RowAggregator
import ai.chronon.aggregator.test.SawtoothAggregatorTest.sawtoothAggregate
import ai.chronon.aggregator.windowing._
import ai.chronon.api.Extensions.AggregationOps
import ai.chronon.api._
import com.google.gson.Gson
import junit.framework.TestCase
import org.junit.Assert._

import java.util
import scala.collection.mutable
import scala.collection.Seq

class Timer {
  @transient lazy val logger = LoggerFactory.getLogger(getClass)

  var ts: Long = System.currentTimeMillis()

  def start(): Unit = { ts = System.currentTimeMillis() }

  // TODO: Write this out into a file checked into git
  // or incorporate proper benchmarks
  def publish(name: String, reset: Boolean = true): Unit = {
    logger.info(s"${name.padTo(25, ' ')} ${System.currentTimeMillis() - ts} ms")
    if (reset) ts = System.currentTimeMillis()
  }
}

class SawtoothAggregatorTest extends TestCase {

  def testTailAccuracy(): Unit = {
    val timer = new Timer
    val queries = CStream.genTimestamps(new Window(30, TimeUnit.DAYS), 10000, 5 * 60 * 1000)

    val columns = Seq(Column("ts", LongType, 180), Column("num", LongType, 1000))
    val events = CStream.gen(columns, 10000).rows
    val schema = columns.map(_.schema)

    val aggregations: Seq[Aggregation] = Seq(
      Builders.Aggregation(
        Operation.AVERAGE,
        "num",
        Seq(new Window(1, TimeUnit.DAYS), new Window(1, TimeUnit.HOURS), new Window(30, TimeUnit.DAYS))))
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

  def testRealTimeAccuracy(): Unit = {
    val timer = new Timer
    val queries = CStream.genTimestamps(new Window(1, TimeUnit.DAYS), 1000)
    val columns = Seq(Column("ts", LongType, 180),
                      Column("num", LongType, 1000),
                      Column("age", LongType, 100),
                      Column("bucket1", StringType, 3),
                      Column("bucket2", StringType, 2))
    val events = CStream.gen(columns, 10000).rows
    val schema = columns.map(_.schema)

    val aggregations: Seq[Aggregation] = Seq(
      Builders.Aggregation(Operation.AVERAGE,
                           "num",
                           Seq(
                             new Window(1, TimeUnit.DAYS),
                             new Window(1, TimeUnit.HOURS),
                             new Window(30, TimeUnit.DAYS)
                           ),
                           buckets = Seq("bucket1", "bucket2")),
      Builders.Aggregation(Operation.AVERAGE, "age", buckets = Seq("bucket1")),
      Builders.Aggregation(Operation.AVERAGE,
                           "age",
                           Seq(
                             new Window(1, TimeUnit.DAYS),
                             new Window(1, TimeUnit.HOURS),
                             new Window(30, TimeUnit.DAYS)
                           )),
      Builders.Aggregation(Operation.SUM, "age")
    )
    timer.publish("setup")
    val sawtoothIrs = sawtoothAggregate(events, queries, aggregations, schema)
    timer.publish("sawtoothAggregate")

    val unpacked = aggregations.flatMap(_.unpack.map(_.window)).toArray
    val tailHops = unpacked.map(FiveMinuteResolution.calculateTailHop)
    val rowAgg = new RowAggregator(schema, aggregations.flatMap(_.unpack))
    val naiveAggregator = new NaiveAggregator(
      rowAgg,
      unpacked,
      tailHops
    )
    val naiveIrs = naiveAggregator.aggregate(events, queries)
    timer.publish("naiveAggregate")

    assertEquals(naiveIrs.length, queries.length)
    assertEquals(sawtoothIrs.length, queries.length)
    val gson = new Gson
    for (i <- queries.indices) {
      val naiveStr = gson.toJson(rowAgg.finalize(naiveIrs(i)))
      val sawtoothStr = gson.toJson(rowAgg.finalize(sawtoothIrs(i)))
      assertEquals(naiveStr, sawtoothStr)
    }
    timer.publish("comparison")
  }

  private val minute = 60 * 1000L
  private val hour = 60 * minute
  private val histogramDay = 1699920000000L // 2023-11-14 00:00 UTC

  // APPROX_HISTOGRAM_K with k = 3 over five-minute hops. The 10:00 hop holds "a" ten times, so its IR stays an exact
  // histogram. The 10:05 hop holds four distinct items, more than k, so its IR is a sketch. Five distinct items in
  // total never fill the sketch, so every count is exact.
  private val histogramEvents: Array[TestRow] =
    (Seq.fill(10)(TestRow(histogramDay + 10 * hour, "a")) ++
      Seq("b" -> 5, "c" -> 4, "d" -> 3, "e" -> 2).flatMap {
        case (item, n) => Seq.fill(n)(TestRow(histogramDay + 10 * hour + 5 * minute, item))
      }).toArray

  // finalized windows per query, from NaiveAggregator and from computeWindows
  private def approxHistogramKWindows(windows: Seq[Window], queries: Array[Long]): (Seq[Seq[Any]], Seq[Seq[Any]]) = {
    val schema = Seq("ts" -> LongType, "item" -> StringType)
    val aggregations =
      Seq(Builders.Aggregation(Operation.APPROX_HISTOGRAM_K, "item", windows, argMap = Map("k" -> "3")))

    val hopsAggregator = new HopsAggregator(queries.min, aggregations, schema, FiveMinuteResolution)
    var hopMaps = hopsAggregator.init()
    histogramEvents.foreach(row => hopMaps = hopsAggregator.update(hopMaps, row))
    val sawtoothAggregator = new SawtoothAggregator(aggregations, schema, FiveMinuteResolution)
    val sawtoothIrs = sawtoothAggregator.computeWindows(hopsAggregator.toTimeSortedArray(hopMaps), queries)

    val rowAgg = sawtoothAggregator.windowedAggregator
    val unpackedWindows = aggregations.flatMap(_.unpack.map(_.window)).toArray
    val naiveIrs =
      new NaiveAggregator(rowAgg, unpackedWindows, unpackedWindows.map(FiveMinuteResolution.calculateTailHop))
        .aggregate(histogramEvents, queries)

    def finalized(irs: Array[Array[Any]]): Seq[Seq[Any]] = irs.toSeq.map(ir => rowAgg.finalize(ir).toSeq)
    (finalized(naiveIrs), finalized(sawtoothIrs))
  }

  private def histogram(counts: (String, Long)*): util.HashMap[String, Long] = {
    val result = new util.HashMap[String, Long]()
    counts.foreach { case (item, count) => result.put(item, count) }
    result
  }

  // Both windows read the same hops. Merging into the 10:05 hop must not write "a" into it, or the second window
  // counts "a" twice.
  def testApproxHistogramKWindowsSharingHops(): Unit = {
    val queries = Array(histogramDay + 10 * hour + 10 * minute)
    val (naive, sawtooth) =
      approxHistogramKWindows(Seq(new Window(1, TimeUnit.HOURS), new Window(2, TimeUnit.HOURS)), queries)

    val expected = histogram("a" -> 10L, "b" -> 5L, "c" -> 4L)
    assertEquals(Seq(Seq(expected, expected)), naive)
    assertEquals(naive, sawtooth)
  }

  // The 11:05 window is [10:05, 11:05), so it excludes the 10:00 events even though the 10:10 window merged them with
  // the 10:05 hop.
  def testApproxHistogramKWindowSlidingPastAHop(): Unit = {
    val queries = Array(histogramDay + 10 * hour + 10 * minute, histogramDay + 11 * hour + 5 * minute)
    val (naive, sawtooth) = approxHistogramKWindows(Seq(new Window(1, TimeUnit.HOURS)), queries)

    assertEquals(Seq(Seq(histogram("a" -> 10L, "b" -> 5L, "c" -> 4L)), Seq(histogram("b" -> 5L, "c" -> 4L, "d" -> 3L))),
                 naive)
    assertEquals(naive, sawtooth)
  }
}

object SawtoothAggregatorTest {
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
    // STEP-2. group events and queries by headStart - 5minute round down of ts
    val groupedQueries: Map[Long, Array[Long]] =
      queries.groupBy(TsUtils.round(_, minResolution))
    val groupedEvents: Map[Long, Array[TestRow]] = events.groupBy { row =>
      TsUtils.round(row.ts, minResolution)
    }

    // STEP-3. compute windows based on headStart
    // aggregates will have at-most 5 minute staleness
    val headStartTimes = groupedQueries.keys.toArray
    util.Arrays.sort(headStartTimes)
    // compute windows up-to 5min accuracy for the queries
    val nonRealtimeIrs = sawtoothAggregator.computeWindows(hops, headStartTimes)

    val result = mutable.ArrayBuffer.empty[Array[Any]]
    // STEP-4. join tailAccurate - Irs with headTimeStamps and headEvents
    // to achieve realtime accuracy
    for (i <- headStartTimes.indices) {
      val headStart = headStartTimes(i)
      val tailIr = nonRealtimeIrs(i)

      // join events and queries on tailEndTimes
      val endTimes: Array[Long] = groupedQueries.getOrElse(headStart, null)
      val headEvents = groupedEvents.getOrElse(headStart, null)
      if (endTimes != null && headEvents != null) {
        util.Arrays.sort(endTimes)
      }

      result ++= sawtoothAggregator.cumulate(
        Option(headEvents).map(_.iterator).orNull,
        endTimes,
        tailIr
      )
    }
    result.toArray
  }
}
