package ai.zipline.aggregator.test

import java.util
import ai.zipline.aggregator.row.RowAggregator
import ai.zipline.aggregator.test.SawtoothAggregatorTest.sawtoothAggregate
import ai.zipline.aggregator.windowing._
import ai.zipline.api.Extensions._
import ai.zipline.api._
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
