package ai.chronon.aggregator.test

import ai.chronon.aggregator.base.TopK
import ai.chronon.aggregator.test.SawtoothAggregatorTest.sawtoothAggregate
import ai.chronon.aggregator.windowing.{FiveMinuteResolution, SawtoothAggregator, TwoStackLiteAggregationBuffer, TwoStackLiteAggregator}
import ai.chronon.api.Extensions.AggregationOps
import ai.chronon.api._
import com.google.gson.Gson
import junit.framework.TestCase
import org.junit.Assert._

class TwoStackLiteAggregatorTest extends TestCase{
  def testBufferWithTopK(): Unit = {
    val topK = new TopK[Integer](IntType, 2)
    val bankersBuffer = new TwoStackLiteAggregationBuffer(topK, 5)
    assertEquals(null, bankersBuffer.query) // null
    Seq(7, 8, 9).map(x => new Integer(x)).foreach(i => bankersBuffer.push(i))
    def assertBufferEquals(a: Seq[Int], b: java.util.ArrayList[Integer]): Unit = {
      if(a==null || b == null) {
        assertEquals(a, b)
      } else {
        assertArrayEquals(
          Option(a).map(_.map(x => new Integer(x).asInstanceOf[AnyRef]).toArray).orNull,
          Option(b).map(_.toArray).orNull)
      }
    }
    assertBufferEquals(Seq(8, 9), bankersBuffer.query)
    bankersBuffer.pop()
    assertBufferEquals(Seq(8, 9),bankersBuffer.query)
    bankersBuffer.pop()
    assertBufferEquals(Seq(9), bankersBuffer.query)
    bankersBuffer.pop()
    assertBufferEquals(null, bankersBuffer.query)
    bankersBuffer.push(new Integer(10))
    assertBufferEquals(Seq(10), bankersBuffer.query)
  }


  def testRealTimeAccuracy(): Unit = {
    val timer = new Timer

    val queryCount: Int = 1000
    val eventCount: Int = 100000

    val queries = CStream.genTimestamps(new Window(1, TimeUnit.DAYS), queryCount)
    val columns = Seq(Column("ts", LongType, 180),
      Column("num", LongType, 1000),
      Column("age", LongType, 100),
      Column("bucket1", StringType, 3),
      Column("bucket2", StringType, 2))
    val events = CStream.gen(columns, eventCount).rows
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


    val sawtoothAggregator = new SawtoothAggregator(aggregations, schema, FiveMinuteResolution)
    val windows = aggregations.flatMap(_.unpack.map(_.window)).toArray
    val tailHops = windows.map(FiveMinuteResolution.calculateTailHop)
    val naiveAggregator = new NaiveAggregator(
      sawtoothAggregator.windowedAggregator,
      windows,
      tailHops
    )
    val naiveIrs = naiveAggregator.aggregate(events, queries).map(sawtoothAggregator.windowedAggregator.finalize)
    timer.publish("naive")


    val bankersAggregator = new TwoStackLiteAggregator(
      StructType("", columns.map(c => StructField(c.name, c.`type`)).toArray),
      aggregations)

    val bankersIrs = bankersAggregator.slidingSawtoothWindow(queries.sorted.iterator, events.sortBy(_.ts).iterator, events.length).toArray

    timer.publish("bankers")


    val gson = new Gson()
    bankersIrs.zip(naiveIrs).foreach { case (bankers, naive) =>
      assertEquals(gson.toJson(naive), gson.toJson(bankers))
    }
  }


  def testAgainstSawtooth(): Unit = {
    val timer = new Timer
    val queries = CStream.genTimestamps(new Window(30, TimeUnit.DAYS), 100000, 5 * 60 * 1000)

    val columns = Seq(Column("ts", LongType, 180), Column("num", LongType, 1000))
    val events = CStream.gen(columns, 10000).rows
    val schema = columns.map(_.schema)

    val aggregations: Seq[Aggregation] = Seq(
      Builders.Aggregation(
        Operation.AVERAGE,
        "num",
        Seq(new Window(1, TimeUnit.DAYS), new Window(1, TimeUnit.HOURS), new Window(30, TimeUnit.DAYS))),
      Builders.Aggregation(
        Operation.AVERAGE,
        "num"),
      Builders.Aggregation(
        Operation.TOP_K,
        "num",
        Seq(new Window(1, TimeUnit.DAYS), new Window(1, TimeUnit.HOURS), new Window(30, TimeUnit.DAYS)),
        argMap = Map("k" -> "300")),
      Builders.Aggregation(
        Operation.TOP_K,
        "num",
        argMap = Map("k" -> "300")),
    )

    timer.publish("setup")

    val sawtoothAggregator =
      new SawtoothAggregator(aggregations, schema, FiveMinuteResolution)

    //    val windows = aggregations.flatMap(_.unpack.map(_.window)).toArray
    //    val tailHops = windows.map(FiveMinuteResolution.calculateTailHop)
    //    val naiveAggregator = new NaiveAggregator(
    //      sawtoothAggregator.windowedAggregator,
    //      windows,
    //      tailHops
    //    )
    //    val naiveIrs = naiveAggregator.aggregate(events, queries).map(sawtoothAggregator.windowedAggregator.finalize)
    //    timer.publish("naive")
    val bankersAggregator = new TwoStackLiteAggregator(
      StructType("", columns.map(c => StructField(c.name, c.`type`)).toArray),
      aggregations)

    // will finalize by default
    val bankersIrs = bankersAggregator.slidingSawtoothWindow(queries.sorted.iterator, events.sortBy(_.ts).iterator, events.length).toArray
    timer.publish("sorting + banker")

    val sawtoothIrs = sawtoothAggregate(events, queries, aggregations, schema)
      .map(sawtoothAggregator.windowedAggregator.finalize)
    timer.publish("sawtooth")

    // rough timings below will vary by processor - but at 100k
    // naive                     256011 ms
    // sorting + banker          1597 ms
    // sawtooth                  914 ms

    val gson = new Gson()
    bankersIrs.zip(sawtoothIrs).foreach { case (bankers, sawtooth) =>
      assertEquals(gson.toJson(sawtooth), gson.toJson(bankers))
    }
  }

}
