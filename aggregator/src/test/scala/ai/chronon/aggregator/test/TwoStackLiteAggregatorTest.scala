package ai.chronon.aggregator.test

import ai.chronon.aggregator.base.TopK
import ai.chronon.aggregator.test.SawtoothAggregatorTest.sawtoothAggregate
import ai.chronon.aggregator.windowing.{FiveMinuteResolution, SawtoothAggregator, TwoStackLiteAggregationBuffer, TwoStackLiteAggregator}
import ai.chronon.api.Extensions.{AggregationOps, AggregationPartOps, WindowOps}
import ai.chronon.api._
import com.google.gson.Gson
import junit.framework.TestCase
import org.junit.Assert._

class TwoStackLiteAggregatorTest extends TestCase {

  def assertBufferEquals(a: Seq[Int], b: java.util.ArrayList[Integer]): Unit = {
    if (a == null || b == null) {
      assertEquals(a, b)
    } else {
      assertArrayEquals(
        Option(a).map(_.map(x => new Integer(x).asInstanceOf[AnyRef]).toArray).orNull,
        Option(b).map(_.toArray).orNull)
    }
  }

  def testSquashBufferWithTopK(): Unit = {
    val topK = new TopK[Integer](IntType, 2)
    val bankersBuffer = new TwoStackLiteAggregationBuffer(topK, 5, Some(10L), true)
    assertEquals(null, bankersBuffer.query) // null
    Seq(7, 8, 9).map(x => new Integer(x)).foreach(i => bankersBuffer.push(i, i * 100L))

    assertBufferEquals(Seq(8, 9), bankersBuffer.query)
    bankersBuffer.pop()
    assertBufferEquals(Seq(8, 9), bankersBuffer.query)
    bankersBuffer.pop()
    assertBufferEquals(Seq(9), bankersBuffer.query)
    bankersBuffer.pop()
    assertBufferEquals(null, bankersBuffer.query)
    bankersBuffer.push(new Integer(10))
    assertBufferEquals(Seq(10), bankersBuffer.query)
  }




  def testNonSquashBufferWithTopK(): Unit = {
    val topK = new TopK[Integer](IntType, 2)
    val bankersBuffer = new TwoStackLiteAggregationBuffer(topK, 5)
    assertEquals(null, bankersBuffer.query) // null
    Seq(7, 8, 9).map(x => new Integer(x)).foreach(i => bankersBuffer.push(i))


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


    val twoStackLiteAggregator = new TwoStackLiteAggregator(
      StructType("", columns.map(c => StructField(c.name, c.`type`)).toArray),
      aggregations)

    val bankersIrs = twoStackLiteAggregator.slidingSawtoothWindow(queries.sorted.iterator, events.sortBy(_.ts).iterator, events.length).toArray

    timer.publish("sort + bankers")


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

    val bankersAggregator = new TwoStackLiteAggregator(
      StructType("", columns.map(c => StructField(c.name, c.`type`)).toArray),
      aggregations)

    // will finalize by default
    val bankersIrs = bankersAggregator.slidingSawtoothWindow(queries.sorted.iterator, events.sortBy(_.ts).iterator, events.length).toArray
    timer.publish("sorting + banker")

    val sawtoothIrs = sawtoothAggregate(events, queries, aggregations, schema)
      .map(sawtoothAggregator.windowedAggregator.finalize)
    timer.publish("sawtooth")

    val gson = new Gson()
    bankersIrs.zip(sawtoothIrs).foreach { case (bankers, sawtooth) =>
      assertEquals(gson.toJson(sawtooth), gson.toJson(bankers))
    }
  }

  def testBankersGenerateManyColumnTestCases(): Unit = {
    (1 to 100).foreach { _ =>
      // TODO: is 5k rows over 60 day windows dense enough to exercise any actual bugs?
      val queries = CStream.genTimestamps(new Window(60, TimeUnit.DAYS), 5000) // don't round off queries

      val columns = Seq(Column("ts", LongType, 60), Column("num", LongType, 1000))
      val events = CStream.gen(columns, 5000).rows
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

      val sawtoothAggregator =
        new SawtoothAggregator(aggregations, schema, FiveMinuteResolution)

      val bankersAggregator = new TwoStackLiteAggregator(
        StructType("", columns.map(c => StructField(c.name, c.`type`)).toArray),
        aggregations)

      // will finalize by default
      val bankersIrs = bankersAggregator.slidingSawtoothWindow(queries.sorted.iterator, events.sortBy(_.ts).iterator, events.length).toArray

      val sawtoothIrs = sawtoothAggregate(events, queries, aggregations, schema)
        .map(sawtoothAggregator.windowedAggregator.finalize)

      val gson = new Gson()
      bankersIrs.zip(sawtoothIrs).foreach { case (bankers, sawtooth) =>
        assertEquals(gson.toJson(sawtooth), gson.toJson(bankers))
      }
    }
  }

  def simpleCompareBankersSawtooths(queries: Array[Long], events: Array[TestRow], columns: Seq[Column], window: Window): Unit = {

    val aggregations: Seq[Aggregation] = Seq(
      Builders.Aggregation(
        Operation.AVERAGE,
        "num",
        Seq(
          window,
        )
      )
    )

    val schema = columns.map(_.schema)
    val sawtoothAggregator =
      new SawtoothAggregator(aggregations, schema, FiveMinuteResolution)

    val bankersAggregator = new TwoStackLiteAggregator(
      StructType("", columns.map(c => StructField(c.name, c.`type`)).toArray),
      aggregations)

    // will finalize by default
    val bankersIrs = bankersAggregator.slidingSawtoothWindow(queries.sorted.iterator, events.sortBy(_.ts).iterator, events.length).toArray
    assert(bankersIrs.length == queries.length)

    val sawtoothIrs = sawtoothAggregate(events, queries, aggregations, schema)
      .map(sawtoothAggregator.windowedAggregator.finalize)

    val gson = new Gson()
    queries.zip(bankersIrs).zip(sawtoothIrs).foreach { case ((queryTs, bankers), sawtooth) =>
      assertEquals(s"Mismatch at query ts $queryTs", gson.toJson(sawtooth), gson.toJson(bankers))
    }
  }

  def testBankersSmallTestCase1(): Unit = {
    /*
    FIXED example of failing test case.

    In the final query at 109439586L, the debugger says that the queue has a hop sum of (56, 2)
    with ts 68800061L, and an aggBack of (51, 1) with frontlen 1 and deque size 1.

    It's setting front to a clone of the first (oldest?) deque hop sum, then merging that with
    aggBack to get (107, 3) => 35.666 instead of (56, 2) => 28
     */
    val columns = Seq(Column("ts", LongType, 2), Column("num", LongType, 100))

    val events = Array(
      TestRow(11735087L, 13L),
      TestRow(68800061L, 5L),
      TestRow(70481921L, 51L)
    )
    val queries = Array(
      60823084L,
      70079474L,
      109439586L
    )

    val window = new Window(1, TimeUnit.DAYS)
    simpleCompareBankersSawtooths(queries, events, columns, window)
  }

  def testBankersSmallTestCase2(): Unit = {
    /*
    Tiny example of failing test case.

    Returning 6.5 or (12 + 1) / 2 instead of 14.333 or (12 + 1 + 30) / 3
     */
    val columns = Seq(Column("ts", LongType, 2), Column("num", LongType, 100))

    val events = Array(
      TestRow(1910812L, 85L),
      TestRow(29454097L, 1L),
      TestRow(50650389L, 12L),
      TestRow(53978384L, 30L)
    )
    val queries = Array(
      49608871L,
      52261248L,
      97488184L
    )

    val window = new Window(1, TimeUnit.DAYS)
    simpleCompareBankersSawtooths(queries, events, columns, window)
  }

  def testBankersGenerateOneColumnSmallTestCases(): Unit = {
    (1 to 300000).foreach { i =>
      val columns = Seq(Column("ts", LongType, 2), Column("num", LongType, 100))
      val events = CStream.gen(columns, 4).rows
      val queries = CStream.genTimestamps(new Window(2, TimeUnit.DAYS), 4)
      val window = new Window(1, TimeUnit.DAYS)

      simpleCompareBankersSawtooths(queries, events, columns, window)
    }
  }

  def testBankersGenerateOneColumnLargeTestCases(): Unit = {
    (1 to 300).foreach { i =>
      val columns = Seq(Column("ts", LongType, 2), Column("num", LongType, 100))
      val events = CStream.gen(columns, 10000).rows
      val queries = CStream.genTimestamps(new Window(2, TimeUnit.DAYS), 10000)
      val window = new Window(1, TimeUnit.DAYS)

      simpleCompareBankersSawtooths(queries, events, columns, window)
    }
  }
}
