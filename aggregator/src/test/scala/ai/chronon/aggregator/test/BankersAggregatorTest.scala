package ai.chronon.aggregator.test

import ai.chronon.aggregator.base.{Sum, TopK}
import ai.chronon.aggregator.windowing.{BankerSawtoothAggregator, BankersAggregationBuffer, FiveMinuteResolution, SawtoothAggregator}
import ai.chronon.api.{Aggregation, Builders, IntType, LongType, Operation, StructField, StructType, TimeUnit, Window}
import junit.framework.TestCase
import org.junit.Assert._
import ai.chronon.api.Extensions.AggregationOps
import com.google.gson.Gson

import scala.collection.Seq

class BankersAggregatorTest extends TestCase{
  def testBufferWithTopK(): Unit = {
    val topK = new TopK[Integer](IntType, 2)
    val bankersBuffer = new BankersAggregationBuffer(topK)
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

  def testAgainstNaive(): Unit = {
    val timer = new Timer
    val queries = CStream.genTimestamps(new Window(30, TimeUnit.DAYS), 10000, 5 * 60 * 1000)

    val columns = Seq(Column("ts", LongType, 180), Column("num", LongType, 1000))
    val events = CStream.gen(columns, 100000).rows
    val schema = columns.map(_.schema)

    val aggregations: Seq[Aggregation] = Seq(
      Builders.Aggregation(
        Operation.AVERAGE,
        "num",
        Seq(new Window(1, TimeUnit.DAYS), new Window(1, TimeUnit.HOURS), new Window(30, TimeUnit.DAYS))),
      Builders.Aggregation(
        Operation.TOP_K,
        "num",
        Seq(new Window(1, TimeUnit.DAYS), new Window(1, TimeUnit.HOURS), new Window(30, TimeUnit.DAYS)),
        argMap = Map("k" -> "300")
      )
    )
    timer.publish("setup")

    val sawtoothAggregator =
      new SawtoothAggregator(aggregations, schema, FiveMinuteResolution)

    val windows = aggregations.flatMap(_.unpack.map(_.window)).toArray
    val tailHops = windows.map(FiveMinuteResolution.calculateTailHop)
    val naiveAggregator = new NaiveAggregator(
      sawtoothAggregator.windowedAggregator,
      windows,
      tailHops
    )
    val naiveIrs = naiveAggregator.aggregate(events, queries).map(sawtoothAggregator.windowedAggregator.finalize)
    timer.publish("naive")
    val bankersAggregator = new BankerSawtoothAggregator(
      StructType("", columns.map(c => StructField(c.name, c.`type`)).toArray),
      aggregations)

    val bankersIrs = bankersAggregator.slidingSawtoothWindow(queries.sorted.iterator, events.sortBy(_.ts).iterator).toArray
    timer.publish("sorting + banker")
    val gson = new Gson()
    naiveIrs.zip(bankersIrs).foreach{case (naive, bankers) =>
        assertEquals(gson.toJson(naive), gson.toJson(bankers))
    }
  }

}
