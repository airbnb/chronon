package ai.chronon.aggregator.test

import ai.chronon.aggregator.base.{Sum, TopK}
import ai.chronon.aggregator.windowing.{BankersAggregationBuffer, FiveMinuteResolution, SawtoothAggregator}
import ai.chronon.api.{Aggregation, Builders, IntType, LongType, Operation, TimeUnit, Window}
import junit.framework.TestCase
import org.junit.Assert._
import ai.chronon.api.Extensions.AggregationOps

import scala.collection.Seq

class BankersAggregatorTest extends TestCase{

  object IntegerNumeric extends Numeric[Integer] {
    def plus(x: Integer, y: Integer): Integer = x.intValue() + y.intValue()
    def minus(x: Integer, y: Integer): Integer = x.intValue() - y.intValue()
    def times(x: Integer, y: Integer): Integer = x.intValue() * y.intValue()
    def negate(x: Integer): Integer = -x.intValue()
    def fromInt(x: Int): Integer = Integer.valueOf(x)
    def toInt(x: Integer): Int = x.intValue()
    def toLong(x: Integer): Long = x.longValue()
    def toFloat(x: Integer): Float = x.floatValue()
    def toDouble(x: Integer): Double = x.doubleValue()
    def compare(x: Integer, y: Integer): Int = x.compareTo(y)
    override def parseString(str: String): Option[Integer] = None
  }

  def testBufferWithSum(): Unit = {
    val summer = new Sum[Integer](IntType)(IntegerNumeric)
    val bankersBuffer = new BankersAggregationBuffer(summer)
    assertEquals(null, bankersBuffer.query)
    Seq(7, 8, 9).map(x => new Integer(x)).foreach(i => bankersBuffer.push(i))
    assertEquals(24,bankersBuffer.query)
    bankersBuffer.pop()
    assertEquals(17,bankersBuffer.query)
    bankersBuffer.pop()
    assertEquals(9, bankersBuffer.query)
    bankersBuffer.pop()
    assertEquals(null, bankersBuffer.query)
    bankersBuffer.push(new Integer(10))
    assertEquals(10, bankersBuffer.query)
  }


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
    val events = CStream.gen(columns, 10000).rows
    val schema = columns.map(_.schema)

    val aggregations: Seq[Aggregation] = Seq(
      Builders.Aggregation(
        Operation.AVERAGE,
        "num",
        Seq(new Window(1, TimeUnit.DAYS), new Window(1, TimeUnit.HOURS), new Window(30, TimeUnit.DAYS))),
      Builders.Aggregation(
        Operation.TOP_K,
        "num",
        Seq(new Window(1, TimeUnit.DAYS), new Window(1, TimeUnit.HOURS), new Window(30, TimeUnit.DAYS)))
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
    val naiveIrs = naiveAggregator.aggregate(events, queries)

  }

}
