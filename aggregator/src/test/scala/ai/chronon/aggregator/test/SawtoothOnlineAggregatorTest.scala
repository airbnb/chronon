package ai.chronon.aggregator.test

import ai.chronon.aggregator.test.SawtoothAggregatorTest.sawtoothAggregate
import ai.chronon.aggregator.windowing.{FiveMinuteResolution, SawtoothOnlineAggregator, TsUtils}
import ai.chronon.api.Extensions.{WindowOps, WindowUtils}
import ai.chronon.api._
import com.google.gson.Gson
import junit.framework.TestCase
import org.junit.Assert.assertEquals

import java.time.{Instant, ZoneOffset}
import java.time.format.DateTimeFormatter
import java.util.Locale

class SawtoothOnlineAggregatorTest extends TestCase {

  def testConsistency(): Unit = {
    val queryEndTs = TsUtils.round(System.currentTimeMillis(), WindowUtils.Day.millis)
    val batchEndTs = queryEndTs - WindowUtils.Day.millis
    val queries = CStream.genTimestamps(new Window(1, TimeUnit.DAYS), 1000)
    val eventCount = 50

    val columns = Seq(Column("ts", LongType, 60),
                      Column("num", LongType, 100),
                      Column("user", StringType, 6),
                      Column("ts_col", StringType, 60))
    val formatter = DateTimeFormatter
      .ofPattern("MM-dd HH:mm:ss", Locale.US)
      .withZone(ZoneOffset.UTC)
    val RowsWithSchema(events, schema) = CStream.gen(columns, eventCount)
    events.foreach { row => row.set(3, formatter.format(Instant.ofEpochMilli(row.get(0).asInstanceOf[Long]))) }
    val aggregations: Seq[Aggregation] = Seq(
      Builders.Aggregation(
        Operation.COUNT,
        "num",
        Seq(
          new Window(300, TimeUnit.HOURS), // hop = 5min
          new Window(7, TimeUnit.DAYS), // hop = 1hr
          new Window(30, TimeUnit.DAYS) // hop = 1d
        )
      ),
//      Builders.Aggregation(
//        Operation.FIRST_K,
//        "ts_col",
//        Seq(new Window(6, TimeUnit.HOURS), new Window(30, TimeUnit.DAYS)),
//        argMap = Map("k" -> "4")
//      ),
      Builders.Aggregation(Operation.SUM, "num", null)
    )

    val sawtoothIrs = sawtoothAggregate(events, queries, aggregations, schema, true)
    val onlineAggregator = new SawtoothOnlineAggregator(batchEndTs, aggregations, schema, FiveMinuteResolution)
    val (events1, events2) = events.splitAt(eventCount / 2)
    val batchIr1 = events1.foldLeft(onlineAggregator.init)(onlineAggregator.update)
    val batchIr2 = events2.foldLeft(onlineAggregator.init)(onlineAggregator.update)
    val batchIr = onlineAggregator.finalizeTail(onlineAggregator.merge(batchIr1, batchIr2))

    val windowHeadEvents = events.filter(_.ts >= batchEndTs)
    val onlineIrs = queries.map(onlineAggregator.lambdaAggregateFinalized(batchIr, windowHeadEvents.iterator, _))

    val gson = new Gson()
    for (i <- queries.indices) {
      val onlineStr = gson.toJson(onlineIrs(i))
      val sawtoothStr = gson.toJson(sawtoothIrs(i))
      assertEquals(sawtoothStr, onlineStr)
    }
  }

}
