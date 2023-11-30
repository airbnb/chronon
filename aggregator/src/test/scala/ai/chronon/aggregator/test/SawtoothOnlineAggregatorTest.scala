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
import java.util.concurrent.Executors
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration._

class SawtoothOnlineAggregatorTest extends TestCase {

  def testConsistency(): Unit = {
    implicit val executionContext: ExecutionContext = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(1))
    val queryEndTs = TsUtils.round(System.currentTimeMillis(), WindowUtils.Day.millis)
    val batchEndTs = queryEndTs - WindowUtils.Day.millis
    val queries = CStream.genTimestamps(new Window(1, TimeUnit.DAYS), 1000)
    val eventCount = 10000

    val columns = Seq(Column("ts", LongType, 60),
                      Column("num", LongType, 100),
                      Column("user", StringType, 6000),
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
          new Window(14, TimeUnit.DAYS), // hop = 1 day
          new Window(20, TimeUnit.HOURS), // hop = 1hr
          new Window(6, TimeUnit.DAYS), // hop = 1hr
          new Window(7, TimeUnit.DAYS), // hop = 1hr,
          new Window(10, TimeUnit.MINUTES) // hop = 5min
        )
      ),
      Builders.Aggregation(
        Operation.AVERAGE,
        "num",
        Seq(
          new Window(14, TimeUnit.DAYS), // hop = 1 day
          new Window(20, TimeUnit.HOURS), // hop = 1hr
          new Window(6, TimeUnit.DAYS), // hop = 1hr
          new Window(7, TimeUnit.DAYS), // hop = 1hr,
          new Window(10, TimeUnit.MINUTES) // hop = 5min
    )
      ),
      Builders.Aggregation(
        Operation.FIRST,
        "ts_col",
        Seq(
          new Window(23, TimeUnit.HOURS),
          new Window(14, TimeUnit.DAYS),
          new Window(10, TimeUnit.MINUTES)
        ),
        argMap = Map("k" -> "4")
      ),
      Builders.Aggregation(
        Operation.LAST,
        "ts_col",
        Seq(
          new Window(23, TimeUnit.HOURS),
          new Window(14, TimeUnit.DAYS),
          new Window(10, TimeUnit.MINUTES)
        )
      ),
      Builders.Aggregation(Operation.SUM, "num", null)
    )

    val sawtoothIrs = sawtoothAggregate(events, queries, aggregations, schema)
    val onlineAggregator = new SawtoothOnlineAggregator(batchEndTs, aggregations, schema, FiveMinuteResolution)
    val (events1, events2) = events.splitAt(eventCount / 2)
    val batchIr1 = events1.foldLeft(onlineAggregator.init)(onlineAggregator.update)
    val batchIr2 = events2.foldLeft(onlineAggregator.init)(onlineAggregator.update)
    val batchIr = onlineAggregator.normalizeBatchIr(onlineAggregator.merge(batchIr1, batchIr2))
    val denormBatchIr = onlineAggregator.denormalizeBatchIr(batchIr)
    val windowHeadEvents = events.filter(_.ts >= batchEndTs)
    val futureOnlineIrs = queries.map(onlineAggregator.lambdaAggregateIr(Future(denormBatchIr), windowHeadEvents.iterator, _))
    val onlineIrs: List[Array[Any]] = Await.result(Future.sequence(futureOnlineIrs.toList), 10.seconds)

    val gson = new Gson()
    for (i <- queries.indices) {
      val onlineStr = gson.toJson(onlineAggregator.windowedAggregator.finalize(onlineIrs(i)))
      val sawtoothStr = gson.toJson(onlineAggregator.windowedAggregator.finalize(sawtoothIrs(i)))
      assertEquals(sawtoothStr, onlineStr)
    }
  }

}
