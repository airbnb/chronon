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

import ai.chronon.aggregator.test.SawtoothAggregatorTest.sawtoothAggregate
import ai.chronon.aggregator.windowing.{FinalBatchIr, FiveMinuteResolution, SawtoothOnlineAggregator, TsUtils}
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
        operation = Operation.COUNT,
        inputColumn = "num",
        windows = Seq(new Window(14, TimeUnit.DAYS),
                      new Window(20, TimeUnit.HOURS),
                      new Window(6, TimeUnit.DAYS),
                      new Window(7, TimeUnit.DAYS))
      ),
      Builders.Aggregation(
        operation = Operation.AVERAGE,
        inputColumn = "num",
        windows = Seq(new Window(14, TimeUnit.DAYS),
                      new Window(20, TimeUnit.HOURS),
                      new Window(6, TimeUnit.DAYS),
                      new Window(7, TimeUnit.DAYS))
      ),
      Builders.Aggregation(
        operation = Operation.FIRST,
        inputColumn = "ts_col",
        windows = Seq(new Window(23, TimeUnit.HOURS), new Window(14, TimeUnit.DAYS)),
        argMap = Map("k" -> "4")
      ),
      Builders.Aggregation(
        operation = Operation.LAST,
        inputColumn = "ts_col",
        windows = Seq(new Window(23, TimeUnit.HOURS), new Window(14, TimeUnit.DAYS))
      ),
      Builders.Aggregation(
        operation = Operation.SUM,
        inputColumn = "num",
        windows = null
      ),
      Builders.Aggregation(
        operation = Operation.UNIQUE_COUNT,
        inputColumn = "user",
        windows = Seq(new Window(23, TimeUnit.HOURS), new Window(14, TimeUnit.DAYS))
      ),
      Builders.Aggregation(
        operation = Operation.APPROX_UNIQUE_COUNT,
        inputColumn = "user",
        windows = Seq(new Window(23, TimeUnit.HOURS), new Window(14, TimeUnit.DAYS))
      ),
      Builders.Aggregation(
        operation = Operation.LAST_K,
        inputColumn = "user",
        windows = Seq(new Window(23, TimeUnit.HOURS), new Window(14, TimeUnit.DAYS)),
        argMap = Map("k" -> "4")
      ),
      Builders.Aggregation(
        operation = Operation.FIRST_K,
        inputColumn = "user",
        windows = Seq(new Window(23, TimeUnit.HOURS), new Window(14, TimeUnit.DAYS)),
        argMap = Map("k" -> "4")
      ),
      Builders.Aggregation(
        operation = Operation.TOP_K,
        inputColumn = "num",
        windows = Seq(new Window(23, TimeUnit.HOURS), new Window(14, TimeUnit.DAYS)),
        argMap = Map("k" -> "4")
      ),
      Builders.Aggregation(
        operation = Operation.MIN,
        inputColumn = "num",
        windows = Seq(new Window(23, TimeUnit.HOURS), new Window(14, TimeUnit.DAYS))
      ),
      Builders.Aggregation(
        operation = Operation.MAX,
        inputColumn = "num",
        windows = Seq(new Window(23, TimeUnit.HOURS), new Window(14, TimeUnit.DAYS))
      )
    )

    val sawtoothIrs = sawtoothAggregate(events, queries, aggregations, schema)
    val onlineAggregator = new SawtoothOnlineAggregator(batchEndTs, aggregations, schema, FiveMinuteResolution)
    val (events1, events2) = events.splitAt(eventCount / 2)
    val batchIr1 = events1.foldLeft(onlineAggregator.init)(onlineAggregator.update)
    val batchIr2 = events2.foldLeft(onlineAggregator.init)(onlineAggregator.update)
    val batchIr = onlineAggregator.normalizeBatchIr(onlineAggregator.merge(batchIr1, batchIr2))
    val denormBatchIr = onlineAggregator.denormalizeBatchIr(batchIr)
    val windowHeadEvents = events.filter(_.ts >= batchEndTs)
    val onlineIrs = queries.map(onlineAggregator.lambdaAggregateIr(denormBatchIr, windowHeadEvents.iterator, _))

    val gson = new Gson()
    for (i <- queries.indices) {
      val onlineStr = gson.toJson(onlineAggregator.windowedAggregator.finalize(onlineIrs(i)))
      val sawtoothStr = gson.toJson(onlineAggregator.windowedAggregator.finalize(sawtoothIrs(i)))
      assertEquals(sawtoothStr, onlineStr)
    }
  }

  def testLambdaAggregateDoNotMutateBatchIrAndOverCount(): Unit = {
    val aggregations = Seq(
      Builders.Aggregation(
        operation = Operation.HISTOGRAM,
        inputColumn = "action",
        windows = Seq(
          new Window(3, TimeUnit.DAYS)
        )
      )
    )
    val inputSchema: Seq[(String, DataType)] = Seq(
      ("ts", LongType),
      ("action", StringType)
    )
    val batchEndTs = 1746921600000L // 2025-05-11 00:00:00 UTC
    val onlineAgg = new SawtoothOnlineAggregator(batchEndTs, aggregations, inputSchema)

    def hop(count: Int, ts: Long): Array[Any] = {
      val m = new java.util.HashMap[String, Int]();
      m.put("view", count)
      Array[Any](m, ts)
    }

    val finalBatchIr = FinalBatchIr(
      Array[Any](
        null // collapsed (T-1 -> T)
      ),
      Array(
        Array.empty, // 1‑day hops (not used)
        Array( // 1-hour hops
              hop(1, 1746745200000L), // 2025-05-08 23:00:00 UTC
              hop(1, 1746766800000L) // 2025-05-09 05:00:00 UTC
        ),
        Array.empty // 5‑minute hops (not used)
      )
    )
    val queryTs = batchEndTs + 100

    val expectedCount = 2
    var aggResult: Array[Any] = Array()
    for (_ <- 0 until 10) {
      aggResult = onlineAgg.lambdaAggregateFinalizedTiled(finalBatchIr, Seq.empty.toIterator, queryTs)
      val res3d0 = aggResult(0).asInstanceOf[java.util.Map[String, Int]]
      assertEquals(expectedCount, res3d0.get("view"))
      val mutated = finalBatchIr.tailHops(1)(0)(0).asInstanceOf[java.util.Map[String, Int]].get("view")
      assertEquals(1, mutated)
    }
  }
}
