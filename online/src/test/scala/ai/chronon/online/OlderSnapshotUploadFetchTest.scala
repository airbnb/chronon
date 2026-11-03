/*
 *    Copyright (C) 2026 The Chronon Authors.
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

package ai.chronon.online

import ai.chronon.api._
import ai.chronon.api.Extensions.{GroupByOps, StringOps, WindowOps, WindowUtils}
import ai.chronon.online.Fetcher.Request
import ai.chronon.online.KVStore.{GetRequest, GetResponse, PutRequest, TimedValue}
import ai.chronon.online.serde.{ArrayRow, AvroConversions}
import org.junit.Assert.{assertEquals, assertTrue}
import org.junit.{Before, Test}

import java.util.Base64
import java.util.concurrent.ConcurrentHashMap
import java.util.function.BiFunction
import scala.collection.Seq
import scala.concurrent.duration.DurationInt
import scala.concurrent.{Await, Future}
import scala.util.Try

/**
  * End to end online fetch of a temporally accurate (batch + streaming) GroupBy after an OLDER batch snapshot is
  * uploaded on top of a NEWER one, e.g. an on-call re-runs the upload for an old ds after newer ds already landed.
  *
  * The upload uses replace semantics, so the KV store ends up with:
  *   - batch IRs as of the older batchEndDate
  *   - GroupByServingInfo whose batchEndDate is the older date
  *
  * Every fetcher host still has the NEWER GroupByServingInfo in its TTL cache. With that stale batchEndTs the fetcher
  * drops the (older) batch IR as outdated and only scans streaming data after the newer batchEndTs, so features
  * collapse to "streaming tail only" (close to zero) until the cache entry is refreshed.
  *
  * Production uses a 2 hour TTL. Here the TTL is a few seconds so the test can show both behaviors:
  *   - the fetcher must serve correct features well before the TTL expires (fails without the refresh on an older
  *     batchEndTs, because only TTL expiry would fix the cache)
  *   - once the TTL expires the cache self heals (true with or without the refresh)
  */
class OlderSnapshotUploadFetchTest {
  import OlderSnapshotUploadFetchTest._

  private val ServingInfoTtlMillis = 3000L
  private val ServingInfoRefreshIntervalMillis = 200L
  private val PollIntervalMillis = 50L

  private var kvStore: InMemoryServingKvStore = _

  @Before
  def setup(): Unit = {
    kvStore = new InMemoryServingKvStore
    kvStore.create(groupByOps.batchDataset)
    kvStore.create(groupByOps.streamingDataset)

    // Initial state: newest snapshot is live, streaming job has written every review event.
    uploadSnapshot(NewerBatchEndDate)
    writeStreamingEvents(AllReviews)
  }

  @Test
  def testOlderSnapshotUploadServesCorrectFeaturesBeforeServingInfoTtlExpires(): Unit = {
    val fetcher = new ShortTtlFetcher(kvStore, ServingInfoTtlMillis, ServingInfoRefreshIntervalMillis)
    // The serving info entry is inserted into the TTL cache during the first fetch, so it expires no earlier than this.
    val cachedAtMillis = System.currentTimeMillis()

    // 1. Newer snapshot is live and cached on the fetcher: batch(as of newer) + streaming(after newer).
    assertEquals(ExpectedFeatures, fetchFeatures(fetcher))
    assertEquals(NewerBatchEndTs, cachedBatchEndTs(fetcher))

    // 2. Older snapshot replaces the newer one in the KV store; the fetcher still caches the newer serving info.
    uploadSnapshot(OlderBatchEndDate)

    // 3. Fetches must recover to batch(as of older) + streaming(after older) while the TTL entry is still live.
    val deadline = cachedAtMillis + ServingInfoTtlMillis / 2
    val (recovered, lastSeen) = pollUntilCorrect(fetcher, deadline)
    assertTrue(
      s"Fetcher kept serving $lastSeen instead of $ExpectedFeatures after an older snapshot upload. " +
        s"Cached batchEndTs=${cachedBatchEndTs(fetcher)} (KV store has $OlderBatchEndTs). " +
        s"It only recovers once the ${ServingInfoTtlMillis}ms serving info TTL expires.",
      recovered
    )
    assertEquals(OlderBatchEndTs, cachedBatchEndTs(fetcher))

    // 4. Re-uploading the newest snapshot (the incident mitigation). The forward direction is safe even with stale
    //    serving info: the newer batch IR passes the batchEndTs filter and batchResponseMaxTs dedupes streaming rows.
    //    The cached serving info should still move forward without waiting for TTL.
    uploadSnapshot(NewerBatchEndDate)
    val forwardDeadline = System.currentTimeMillis() + ServingInfoTtlMillis / 2
    while (cachedBatchEndTs(fetcher) != NewerBatchEndTs && System.currentTimeMillis() < forwardDeadline) {
      assertEquals(ExpectedFeatures, fetchFeatures(fetcher))
      Thread.sleep(PollIntervalMillis)
    }
    assertEquals(NewerBatchEndTs, cachedBatchEndTs(fetcher))
    assertEquals(ExpectedFeatures, fetchFeatures(fetcher))
  }

  @Test
  def testOlderSnapshotUploadServesCorrectFeaturesAfterServingInfoTtlExpires(): Unit = {
    val fetcher = new ShortTtlFetcher(kvStore, ServingInfoTtlMillis, ServingInfoRefreshIntervalMillis)
    assertEquals(ExpectedFeatures, fetchFeatures(fetcher))

    uploadSnapshot(OlderBatchEndDate)

    // Wait out the TTL. Expiry triggers an async reload, so keep polling briefly after it.
    Thread.sleep(ServingInfoTtlMillis + ServingInfoRefreshIntervalMillis)
    val (recovered, lastSeen) = pollUntilCorrect(fetcher, System.currentTimeMillis() + 2000L)
    assertTrue(s"Fetcher served $lastSeen even after the serving info TTL expired", recovered)
    assertEquals(OlderBatchEndTs, cachedBatchEndTs(fetcher))
  }

  /** Mimics a GroupBy batch upload for `batchEndDate`: replaces the whole batch dataset (IRs + serving info). */
  private def uploadSnapshot(batchEndDate: String): Unit = {
    val servingInfo = servingInfoFor(batchEndDate)
    val aggregator = servingInfo.aggregator
    val batchIr = AllReviews
      .filter(_.tsMillis < servingInfo.batchEndTsMillis)
      .foldLeft(aggregator.init) { (ir, review) =>
        aggregator.update(ir, new ArrayRow(Array[Any](review.rating), review.tsMillis))
      }
    val finalBatchIr = aggregator.normalizeBatchIr(batchIr)
    val irBytes = AvroConversions.encodeBytes(servingInfo.irChrononSchema)(
      Array[Any](finalBatchIr.collapsed, finalBatchIr.tailHops))

    val servingInfoJson = ThriftJsonCodec.toJsonStr(servingInfo.groupByServingInfo)
    kvStore.replaceDataset(
      groupByOps.batchDataset,
      Seq(
        PutRequest(keyBytes(servingInfo), irBytes, groupByOps.batchDataset, Some(servingInfo.batchEndTsMillis)),
        PutRequest(
          Constants.GroupByServingInfoKey.getBytes(Constants.UTF8),
          servingInfoJson.getBytes(Constants.UTF8),
          groupByOps.batchDataset,
          Some(servingInfo.batchEndTsMillis)
        )
      )
    )
  }

  /** Mimics the streaming job writing raw events to the streaming dataset. */
  private def writeStreamingEvents(reviews: Seq[Review]): Unit = {
    val servingInfo = servingInfoFor(NewerBatchEndDate)
    val puts = reviews.map { review =>
      PutRequest(keyBytes(servingInfo),
                 servingInfo.valueAvroCodec.encodeArray(Array[Any](review.rating)),
                 groupByOps.streamingDataset,
                 Some(review.tsMillis))
    }
    Await.result(kvStore.multiPut(puts), 5.seconds)
  }

  private def keyBytes(servingInfo: GroupByServingInfoParsed): Array[Byte] =
    servingInfo.keyCodec.encode(Map(ListingIdColumn -> ListingId))

  private def fetchFeatures(fetcher: FetcherBase): Map[String, AnyRef] = {
    val request = Request(GroupByName, Map(ListingIdColumn -> ListingId), Some(QueryTs))
    Await.result(fetcher.fetchGroupBys(Seq(request)), 5.seconds).head.values.get
  }

  private def cachedBatchEndTs(fetcher: FetcherBase): Long =
    fetcher.getGroupByServingInfo(GroupByName).get.batchEndTsMillis

  private def pollUntilCorrect(fetcher: FetcherBase, deadlineMillis: Long): (Boolean, Map[String, AnyRef]) = {
    var lastSeen = fetchFeatures(fetcher)
    while (lastSeen != ExpectedFeatures && System.currentTimeMillis() < deadlineMillis) {
      Thread.sleep(PollIntervalMillis)
      lastSeen = fetchFeatures(fetcher)
    }
    (lastSeen == ExpectedFeatures, lastSeen)
  }
}

object OlderSnapshotUploadFetchTest {
  private val GroupByName = "reviews.listing_ratings.v1"
  private val ListingIdColumn = "listing_id"
  private val RatingColumn = "rating"
  private val ListingId: AnyRef = Long.box(42L)

  private val partitionSpec = PartitionSpec(format = "yyyy-MM-dd", spanMillis = WindowUtils.Day.millis)
  private val OlderBatchEndDate = "2026-10-31"
  private val NewerBatchEndDate = "2026-11-02"
  private val OlderBatchEndTs = partitionSpec.epochMillis(OlderBatchEndDate)
  private val NewerBatchEndTs = partitionSpec.epochMillis(NewerBatchEndDate)
  private val HourMillis = WindowUtils.Hour.millis
  private val QueryTs = NewerBatchEndTs + 6 * HourMillis

  case class Review(rating: Long, tsMillis: Long)

  // Reviews landing before the older snapshot, between the two snapshots, and after the newer snapshot.
  private val AllReviews: Seq[Review] = Seq(
    Review(5L, OlderBatchEndTs - 30 * HourMillis),
    Review(4L, OlderBatchEndTs - 10 * HourMillis),
    Review(5L, OlderBatchEndTs - 1 * HourMillis),
    Review(3L, OlderBatchEndTs + 5 * HourMillis),
    Review(5L, NewerBatchEndTs - 2 * HourMillis),
    Review(4L, NewerBatchEndTs + 1 * HourMillis)
  )

  // Every review before the query time counts, regardless of which snapshot is live.
  private val ExpectedFeatures: Map[String, AnyRef] = Map(
    s"${RatingColumn}_count" -> Long.box(AllReviews.size.toLong),
    s"${RatingColumn}_sum" -> Long.box(AllReviews.map(_.rating).sum)
  )

  private val groupByConf: GroupBy = Builders.GroupBy(
    metaData = Builders.MetaData(name = GroupByName, team = "reviews"),
    sources = Seq(
      Builders.Source.events(
        query = Builders.Query(selects = Builders.Selects(ListingIdColumn, RatingColumn), timeColumn = "ts"),
        table = "reviews.review_events",
        topic = "reviews_review_events"
      )),
    keyColumns = Seq(ListingIdColumn),
    aggregations = Seq(
      Builders.Aggregation(operation = Operation.COUNT, inputColumn = RatingColumn),
      Builders.Aggregation(operation = Operation.SUM, inputColumn = RatingColumn)
    ),
    accuracy = Accuracy.TEMPORAL
  )
  private val groupByOps = new GroupByOps(groupByConf)

  private def servingInfoFor(batchEndDate: String): GroupByServingInfoParsed = {
    val keySchema = StructType("Key", Array(StructField(ListingIdColumn, LongType)))
    val selectedSchema = StructType("Value", Array(StructField(RatingColumn, LongType)))
    val inputSchema = StructType(
      "Input",
      Array(StructField(ListingIdColumn, LongType), StructField(RatingColumn, LongType), StructField("ts", LongType)))
    val servingInfo = new GroupByServingInfo()
    servingInfo.setGroupBy(groupByConf)
    servingInfo.setBatchEndDate(batchEndDate)
    servingInfo.setKeyAvroSchema(AvroConversions.fromChrononSchema(keySchema).toString(true))
    servingInfo.setSelectedAvroSchema(AvroConversions.fromChrononSchema(selectedSchema).toString(true))
    servingInfo.setInputAvroSchema(AvroConversions.fromChrononSchema(inputSchema).toString(true))
    new GroupByServingInfoParsed(servingInfo, partitionSpec)
  }

  /** FetcherBase whose GroupByServingInfo cache loads from the KV store like MetadataStore, but with a short TTL. */
  class ShortTtlFetcher(kvStore: KVStore, ttlMillis: Long, refreshIntervalMillis: Long) extends FetcherBase(kvStore) {
    override lazy val getGroupByServingInfo: TTLCache[String, Try[GroupByServingInfoParsed]] =
      new TTLCache[String, Try[GroupByServingInfoParsed]](
        { name =>
          val batchDataset = s"${name.sanitize.toUpperCase()}_BATCH"
          kvStore
            .getString(Constants.GroupByServingInfoKey, batchDataset, timeoutMillis = 5000L)
            .map { json =>
              val info =
                ThriftJsonCodec.fromJsonStr[GroupByServingInfo](json, check = true, classOf[GroupByServingInfo])
              new GroupByServingInfoParsed(info, partitionSpec)
            }
        },
        { name => Metrics.Context(environment = "test", groupBy = name) },
        ttlMillis = ttlMillis,
        refreshIntervalMillis = refreshIntervalMillis
      )
  }

  /** Minimal versioned KV store. `replaceDataset` mirrors bulk load replace semantics for batch uploads. */
  class InMemoryServingKvStore extends KVStore {
    private val datasets = new ConcurrentHashMap[String, ConcurrentHashMap[String, Seq[TimedValue]]]()
    private def encode(bytes: Array[Byte]): String = Base64.getEncoder.encodeToString(bytes)

    override def create(dataset: String): Unit =
      datasets.putIfAbsent(dataset, new ConcurrentHashMap[String, Seq[TimedValue]]())

    override def multiGet(requests: Seq[GetRequest]): Future[Seq[GetResponse]] =
      Future {
        requests.map { request =>
          val values = Try {
            val stored = Option(datasets.get(request.dataset))
              .flatMap(table => Option(table.get(encode(request.keyBytes))))
              .getOrElse(throw new NoSuchElementException(s"No value for key in ${request.dataset}"))
            stored.filter(value => request.afterTsMillis.forall(value.millis >= _)).sortBy(_.millis)
          }
          GetResponse(request, values)
        }
      }

    override def multiPut(putRequests: Seq[PutRequest]): Future[Seq[Boolean]] = {
      putRequests.foreach { put =>
        datasets
          .get(put.dataset)
          .compute(
            encode(put.keyBytes),
            new BiFunction[String, Seq[TimedValue], Seq[TimedValue]] {
              override def apply(key: String, existing: Seq[TimedValue]): Seq[TimedValue] =
                Option(existing).getOrElse(Seq.empty) :+ TimedValue(put.valueBytes, put.tsMillis.get)
            }
          )
      }
      Future.successful(putRequests.map(_ => true))
    }

    override def bulkPut(sourceOfflineTable: String, destinationOnlineDataSet: String, partition: String): Unit =
      throw new UnsupportedOperationException("use replaceDataset")

    def replaceDataset(dataset: String, rows: Seq[PutRequest]): Unit = {
      val table = new ConcurrentHashMap[String, Seq[TimedValue]]()
      rows.foreach(row => table.put(encode(row.keyBytes), Seq(TimedValue(row.valueBytes, row.tsMillis.get))))
      datasets.put(dataset, table)
    }
  }
}
