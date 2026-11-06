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
import ai.chronon.online.FetcherCache.{BatchIrCache, CachedBatchResponse}
import ai.chronon.online.KVStore.{GetRequest, GetResponse, PutRequest, TimedValue}
import ai.chronon.online.serde.{ArrayRow, AvroConversions}
import org.junit.Assert.assertEquals
import org.junit.{Before, Test}

import java.util.Base64
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong
import java.util.function.BiFunction
import scala.collection.Seq
import scala.concurrent.duration.DurationInt
import scala.concurrent.{Await, Future}
import scala.util.Try

/**
  * End to end online fetch of a temporally accurate (batch + streaming) GroupBy across batch uploads that land while
  * the fetcher still has the previous GroupByServingInfo in its TTL cache. In particular an OLDER batch snapshot
  * uploaded on top of a NEWER one, i.e. a rollback (or an on-call re-running the upload for an old ds).
  *
  * The upload uses replace semantics, so the KV store ends up with:
  *   - batch IRs as of the older batchEndDate
  *   - GroupByServingInfo whose batchEndDate is the older date
  *
  * Every fetcher host still has the NEWER GroupByServingInfo in its TTL cache (2 hours in production). Before this
  * was handled, the stale batchEndTs made the fetcher drop the (older) batch IR as outdated and only scan streaming
  * data after the newer batchEndTs: features collapsed to the streaming tail (close to zero), or to null when a key
  * had no streaming data after the newer date, until the cache entry was refreshed.
  *
  * Now the batch value's own timestamp is the batch end the fetcher serves as of, and streaming data is re-read from
  * there when it is earlier than the cached one, so the very first fetch after the upload is already correct. The
  * serving info is refreshed as well so the extra streaming read stops after a few seconds.
  *
  * The TTL cache runs on a fake clock here, so neither the TTL nor the refresh interval can expire unless a test
  * advances the clock: what each fetch is served from is deterministic.
  *
  * Rollback envelope exercised here: windowed features (`rating_count_7d`) are exact while the query time is within
  * the aggregator's 2 day tail buffer of the rolled back batch end, which is the case for the query time used below.
  */
class OlderSnapshotUploadFetchTest {
  import OlderSnapshotUploadFetchTest._

  private val ServingInfoTtlMillis = 2 * 60 * 60 * 1000L // production value
  private val ServingInfoRefreshIntervalMillis = 8 * 1000L // production value
  private val ConvergenceTimeoutMillis = 5000L

  private var kvStore: InMemoryServingKvStore = _
  private var clock: AtomicLong = _

  @Before
  def setup(): Unit = {
    kvStore = new InMemoryServingKvStore
    kvStore.create(groupByOps.batchDataset)
    kvStore.create(groupByOps.streamingDataset)
    clock = new AtomicLong(0L)

    // Initial state: newest snapshot is live, streaming job has written every review event.
    uploadSnapshot(NewerBatchEndDate)
    writeStreamingEvents(AllReviews)
  }

  private def newFetcher(batchIrCache: Option[BatchIrCache] = None): FetcherBase =
    new FakeClockFetcher(kvStore,
                         ServingInfoTtlMillis,
                         ServingInfoRefreshIntervalMillis,
                         () => clock.get(),
                         batchIrCache)

  @Test
  def testOlderSnapshotUploadServesCorrectFeaturesOnTheFirstFetch(): Unit = {
    val fetcher = newFetcher()

    // 1. Newer snapshot is live and cached on the fetcher: batch(as of newer) + streaming(after newer).
    assertAllListingsCorrect(fetcher)
    assertEquals(NewerBatchEndTs, cachedBatchEndTs(fetcher))

    // 2. Older snapshot replaces the newer one in the KV store; the fetcher still caches the newer serving info and
    //    cannot refresh it: the clock hasn't moved past the refresh interval.
    uploadSnapshot(OlderBatchEndDate)
    assertEquals(NewerBatchEndTs, cachedBatchEndTs(fetcher))

    // 3. The very first fetches are correct: batch(as of older) + streaming(re-read from the older batch end).
    //    ListingWithoutStreamingTail is the key that used to come back null (no streaming data after the newer date).
    assertAllListingsCorrect(fetcher)
    assertEquals(NewerBatchEndTs, cachedBatchEndTs(fetcher))

    // 4. Once the refresh interval has passed, a fetch moves the cached serving info back to the older batch end,
    //    long before the TTL would, and fetches stay correct.
    clock.addAndGet(ServingInfoRefreshIntervalMillis + 1)
    assertAllListingsCorrect(fetcher)
    awaitCachedBatchEndTs(fetcher, OlderBatchEndTs)
    assertAllListingsCorrect(fetcher)

    // 5. Re-uploading the newest snapshot (rolling forward again) is correct on the first fetch too: the newer batch
    //    IR is served as of its own batch end, and the streaming rows it already covers are filtered out.
    uploadSnapshot(NewerBatchEndDate)
    assertAllListingsCorrect(fetcher)
    assertEquals(OlderBatchEndTs, cachedBatchEndTs(fetcher))
    clock.addAndGet(ServingInfoRefreshIntervalMillis + 1)
    assertAllListingsCorrect(fetcher)
    awaitCachedBatchEndTs(fetcher, NewerBatchEndTs)
    assertAllListingsCorrect(fetcher)
  }

  @Test
  def testOlderSnapshotUploadServesCorrectFeaturesAfterServingInfoTtlExpires(): Unit = {
    val fetcher = newFetcher()
    assertAllListingsCorrect(fetcher)

    uploadSnapshot(OlderBatchEndDate)

    // Expiry triggers an async reload on the next access.
    clock.addAndGet(ServingInfoTtlMillis + 1)
    awaitCachedBatchEndTs(fetcher, OlderBatchEndTs)
    assertAllListingsCorrect(fetcher)
  }

  @Test
  def testBatchIrCacheServesCorrectFeaturesThroughForwardAndRollbackUploads(): Unit = {
    val batchIrCache = new BatchIrCache("test_batch_ir_cache", 100)
    val fetcher = newFetcher(Some(batchIrCache))

    // Start from the older snapshot so the fetcher caches the OLDER serving info and the older IR of one listing.
    uploadSnapshot(OlderBatchEndDate)
    assertEquals(ExpectedFeatures(ListingWithStreamingTail), fetchFeatures(fetcher, ListingWithStreamingTail))
    assertEquals(OlderBatchEndTs, cachedBatchEndTs(fetcher))
    assertEquals(OlderBatchEndTs, cachedIrBatchEndTs(fetcher, batchIrCache, ListingWithStreamingTail))

    // Forward upload while the older serving info is still cached. The other listing's IR is a cache miss: the newer
    // IR gets cached under the older serving info's key, carrying its own (newer) batch end. The following cache hit
    // must serve as of that newer batch end, otherwise the streaming rows between the two batch ends count twice.
    uploadSnapshot(NewerBatchEndDate)
    assertEquals(ExpectedFeatures(ListingWithoutStreamingTail), fetchFeatures(fetcher, ListingWithoutStreamingTail))
    assertEquals(OlderBatchEndTs, cachedBatchEndTs(fetcher))
    assertEquals(NewerBatchEndTs, cachedIrBatchEndTs(fetcher, batchIrCache, ListingWithoutStreamingTail))
    assertEquals(ExpectedFeatures(ListingWithoutStreamingTail), fetchFeatures(fetcher, ListingWithoutStreamingTail))
    // A cache hit on the older IR is still served correctly, as of the older batch end.
    assertEquals(ExpectedFeatures(ListingWithStreamingTail), fetchFeatures(fetcher, ListingWithStreamingTail))

    // Let the serving info catch up with the newer snapshot; the cache keys move with it.
    clock.addAndGet(ServingInfoRefreshIntervalMillis + 1)
    assertAllListingsCorrect(fetcher)
    awaitCachedBatchEndTs(fetcher, NewerBatchEndTs)
    assertAllListingsCorrect(fetcher)
    assertEquals(NewerBatchEndTs, cachedIrBatchEndTs(fetcher, batchIrCache, ListingWithStreamingTail))

    // Rollback while the newer serving info is cached. Cache hits keep serving the newer IR (correct, since it is
    // consistent with the streaming data), until the serving info refresh moves the key to the older batch end and
    // the older IR is read from the KV store and served as of its own batch end.
    uploadSnapshot(OlderBatchEndDate)
    assertAllListingsCorrect(fetcher)
    clock.addAndGet(ServingInfoRefreshIntervalMillis + 1)
    assertAllListingsCorrect(fetcher)
    awaitCachedBatchEndTs(fetcher, OlderBatchEndTs)
    assertAllListingsCorrect(fetcher)
    assertEquals(OlderBatchEndTs, cachedIrBatchEndTs(fetcher, batchIrCache, ListingWithStreamingTail))
    assertAllListingsCorrect(fetcher) // cache hits on the older IR
  }

  @Test
  def testBatchedFetchOfSeveralKeysIsCorrectOnTheFirstFetchAfterRollback(): Unit = {
    val fetcher = newFetcher()
    assertEquals(AllListings.map(ExpectedFeatures), fetchFeaturesBatched(fetcher, AllListings))

    uploadSnapshot(OlderBatchEndDate)

    // Both keys need their streaming data re-read in the same fetchGroupBys call; each response must be matched back
    // to its own request.
    assertEquals(AllListings.map(ExpectedFeatures), fetchFeaturesBatched(fetcher, AllListings))
    assertEquals(AllListings.reverse.map(ExpectedFeatures), fetchFeaturesBatched(fetcher, AllListings.reverse))
  }

  @Test
  def testSnapshotAccurateGroupByServesOlderValuesOnTheFirstFetchAfterRollback(): Unit = {
    val fetcher = newFetcher()
    uploadSnapshotGroupBy(NewerBatchEndDate)
    assertEquals(ExpectedSnapshotFeatures(ListingWithStreamingTail, NewerBatchEndTs),
                 fetchFeatures(fetcher, ListingWithStreamingTail, SnapshotGroupByName))

    // No streaming data to reconcile with: the older values are simply served. This used to come back null.
    uploadSnapshotGroupBy(OlderBatchEndDate)
    assertEquals(ExpectedSnapshotFeatures(ListingWithStreamingTail, OlderBatchEndTs),
                 fetchFeatures(fetcher, ListingWithStreamingTail, SnapshotGroupByName))
  }

  @Test
  def testStreamingReReadFailureDegradesOnlyTheAffectedRequestInsteadOfFailing(): Unit = {
    val fetcher = newFetcher()
    assertAllListingsCorrect(fetcher)

    uploadSnapshot(OlderBatchEndDate)
    kvStore.failReadsScanningBefore = Some(NewerBatchEndTs)

    // The re-read from the older batch end fails. The request is still answered, from the older batch IR plus the
    // streaming rows already fetched (after the newer batch end): the rows in between are missing until the serving
    // info refresh moves the scan start back. The failure is counted on the request's metrics context.
    val degraded = fetchFeatures(fetcher, ListingWithStreamingTail)
    val reviews = AllReviews.filter(_.listingId == ListingWithStreamingTail)
    val servedCount =
      reviews.count(r => r.tsMillis < OlderBatchEndTs || (r.tsMillis >= NewerBatchEndTs && r.tsMillis < QueryTs))
    assertEquals(Long.box(servedCount.toLong), degraded(s"${RatingColumn}_count"))

    // Once the serving info has caught up no re-read is needed and the fetch is correct again.
    kvStore.failReadsScanningBefore = None
    clock.addAndGet(ServingInfoRefreshIntervalMillis + 1)
    fetchFeatures(fetcher, ListingWithStreamingTail)
    awaitCachedBatchEndTs(fetcher, OlderBatchEndTs)
    assertAllListingsCorrect(fetcher)
  }

  @Test
  def testKeyWithoutBatchRowRecoversOnlyAfterServingInfoRefresh(): Unit = {
    // A listing whose first review landed between the two snapshots: it has a batch row in the newer snapshot only.
    val bornBetween = 44L
    val reviews = Seq(Review(bornBetween, 5L, OlderBatchEndTs + 2 * HourMillis),
                      Review(bornBetween, 4L, NewerBatchEndTs + 2 * HourMillis))
    uploadSnapshot(NewerBatchEndDate, AllReviews ++ reviews)
    writeStreamingEvents(reviews)
    val fetcher = newFetcher()
    val expected = Map(s"${RatingColumn}_count" -> Long.box(2L),
                       s"${RatingColumn}_sum" -> Long.box(9L),
                       s"${RatingColumn}_count_${WindowDays}d" -> Long.box(2L))
    assertEquals(expected, fetchFeatures(fetcher, bornBetween))

    // After the rollback there is no batch row for this key at all, so nothing tells the fetcher the batch end moved:
    // only the streaming tail after the cached (newer) batch end is served until the serving info is refreshed.
    // Known residual gap of the batch-end-from-batch-value approach; keys with a batch row are correct right away.
    uploadSnapshot(OlderBatchEndDate, AllReviews ++ reviews)
    assertEquals(Long.box(1L), fetchFeatures(fetcher, bornBetween)(s"${RatingColumn}_count"))

    clock.addAndGet(ServingInfoRefreshIntervalMillis + 1)
    fetchFeatures(fetcher, ListingWithStreamingTail) // a key with a batch row triggers the refresh
    awaitCachedBatchEndTs(fetcher, OlderBatchEndTs)
    assertEquals(expected, fetchFeatures(fetcher, bornBetween))
  }

  /** Mimics a batch upload of the snapshot accurate GroupBy: finalized values encoded with the output codec. */
  private def uploadSnapshotGroupBy(batchEndDate: String): Unit = {
    val servingInfo = servingInfoFor(batchEndDate, snapshotGroupByConf)
    val rows = AllReviews.groupBy(_.listingId).toSeq.map {
      case (listingId, reviews) =>
        val expected = ExpectedSnapshotFeatures(listingId, servingInfo.batchEndTsMillis)
        val values = servingInfo.outputCodec.encodeArray(
          Array[Any](expected(s"${RatingColumn}_count"), expected(s"${RatingColumn}_sum")))
        PutRequest(keyBytes(servingInfo, listingId),
                   values,
                   snapshotGroupByOps.batchDataset,
                   Some(servingInfo.batchEndTsMillis))
    }
    val servingInfoJson = ThriftJsonCodec.toJsonStr(servingInfo.groupByServingInfo)
    kvStore.replaceDataset(
      snapshotGroupByOps.batchDataset,
      rows :+ PutRequest(
        Constants.GroupByServingInfoKey.getBytes(Constants.UTF8),
        servingInfoJson.getBytes(Constants.UTF8),
        snapshotGroupByOps.batchDataset,
        Some(servingInfo.batchEndTsMillis)
      )
    )
  }

  private def fetchFeaturesBatched(fetcher: FetcherBase, listingIds: Seq[Long]): Seq[Map[String, AnyRef]] = {
    val requests = listingIds.map(id => Request(GroupByName, Map(ListingIdColumn -> Long.box(id)), Some(QueryTs)))
    Await.result(fetcher.fetchGroupBys(requests), 5.seconds).map(_.values.get)
  }

  /** Mimics a GroupBy batch upload for `batchEndDate`: replaces the whole batch dataset (IRs + serving info). */
  private def uploadSnapshot(batchEndDate: String, reviews: Seq[Review] = AllReviews): Unit = {
    val servingInfo = servingInfoFor(batchEndDate)
    val aggregator = servingInfo.aggregator
    // Like GroupByUpload, only keys with events before the batch end get a row.
    val irRows = reviews.filter(_.tsMillis < servingInfo.batchEndTsMillis).groupBy(_.listingId).toSeq.map {
      case (listingId, reviews) =>
        val batchIr = reviews
          .foldLeft(aggregator.init) { (ir, review) =>
            aggregator.update(ir, new ArrayRow(Array[Any](review.rating), review.tsMillis))
          }
        val finalBatchIr = aggregator.normalizeBatchIr(batchIr)
        val irBytes = AvroConversions.encodeBytes(servingInfo.irChrononSchema)(
          Array[Any](finalBatchIr.collapsed, finalBatchIr.tailHops))
        PutRequest(keyBytes(servingInfo, listingId),
                   irBytes,
                   groupByOps.batchDataset,
                   Some(servingInfo.batchEndTsMillis))
    }

    val servingInfoJson = ThriftJsonCodec.toJsonStr(servingInfo.groupByServingInfo)
    kvStore.replaceDataset(
      groupByOps.batchDataset,
      irRows :+ PutRequest(
        Constants.GroupByServingInfoKey.getBytes(Constants.UTF8),
        servingInfoJson.getBytes(Constants.UTF8),
        groupByOps.batchDataset,
        Some(servingInfo.batchEndTsMillis)
      )
    )
  }

  /** Mimics the streaming job writing raw events to the streaming dataset. */
  private def writeStreamingEvents(reviews: Seq[Review]): Unit = {
    val servingInfo = servingInfoFor(NewerBatchEndDate)
    val puts = reviews.map { review =>
      PutRequest(
        keyBytes(servingInfo, review.listingId),
        servingInfo.valueAvroCodec.encodeArray(Array[Any](review.rating)),
        groupByOps.streamingDataset,
        Some(review.tsMillis)
      )
    }
    Await.result(kvStore.multiPut(puts), 5.seconds)
  }

  private def keyBytes(servingInfo: GroupByServingInfoParsed, listingId: Long): Array[Byte] =
    servingInfo.keyCodec.encode(Map(ListingIdColumn -> Long.box(listingId)))

  private def fetchFeatures(fetcher: FetcherBase,
                            listingId: Long,
                            groupByName: String = GroupByName): Map[String, AnyRef] = {
    val request = Request(groupByName, Map(ListingIdColumn -> Long.box(listingId)), Some(QueryTs))
    Await.result(fetcher.fetchGroupBys(Seq(request)), 5.seconds).head.values.get
  }

  private def assertAllListingsCorrect(fetcher: FetcherBase): Unit =
    AllListings.foreach { listingId =>
      assertEquals(s"listing $listingId (cached batchEndTs=${cachedBatchEndTs(fetcher)})",
                   ExpectedFeatures(listingId),
                   fetchFeatures(fetcher, listingId))
    }

  private def cachedBatchEndTs(fetcher: FetcherBase): Long =
    fetcher.getGroupByServingInfo(GroupByName).get.batchEndTsMillis

  /**
    * The batch end carried by the IR cached for `listingId` under the key the fetcher is currently using, i.e. the
    * one with the cached serving info's batch end. (Entries under previous keys stay in the LRU until evicted.)
    */
  private def cachedIrBatchEndTs(fetcher: FetcherBase, batchIrCache: BatchIrCache, listingId: Long): Long = {
    val key =
      BatchIrCache.Key(groupByOps.batchDataset, Map(ListingIdColumn -> Long.box(listingId)), cachedBatchEndTs(fetcher))
    batchIrCache.cache.getIfPresent(key) match {
      case cached: CachedBatchResponse => cached.batchEndTsMillis
      case other                       => throw new AssertionError(s"no cached IR for listing $listingId under $key: $other")
    }
  }

  /** Serving info refreshes are asynchronous; wait (briefly) for one that a fetch has already triggered to land. */
  private def awaitCachedBatchEndTs(fetcher: FetcherBase, expected: Long): Unit = {
    val deadline = System.currentTimeMillis() + ConvergenceTimeoutMillis
    while (cachedBatchEndTs(fetcher) != expected && System.currentTimeMillis() < deadline) {
      Thread.sleep(10)
    }
    assertEquals("cached serving info batchEndTs", expected, cachedBatchEndTs(fetcher))
  }
}

object OlderSnapshotUploadFetchTest {
  private val GroupByName = "reviews.listing_ratings.v1"
  private val ListingIdColumn = "listing_id"
  private val RatingColumn = "rating"

  private val partitionSpec = PartitionSpec(format = "yyyy-MM-dd", spanMillis = WindowUtils.Day.millis)
  private val OlderBatchEndDate = "2026-11-01"
  private val NewerBatchEndDate = "2026-11-02"
  private val OlderBatchEndTs = partitionSpec.epochMillis(OlderBatchEndDate)
  private val NewerBatchEndTs = partitionSpec.epochMillis(NewerBatchEndDate)
  private val HourMillis = WindowUtils.Hour.millis
  private val DayMillis = WindowUtils.Day.millis
  // After the newer batch end, and within the 2 day tail buffer of the older one (see the class doc).
  private val QueryTs = NewerBatchEndTs + 6 * HourMillis
  private val WindowDays = 7

  case class Review(listingId: Long, rating: Long, tsMillis: Long)

  // A listing with reviews before the older snapshot, between the two snapshots, and after the newer snapshot.
  private val ListingWithStreamingTail = 42L
  // A listing whose last review landed between the two snapshots: nothing in the streaming data after the newer
  // batch end. This is the key that came back null during the incident.
  private val ListingWithoutStreamingTail = 43L
  private val AllListings = Seq(ListingWithStreamingTail, ListingWithoutStreamingTail)

  private val AllReviews: Seq[Review] = Seq(
    Review(ListingWithStreamingTail, 2L, OlderBatchEndTs - 10 * DayMillis), // outside the 7 day window at QueryTs
    Review(ListingWithStreamingTail, 5L, OlderBatchEndTs - 30 * HourMillis),
    Review(ListingWithStreamingTail, 4L, OlderBatchEndTs - 10 * HourMillis),
    Review(ListingWithStreamingTail, 5L, OlderBatchEndTs - 1 * HourMillis),
    Review(ListingWithStreamingTail, 3L, OlderBatchEndTs + 5 * HourMillis),
    Review(ListingWithStreamingTail, 5L, NewerBatchEndTs - 2 * HourMillis),
    Review(ListingWithStreamingTail, 4L, NewerBatchEndTs + 1 * HourMillis),
    Review(ListingWithoutStreamingTail, 1L, OlderBatchEndTs - 20 * HourMillis),
    Review(ListingWithoutStreamingTail, 2L, OlderBatchEndTs - 2 * HourMillis),
    Review(ListingWithoutStreamingTail, 3L, OlderBatchEndTs + 3 * HourMillis)
  )

  // Every review before the query time counts, regardless of which snapshot is live.
  private def ExpectedFeatures(listingId: Long): Map[String, AnyRef] = {
    val reviews = AllReviews.filter(r => r.listingId == listingId && r.tsMillis < QueryTs)
    val inWindow = reviews.filter(_.tsMillis >= QueryTs - WindowDays * DayMillis)
    Map(
      s"${RatingColumn}_count" -> Long.box(reviews.size.toLong),
      s"${RatingColumn}_sum" -> Long.box(reviews.map(_.rating).sum),
      s"${RatingColumn}_count_${WindowDays}d" -> Long.box(inWindow.size.toLong)
    )
  }

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
      Builders.Aggregation(operation = Operation.SUM, inputColumn = RatingColumn),
      Builders.Aggregation(operation = Operation.COUNT,
                           inputColumn = RatingColumn,
                           windows = Seq(new Window(WindowDays, TimeUnit.DAYS)))
    ),
    accuracy = Accuracy.TEMPORAL
  )
  private val groupByOps = new GroupByOps(groupByConf)

  // Snapshot accurate variant: served from the batch value alone, no streaming data.
  private val SnapshotGroupByName = "reviews.listing_ratings_snapshot.v1"
  private val snapshotGroupByConf: GroupBy = Builders.GroupBy(
    metaData = Builders.MetaData(name = SnapshotGroupByName, team = "reviews"),
    sources = Seq(
      Builders.Source.events(
        query = Builders.Query(selects = Builders.Selects(ListingIdColumn, RatingColumn), timeColumn = "ts"),
        table = "reviews.review_events"
      )),
    keyColumns = Seq(ListingIdColumn),
    aggregations = Seq(
      Builders.Aggregation(operation = Operation.COUNT, inputColumn = RatingColumn),
      Builders.Aggregation(operation = Operation.SUM, inputColumn = RatingColumn)
    ),
    accuracy = Accuracy.SNAPSHOT
  )
  private val snapshotGroupByOps = new GroupByOps(snapshotGroupByConf)

  private def ExpectedSnapshotFeatures(listingId: Long, batchEndTs: Long): Map[String, AnyRef] = {
    val reviews = AllReviews.filter(r => r.listingId == listingId && r.tsMillis < batchEndTs)
    Map(
      s"${RatingColumn}_count" -> Long.box(reviews.size.toLong),
      s"${RatingColumn}_sum" -> Long.box(reviews.map(_.rating).sum)
    )
  }

  private def servingInfoFor(batchEndDate: String, conf: GroupBy = groupByConf): GroupByServingInfoParsed = {
    val keySchema = StructType("Key", Array(StructField(ListingIdColumn, LongType)))
    val selectedSchema = StructType("Value", Array(StructField(RatingColumn, LongType)))
    val inputSchema = StructType(
      "Input",
      Array(StructField(ListingIdColumn, LongType), StructField(RatingColumn, LongType), StructField("ts", LongType)))
    val servingInfo = new GroupByServingInfo()
    servingInfo.setGroupBy(conf)
    servingInfo.setBatchEndDate(batchEndDate)
    servingInfo.setKeyAvroSchema(AvroConversions.fromChrononSchema(keySchema).toString(true))
    servingInfo.setSelectedAvroSchema(AvroConversions.fromChrononSchema(selectedSchema).toString(true))
    servingInfo.setInputAvroSchema(AvroConversions.fromChrononSchema(inputSchema).toString(true))
    new GroupByServingInfoParsed(servingInfo, partitionSpec)
  }

  /**
    * FetcherBase whose GroupByServingInfo cache loads from the KV store like MetadataStore, but runs on the given
    * clock, and optionally with the batch IR cache enabled for every GroupBy.
    */
  class FakeClockFetcher(kvStore: KVStore,
                         ttlMillis: Long,
                         refreshIntervalMillis: Long,
                         nowFunc: () => Long,
                         batchIrCache: Option[BatchIrCache])
      extends FetcherBase(kvStore) {
    override val maybeBatchIrCache: Option[BatchIrCache] = batchIrCache
    override def isCachingEnabled(groupBy: GroupBy): Boolean = batchIrCache.isDefined

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
        nowFunc = nowFunc,
        refreshIntervalMillis = refreshIntervalMillis
      )
  }

  /** Minimal versioned KV store. `replaceDataset` mirrors bulk load replace semantics for batch uploads. */
  class InMemoryServingKvStore extends KVStore {
    private val datasets = new ConcurrentHashMap[String, ConcurrentHashMap[String, Seq[TimedValue]]]()
    // When set, any multiGet that scans from before this ts fails: simulates the streaming re-read failing.
    @volatile var failReadsScanningBefore: Option[Long] = None
    private def encode(bytes: Array[Byte]): String = Base64.getEncoder.encodeToString(bytes)

    override def create(dataset: String): Unit =
      datasets.putIfAbsent(dataset, new ConcurrentHashMap[String, Seq[TimedValue]]())

    override def multiGet(requests: Seq[GetRequest]): Future[Seq[GetResponse]] =
      if (failReadsScanningBefore.exists(ts => requests.exists(_.afterTsMillis.exists(_ < ts)))) {
        Future.failed(new RuntimeException("simulated KV store failure on streaming re-read"))
      } else
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
