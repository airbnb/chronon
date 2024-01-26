package ai.chronon.online

import ai.chronon.aggregator.windowing.FinalBatchIr
import ai.chronon.api.Extensions.MetadataOps
import ai.chronon.api.GroupBy
import ai.chronon.online.BaseFetcher.GroupByRequestMeta
import ai.chronon.online.Fetcher.Request
import ai.chronon.online.FetcherCache.{
  BatchIrCache,
  BatchResponses,
  CachedBatchResponse,
  CachedFinalIrBatchResponse,
  CachedMapBatchResponse,
  KvStoreBatchResponse
}
import ai.chronon.online.KVStore.{GetRequest, TimedValue}
import com.github.benmanes.caffeine.cache.{Cache => CaffeineCache}

import scala.util.{Success, Try}
import java.util.concurrent.ConcurrentHashMap
import scala.collection.JavaConverters.mapAsScalaConcurrentMapConverter

/*
 * FetcherCache is an extension to BaseFetcher that provides caching functionality. It caches KV store
 * requests to decrease feature serving latency.
 * */
trait FetcherCache {
  val batchIrCacheName = "batch_cache"
  val maybeBatchIrCache: Option[BatchIrCache] =
    Config
      .getEnvConfig("ai.chronon.fetcher.batch_ir_cache_size")
      .map(size => new BatchIrCache(batchIrCacheName, size.toInt))
      .orElse(None)

  def isCacheSizeConfigured: Boolean = maybeBatchIrCache.isDefined

  // Memoize which GroupBys have caching enabled
  private[online] val isCachingEnabledForGroupBy: collection.concurrent.Map[String, Boolean] =
    new ConcurrentHashMap[String, Boolean]().asScala

  def isCachingEnabled(groupBy: GroupBy): Boolean = {
    if (!isCacheSizeConfigured || groupBy.getMetaData == null || groupBy.getMetaData.getName == null) return false

    val groupByName = groupBy.getMetaData.getName
    isCachingEnabledForGroupBy.getOrElse(
      groupByName, {
        groupBy.getMetaData.customJsonLookUp("enable_caching") match {
          case b: Boolean =>
            println(s"Caching is ${if (b) "enabled" else "disabled"} for $groupByName")
            isCachingEnabledForGroupBy.putIfAbsent(groupByName, b)
            b
          case null =>
            println(s"Caching is disabled for $groupByName, enable_caching is not set.")
            isCachingEnabledForGroupBy.putIfAbsent(groupByName, false)
            false
          case _ => false
        }
      }
    )
  }

  protected val caffeineMetricsContext: Metrics.Context = Metrics.Context(Metrics.Environment.JoinFetching)

  /**
    * Obtain the Map[String, AnyRef] response from a batch response.
    *
    * If batch IR caching is enabled, this method will try to fetch the IR from the cache. If it's not in the cache,
    * it will decode it from the batch bytes and store it.
    *
    * @param batchResponses the batch responses
    * @param batchBytes the batch bytes corresponding to the batchResponses. Can be `null`.
    * @param servingInfo the GroupByServingInfoParsed that contains the info to decode the bytes
    * @param decodingFunction the function to decode bytes into Map[String, AnyRef]
    * @param keys the keys used to fetch this particular batch response, for caching purposes
    */
  private[online] def getMapResponseFromBatchResponse(batchResponses: BatchResponses,
                                                      batchBytes: Array[Byte],
                                                      decodingFunction: Array[Byte] => Map[String, AnyRef],
                                                      servingInfo: GroupByServingInfoParsed,
                                                      keys: Map[String, Any]): Map[String, AnyRef] = {
    if (!isCachingEnabled(servingInfo.groupBy)) return decodingFunction(batchBytes)

    batchResponses match {
      case _: KvStoreBatchResponse =>
        val batchRequestCacheKey =
          BatchIrCache.Key(servingInfo.groupByOps.batchDataset, keys, servingInfo.batchEndTsMillis)
        val decodedBytes = decodingFunction(batchBytes)
        if (decodedBytes != null)
          maybeBatchIrCache.get.cache.put(batchRequestCacheKey, CachedMapBatchResponse(decodedBytes))
        decodedBytes
      case cachedResponse: CachedBatchResponse =>
        cachedResponse match {
          case CachedFinalIrBatchResponse(_: FinalBatchIr)              => decodingFunction(batchBytes)
          case CachedMapBatchResponse(mapResponse: Map[String, AnyRef]) => mapResponse
        }
    }
  }

  /**
    * Obtain the FinalBatchIr from a batch response.
    *
    * If batch IR caching is enabled, this method will try to fetch the IR from the cache. If it's not in the cache,
    * it will decode it from the batch bytes and store it.
    *
    * @param batchResponses the batch responses
    * @param batchBytes the batch bytes corresponding to the batchResponses. Can be `null`.
    * @param servingInfo the GroupByServingInfoParsed that contains the info to decode the bytes
    * @param decodingFunction the function to decode bytes into FinalBatchIr
    * @param keys the keys used to fetch this particular batch response, for caching purposes
    */
  private[online] def getBatchIrFromBatchResponse(
      batchResponses: BatchResponses,
      batchBytes: Array[Byte],
      servingInfo: GroupByServingInfoParsed,
      decodingFunction: (Array[Byte], GroupByServingInfoParsed) => FinalBatchIr,
      keys: Map[String, Any]): FinalBatchIr = {
    if (!isCachingEnabled(servingInfo.groupBy)) return decodingFunction(batchBytes, servingInfo)

    batchResponses match {
      case _: KvStoreBatchResponse =>
        val batchRequestCacheKey =
          BatchIrCache.Key(servingInfo.groupByOps.batchDataset, keys, servingInfo.batchEndTsMillis)
        val decodedBytes = decodingFunction(batchBytes, servingInfo)
        if (decodedBytes != null)
          maybeBatchIrCache.get.cache.put(batchRequestCacheKey, CachedFinalIrBatchResponse(decodedBytes))
        decodedBytes
      case cachedResponse: CachedBatchResponse =>
        cachedResponse match {
          case CachedFinalIrBatchResponse(finalBatchIr: FinalBatchIr) => finalBatchIr
          case CachedMapBatchResponse(_: Map[String, AnyRef])         => decodingFunction(batchBytes, servingInfo)
        }
    }
  }

  /**
    * Given a list of GetRequests, return a map of GetRequests to cached FinalBatchIrs.
    */
  def getCachedRequests(
      groupByRequestToKvRequest: Seq[(Request, Try[GroupByRequestMeta])]): Map[GetRequest, CachedBatchResponse] = {
    if (!isCacheSizeConfigured) return Map.empty

    groupByRequestToKvRequest
      .map {
        case (request, Success(GroupByRequestMeta(servingInfo, batchRequest, _, _, _))) =>
          if (!isCachingEnabled(servingInfo.groupBy)) { Map.empty }
          else {
            val batchRequestCacheKey =
              BatchIrCache.Key(batchRequest.dataset, request.keys, servingInfo.batchEndTsMillis)

            // Metrics so we can get per-groupby cache metrics
            val metricsContext =
              request.context.getOrElse(Metrics.Context(Metrics.Environment.JoinFetching, servingInfo.groupBy))

            maybeBatchIrCache.get.cache.getIfPresent(batchRequestCacheKey) match {
              case null =>
                metricsContext.increment(s"${batchIrCacheName}_gb_misses")
                val emptyMap: Map[GetRequest, CachedBatchResponse] = Map.empty
                emptyMap
              case cachedIr: CachedBatchResponse =>
                metricsContext.increment(s"${batchIrCacheName}_gb_hits")
                Map(batchRequest -> cachedIr)
            }
          }
        case _ =>
          val emptyMap: Map[GetRequest, CachedBatchResponse] = Map.empty
          emptyMap
      }
      .foldLeft(Map.empty[GetRequest, CachedBatchResponse])(_ ++ _)
  }
}

object FetcherCache {
  private[online] class BatchIrCache(val cacheName: String, val maximumSize: Int = 10000) {
    import BatchIrCache._

    val cache: CaffeineCache[Key, Value] =
      Cache[Key, Value](cacheName = cacheName, maximumSize = maximumSize)
  }

  private[online] object BatchIrCache {
    // We use the dataset, keys, and batchEndTsMillis to identify a batch request.
    // There's one edge case to be aware of: if a batch job is re-run in the same day, the batchEndTsMillis will
    // be the same but the underlying data may have have changed. If that new batch data is needed immediately, the
    // Fetcher service should be restarted.
    case class Key(dataset: String, keys: Map[String, Any], batchEndTsMillis: Long)

    // FinalBatchIr is for GroupBys using temporally accurate aggregation.
    // Map[String, Any] is for GroupBys using snapshot accurate aggregation or no aggregation.
    type Value = BatchResponses
  }

  // BatchResponses encapsulates either a batch response from kv store or a cached batch response.
  sealed abstract class BatchResponses {
    def getBatchBytes(batchEndTsMillis: Long): Array[Byte]
  }
  object BatchResponses {
    def apply(kvStoreResponse: Try[Seq[TimedValue]]): KvStoreBatchResponse = KvStoreBatchResponse(kvStoreResponse)
    def apply(cachedResponse: FinalBatchIr): CachedFinalIrBatchResponse = CachedFinalIrBatchResponse(cachedResponse)
    def apply(cachedResponse: Map[String, AnyRef]): CachedMapBatchResponse = CachedMapBatchResponse(cachedResponse)
  }
  case class KvStoreBatchResponse(response: Try[Seq[TimedValue]]) extends BatchResponses {
    def getBatchBytes(batchEndTsMillis: Long): Array[Byte] =
      response
        .map(_.maxBy(_.millis))
        .filter(_.millis >= batchEndTsMillis)
        .map(_.bytes)
        .getOrElse(null)
  }
  sealed abstract class CachedBatchResponse extends BatchResponses {
    // This is the case where we don't have bytes because the decoded IR was cached so we didn't hit the KV store again.
    def getBatchBytes(batchEndTsMillis: Long): Null = null
  }
  case class CachedFinalIrBatchResponse(response: FinalBatchIr) extends CachedBatchResponse
  case class CachedMapBatchResponse(response: Map[String, AnyRef]) extends CachedBatchResponse
}
