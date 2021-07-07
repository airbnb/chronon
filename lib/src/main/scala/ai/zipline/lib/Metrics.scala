package ai.zipline.lib

import scala.collection.mutable

import com.timgroup.statsd.{NonBlockingStatsDClient, StatsDClient}

object Metrics {

  val statsd: StatsDClient = new NonBlockingStatsDClient("zipline", "localhost", 8125)

  val tagGroupBy = "group_by"
  val tagJoin = "join"
  val tagStreaming = "streaming"
  val tagBatch = "batch"
  val tagRequestType = "request_type"
  val tagOwner = "owner"
  val tagMethod = "method"
  val tagProduction = "production"
  val tagAccuracy = "accuracy"
  val tagTeam = "team"

  case class Context(join: String = null,
                     groupBy: String = null,
                     production: Boolean = false,
                     isStaging: Boolean = false,
                     isStreaming: Boolean = false,
                     method: String = null,
                     owner: String = null,
                     accuracy: String = null,
                     team: String = null) {
    def withJoin(join: String): Context = copy(join = join)
    def withGroupBy(groupBy: String): Context = copy(groupBy = groupBy)
    def withIsStreaming(isStreaming: Boolean):  Context = copy(isStreaming = isStreaming)
    def withMethod(method: String): Context = copy(method = method)
    def withProduction(production: Boolean): Context = copy(production = production)
    def asBatch: Context = copy(isStreaming = false)
    def asStreaming: Context = copy(isStreaming = true)
    def withAccuracy(accuracy: String): Context = copy(accuracy = accuracy)
    def withTeam(team: String): Context = copy(team = team)

    def toTags(tags: (String, Any)*): Seq[String] = {
      assert(join != null || groupBy != null,
        "Either Join, groupBy should be set.")
      val result = mutable. HashMap.empty[String, String]
      if (join != null) {
        result.update(tagJoin, join)
      }
      if (groupBy != null) {
        result.update(tagGroupBy, groupBy)
        result.update(tagProduction, production.toString)
      }
      result.update(tagTeam, team)
      result.update(tagMethod, method)
      result.update(tagRequestType, requestType(isStreaming))
      result.toMap.map(t => s"${t._1}:${t._2}").toSeq ++
        tags.map(t => s"${t._1}:${t._2}")
    }

    private def requestType(isStreaming: Boolean): String = if (isStreaming) tagStreaming else tagBatch
  }
  object Fetcher {

    private val fetcher = "fetcher"
    private val success = "success"
    private val failure = "failure"
    private val tagException = "exception"
    private val requestBatchSizeMetricName = s"$fetcher.request_batch.size"
    private val responseSizeMetricName = s"$fetcher.response.size"
    private val emptyResponseCountMetricName = s"$fetcher.empty_response.count"

    private val requestSuccessMetricName: String = s"$fetcher.success"
    private val requestSuccessByJoinMetricName: String = s"$fetcher.join.$success"
    private val requestSuccessByGroupByMetricName: String = s"$fetcher.group_by.$success"
    private val requestFailureMetricName = s"$fetcher.$failure"
    private val requestFailureByGroupByMetricName = s"$fetcher.group_by.$failure"
    private val requestFailureByJoinMetricName = s"$fetcher.join.$failure"

    private val dataFreshnessMetricName = s"$fetcher.data_freshness_ms"
    private val dataSizeBytesMetricName = s"$fetcher.data_size_bytes"
    private val latencyMetricName = s"$fetcher.latency_ms"

    private val groupByServingLatencyMetricName: String = s"$fetcher.group_by_serving_info.latency_ms"
    private val joinConfLatencyMetricName: String = s"$fetcher.join_conf.latency_ms"
    private val streamingRowSizeMetricName: String = s"$fetcher.streaming.row_size"


    def reportJoinConfRequestMetric(latencyMs: Long, context: Context): Unit = {
      statsd.histogram(joinConfLatencyMetricName, latencyMs, context.toTags(): _*)
    }

    def reportGroupByServingInfoRequestMetric(latencyMs: Long, context: Context):Unit = {
      statsd.histogram(groupByServingLatencyMetricName, latencyMs, context.toTags(): _*)
    }

    // report number of streaming rows to aggregate.
    def reportStreamingRowSize(rowSize: Int, context: Context): Unit = {
      statsd.histogram(streamingRowSizeMetricName, rowSize, context.toTags(): _*)
    }

    def reportRequestSuccess(context: Context): Unit = {
      val metricName = {
        if (context.groupBy != null) requestSuccessByGroupByMetricName
        else if (context.join != null) requestSuccessByJoinMetricName
        else requestSuccessMetricName
      }
      statsd.increment(metricName, context.toTags(): _*)
    }

    def reportRequestBatchSize(size: Int, metricsContext: Context): Unit = {
      statsd.histogram(requestBatchSizeMetricName, size, metricsContext.toTags(): _*)
    }

    def reportResponseByFeatureSet(size: Int, metricsContext: Context): Unit = {
      statsd.histogram(responseSizeMetricName, size, metricsContext.toTags():_*)
    }

    def reportRequestFailure(exception: Exception, metricsContext: Context): Unit = {
      val metricName =
        if (metricsContext.groupBy != null) requestFailureByGroupByMetricName
        else if (metricsContext.join != null) requestFailureByJoinMetricName
        else requestFailureMetricName
      statsd.increment(
        metricName,
        metricsContext.toTags(tagException -> exception.getClass.toString): _*
      )
    }

    def reportDataFreshness(millis: Long, metricsContext: Context): Unit = {
      statsd.histogram(dataFreshnessMetricName, millis, metricsContext.toTags(): _*)
    }

    def reportDataSize(sizeBytes: Long, metricsContext: Context): Unit = {
      statsd.histogram(dataSizeBytesMetricName, sizeBytes, metricsContext.toTags(): _*)
      if (sizeBytes == 0) {
        statsd.increment(emptyResponseCountMetricName, metricsContext.toTags(): _*)
      }
    }

    def reportLatency(millis: Long, metricsContext: Context): Unit = {
      statsd.histogram(latencyMetricName, millis, metricsContext.toTags(): _*)
    }
  }
}
