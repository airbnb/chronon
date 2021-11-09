package ai.zipline.fetcher

import ai.zipline.api.GroupByServingInfo
import ai.zipline.fetcher.Metrics.{Context, statsd}
import com.timgroup.statsd.{NonBlockingStatsDClient, StatsDClient}

import scala.collection.mutable

object Metrics {

  val statsd: StatsDClient = new NonBlockingStatsDClient("zipline", "localhost", 8125)

  object Tag {
    val GroupBy = "group_by"
    val Join = "join"
    val Streaming = "streaming"
    val Batch = "batch"
    val RequestType = "request_type"
    val Owner = "owner"
    val Method = "method"
    val Production = "production"
    val Accuracy = "accuracy"
    val Team = "team"
    val Exception = "exception"
  }

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
    def withIsStreaming(isStreaming: Boolean): Context = copy(isStreaming = isStreaming)
    def withMethod(method: String): Context = copy(method = method)
    def withProduction(production: Boolean): Context = copy(production = production)
    def asBatch: Context = copy(isStreaming = false)
    def asStreaming: Context = copy(isStreaming = true)
    def withAccuracy(accuracy: String): Context = copy(accuracy = accuracy)
    def withTeam(team: String): Context = copy(team = team)

    // Tagging happens to be the most expensive part(~40%) of reporting stats.
    // And reporting stats is about 30% of overall fetching latency.
    // So we do array packing directly instead of regular string interpolation.
    // This simply creates "key:value"
    // The optimization shaves about 2ms of 6ms of e2e overhead for 500 batch size.
    def buildTag(key: String, value: String): String = {
      val charBuf = new Array[Char](key.size + value.size + 1)
      key.getChars(0, key.size, charBuf, 0)
      value.getChars(0, value.size, charBuf, key.size + 1)
      charBuf.update(key.size, ':')
      new String(charBuf)
    }

    def toTags(tags: (String, String)*): Array[String] = {
      assert(join != null || groupBy != null, "Either Join, groupBy should be set.")
      val buffer = new Array[String](tags.size + 6)
      var counter = 0
      def addTag(key: String, value: String): Unit = {
        if (value == null) return
        assert(counter < buffer.size, "array overflow")
        buffer.update(counter, buildTag(key, value))
        counter += 1
      }

      addTag(Tag.Join, join)
      addTag(Tag.GroupBy, groupBy)
      addTag(Tag.Production, production.toString)
      addTag(Tag.Team, team)
      addTag(Tag.Method, method)
      addTag(Tag.RequestType, requestType(isStreaming))
      var i = 0
      while (i < tags.size) {
        val tag = tags(i)
        addTag(tag._1, tag._2)
        i += 1
      }

      buffer
    }

    private def requestType(isStreaming: Boolean): String = if (isStreaming) Tag.Streaming else Tag.Batch
  }
}

object FetcherMetrics {

  object Name {
    private val fetcher = "fetcher"
    private val success = "success"
    private val failure = "failure"

    val RequestBatchSize = s"$fetcher.request_batch.size"
    val ResponseSize = s"$fetcher.response.size"
    val EmptyResponseCount = s"$fetcher.empty_response.count"

    val FailureByGroupBy = s"$fetcher.group_by.$failure"
    val FailureByJoin = s"$fetcher.join.$failure"

    val DataFreshness = s"$fetcher.data_freshness_ms"
    val DataSizeBytes = s"$fetcher.data_size_bytes"
    val Latency = s"$fetcher.latency_ms"
    val FinalLatency = s"$fetcher.final_latency_ms"
    val StreamingRowSize: String = s"$fetcher.streaming.row_size"
  }

  def getGroupByContext(groupByServingInfo: GroupByServingInfo,
                        contextOption: Option[Metrics.Context] = None): Metrics.Context = {
    val context = contextOption.getOrElse(Metrics.Context())
    val groupBy = groupByServingInfo.getGroupBy
    context
      .withGroupBy(groupBy.getMetaData.getName)
      .withProduction(groupBy.getMetaData.isProduction)
      .withTeam(groupBy.getMetaData.getTeam)
  }

  def reportRequestBatchSize(size: Int, metricsContext: Context): Unit = {
    statsd.histogram(Name.RequestBatchSize, size, metricsContext.toTags(): _*)
  }

  def reportResponseNumRows(size: Int, metricsContext: Context): Unit = {
    statsd.histogram(Name.ResponseSize, size, metricsContext.toTags(): _*)
  }

  def reportFailure(exception: Exception, metricsContext: Context): Unit = {
    val metricName =
      if (metricsContext.groupBy != null) Name.FailureByGroupBy
      else if (metricsContext.join != null) Name.FailureByJoin
      else throw new RuntimeException("context must be set with either join or group by")
    statsd.increment(metricName, metricsContext.toTags(Metrics.Tag.Exception -> exception.getClass.toString): _*)
  }

  def reportDataFreshness(millis: Long, metricsContext: Context): Unit = {
    statsd.histogram(Name.DataFreshness, millis, metricsContext.toTags(): _*)
  }

  def reportResponseBytesSize(sizeBytes: Long, metricsContext: Context): Unit = {
    statsd.histogram(Name.DataSizeBytes, sizeBytes, metricsContext.toTags(): _*)
    if (sizeBytes == 0) {
      statsd.increment(Name.EmptyResponseCount, metricsContext.toTags(): _*)
    }
  }

  def reportLatency(millis: Long, metricsContext: Context): Unit = {
    statsd.histogram(Name.Latency, millis, metricsContext.toTags(): _*)
  }

  // report latency as the maximum latency of all group bys / joins included in multiGet requests.
  def reportFinalLatency(millis: Long, metricsContext: Context): Unit = {
    statsd.histogram(Name.FinalLatency, millis, metricsContext.toTags(): _*)
  }
}

object MetadataMetrics {

  object Name {
    private val metadata = "metadata"
    val JoinConfLatency: String = s"$metadata.join_conf.latency_ms"
    val GroupByServingLatency: String = s"$metadata.group_by_serving_info.latency_ms"
  }

  def reportJoinConfRequestMetric(latencyMs: Long, context: Context): Unit = {
    statsd.histogram(Name.JoinConfLatency, latencyMs, context.toTags(): _*)
  }

  def reportGroupByServingInfoRequestMetric(latencyMs: Long, context: Context): Unit = {
    statsd.histogram(Name.GroupByServingLatency, latencyMs, context.toTags(): _*)
  }
}
