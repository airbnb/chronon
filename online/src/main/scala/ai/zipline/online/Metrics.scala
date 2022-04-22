package ai.zipline.online

import ai.zipline.api.GroupByServingInfo
import ai.zipline.online.KVStore.TimedValue
import ai.zipline.online.Metrics.Context
import com.timgroup.statsd.{NonBlockingStatsDClient, StatsDClient}

object Metrics {

  val statsCache: TTLCache[Context, NonBlockingStatsDClient] = new TTLCache[Context, NonBlockingStatsDClient](
    { ctx =>
      println(s"""Building new stats cache for: Join(${ctx.join}), GroupByJoin(${ctx.groupBy}) 
           |hash: ${ctx.hashCode()}
           |context $ctx         
           |""".stripMargin)
      new NonBlockingStatsDClient("zipline", "localhost", 8125, ctx.toTags: _*)
    }
  )

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
      val charBuf = new Array[Char](key.length + value.length + 1)
      key.getChars(0, key.length, charBuf, 0)
      value.getChars(0, value.length, charBuf, key.length + 1)
      charBuf.update(key.length, ':')
      new String(charBuf)
    }

    @transient lazy val stats: NonBlockingStatsDClient = statsCache(this)

    private[Metrics] def toTags: Array[String] = {
      assert(join != null || groupBy != null, "Either Join, groupBy should be set.")
      val buffer = new Array[String](6)
      var counter = 0
      def addTag(key: String, value: String): Unit = {
        if (value == null) return
        assert(counter < buffer.length, "array overflow")
        buffer.update(counter, buildTag(key, value))
        counter += 1
      }

      addTag(Tag.Join, join)
      addTag(Tag.GroupBy, groupBy)
      addTag(Tag.Production, production.toString)
      addTag(Tag.Team, team)
      addTag(Tag.Method, method)
      addTag(Tag.RequestType, requestType(isStreaming))
      buffer
    }

    private def requestType(isStreaming: Boolean): String = if (isStreaming) Tag.Streaming else Tag.Batch
  }
}

object FetcherMetrics {

  object Name {
    private val fetcher = "fetcher"
    private val failure = "failure"

    val FetcherRequest = s"$fetcher.request"
    val RequestBatchSize = s"$fetcher.request_batch.size"
    val ResponseSize = s"$fetcher.response.size"
    val EmptyResponseCount = s"$fetcher.empty_response.count"

    val FailureByGroupBy = s"$fetcher.group_by.$failure"
    val FailureByJoin = s"$fetcher.join.$failure"

    val KvResponseSizeBytes = s"$fetcher.kv_store.response.size_bytes"
    val KvResponseRowCount = s"$fetcher.kv_store.response.row_count"
    val KvFreshnessMillis = s"$fetcher.kv_store.response.freshness_millis"
    val KvTotalResponseBytes = s"$fetcher.kv_store.response.total_size_bytes"
    val KvAttributedLatencyMillis = s"$fetcher.kv_store.response.attributed_latency_millis"
    val KvOverallLatencyMillis = s"$fetcher.kv_store.response.overall_latency_millis"

    val BatchSizeBytes = s"$fetcher.batch_size_bytes"
    val DataFreshness = s"$fetcher.data_freshness_ms"
    val DataSizeBytes = s"$fetcher.data_size_bytes"
    val Latency = s"$fetcher.latency_ms"
    val FinalLatency = s"$fetcher.final_latency_ms"
    val KvLatency = s"$fetcher.kv_latency_ms"
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
    metricsContext.stats.histogram(Name.RequestBatchSize, size)
  }

  def reportResponseNumRows(size: Int, metricsContext: Context): Unit = {
    metricsContext.stats.histogram(Name.ResponseSize, size)
  }

  def reportFailure(exception: Throwable, metricsContext: Context): Unit = {
    val metricName =
      if (metricsContext.groupBy != null) Name.FailureByGroupBy
      else if (metricsContext.join != null) Name.FailureByJoin
      else throw new RuntimeException("context must be set with either join or group by")
    metricsContext.stats
      .increment(metricName, s"${Metrics.Tag.Exception}:${exception.getClass.toString}")
  }

  def reportDataFreshness(millis: Long, metricsContext: Context): Unit = {
    metricsContext.stats.histogram(Name.DataFreshness, millis)
  }

  def reportKvResponse(response: Seq[TimedValue],
                       startTsMillis: Long,
                       latencyMillis: Long,
                       totalResponseBytes: Int,
                       metricsContext: Context): Unit = {
    val latestResponseTs = response.iterator.map(_.millis).reduceOption(Ordering[Long].max).getOrElse(startTsMillis)
    val responseBytes = response.iterator.map(_.bytes.length).sum
    metricsContext.stats.histogram(Name.KvResponseRowCount, response.length)
    metricsContext.stats.histogram(Name.KvResponseSizeBytes, responseBytes)
    metricsContext.stats.histogram(Name.KvFreshnessMillis, startTsMillis - latestResponseTs)
    metricsContext.stats.histogram(Name.KvTotalResponseBytes, totalResponseBytes)
    metricsContext.stats.histogram(Name.KvOverallLatencyMillis, latencyMillis)
    metricsContext.stats
      .histogram(Name.KvAttributedLatencyMillis, (responseBytes.toDouble / totalResponseBytes.toDouble) * latencyMillis)
  }

  def reportResponseBytesSize(sizeBytes: Long, metricsContext: Context): Unit = {
    metricsContext.stats.histogram(Name.DataSizeBytes, sizeBytes)
    if (sizeBytes == 0) {
      metricsContext.stats.increment(Name.EmptyResponseCount)
    }
  }

  def reportLatency(millis: Long, metricsContext: Context): Unit = {
    metricsContext.stats.histogram(Name.Latency, millis)
  }

  def reportKvLatency(millis: Long, metricsContext: Context): Unit = {
    metricsContext.stats.histogram(Name.KvLatency, millis)
  }

  def reportRequest(metricsContext: Context): Unit = {
    metricsContext.stats.increment(Name.FetcherRequest)
  }
}

object MetadataMetrics {

  object Name {
    private val metadata = "metadata"
    val JoinConfLatency: String = s"$metadata.join_conf.latency_ms"
    val GroupByServingLatency: String = s"$metadata.group_by_serving_info.latency_ms"
  }

  def reportJoinConfRequestMetric(latencyMs: Long, context: Context): Unit = {
    context.stats.histogram(Name.JoinConfLatency, latencyMs)
  }

  def reportGroupByServingInfoRequestMetric(latencyMs: Long, context: Context): Unit = {
    context.stats.histogram(Name.GroupByServingLatency, latencyMs)
  }
}
