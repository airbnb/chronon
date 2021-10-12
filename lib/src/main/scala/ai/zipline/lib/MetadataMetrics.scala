package ai.zipline.lib

import ai.zipline.lib.Metrics.{Context, statsd}

object MetadataMetrics {

  object Name {
    private val metadata = "metadata"
    val JoinConfLatency: String = s"$metadata.join_conf.latency_ms"
    val GroupByServingLatency: String = s"$metadata.group_by_serving_info.latency_ms"
  }

  def reportJoinConfRequestMetric(latencyMs: Long, context: Context): Unit = {
      statsd.histogram(Name.JoinConfLatency, latencyMs, context.toTags(): _*)
    }

  def reportGroupByServingInfoRequestMetric(latencyMs: Long, context: Context):Unit = {
    statsd.histogram(Name.GroupByServingLatency, latencyMs, context.toTags(): _*)
  }
}
