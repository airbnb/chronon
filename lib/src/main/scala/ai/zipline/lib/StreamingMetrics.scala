package ai.zipline.lib

import ai.zipline.lib.Metrics.{Context, statsd}

object StreamingMetrics {

  private val stream = "stream"
  private val rowCount = "row_count"
  private val v21prefix = "v21"

  object Egress {
    private val egress = s"$v21prefix.${stream}_egress"
    private val egressDataSizeMetricName = s"$egress.data_size_bytes"
    private val egressLatencyMillisMetricName = s"$egress.latency_millis"
    private val egressRowCountMetricName = s"$egress.$rowCount"

    def reportDataSize(sizeBytes: Long, metricsContext: Context): Unit = {
      statsd.histogram(egressDataSizeMetricName, sizeBytes, metricsContext.toTags() : _*)
    }

    def reportLatency(millis: Long, metricsContext: Context): Unit = {
      statsd.histogram(egressLatencyMillisMetricName, millis, metricsContext.toTags() : _*)
    }

    def reportRowCount(metricsContext: Context): Unit = {
      statsd.increment(egressRowCountMetricName, metricsContext.toTags() : _*)
    }
  }

  object Ingress {
    private val ingress = s"$v21prefix${stream}_ingress"
    private val ingressRowCountMetricName = s"$ingress.$rowCount"
    private val ingressDataSizeMetricName = s"$ingress.data_size_bytes"

    def reportRowCount(metricsContext: Context): Unit = {
      statsd.increment(ingressRowCountMetricName, metricsContext.toTags() : _*)
    }

    def reportDataSize(sizeBytes: Long, metricsContext: Context): Unit = {
      statsd.histogram(ingressDataSizeMetricName, sizeBytes, metricsContext.toTags() : _*)
    }
  }
}
