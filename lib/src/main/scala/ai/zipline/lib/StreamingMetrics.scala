package ai.zipline.lib

import ai.zipline.lib.Metrics.{Context, statsd}

object StreamingMetrics {

  object Name {
    val Stream = "stream"
    val RowCount = "row_count"

    val Egress = s"${Stream}_egress"
    val EgressDataSize = s"$Egress.data_size_bytes"
    val EgressLatencyMillis = s"$Egress.latency_millis"
    val EgressRowCount = s"$Egress.$RowCount"

    val Ingress = s"${Stream}_ingress"
    val IngressRowCount = s"$Ingress.$RowCount"
    val IngressDataSize = s"$Ingress.data_size_bytes"
  }


  object Egress {


    def reportDataSize(sizeBytes: Long, metricsContext: Context): Unit = {
      statsd.histogram(Name.EgressDataSize, sizeBytes, metricsContext.toTags() : _*)
    }

    def reportLatency(millis: Long, metricsContext: Context): Unit = {
      statsd.histogram(Name.EgressLatencyMillis, millis, metricsContext.toTags() : _*)
    }

    def reportRowCount(metricsContext: Context): Unit = {
      statsd.increment(Name.EgressRowCount, metricsContext.toTags() : _*)
    }
  }

  object Ingress {

    def reportRowCount(metricsContext: Context): Unit = {
      statsd.increment(Name.IngressRowCount, metricsContext.toTags() : _*)
    }

    def reportDataSize(sizeBytes: Long, metricsContext: Context): Unit = {
      statsd.histogram(Name.IngressDataSize, sizeBytes, metricsContext.toTags() : _*)
    }
  }
}
