package ai.zipline.spark.streaming

import ai.zipline.online.Metrics.Context

object Metrics {

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
      metricsContext.stats.histogram(Name.EgressDataSize, sizeBytes)
    }

    def reportLatency(millis: Long, metricsContext: Context): Unit = {
      metricsContext.stats.histogram(Name.EgressLatencyMillis, millis)
    }

    def reportRowCount(metricsContext: Context): Unit = {
      metricsContext.stats.increment(Name.EgressRowCount)
    }
  }

  object Ingress {
    def reportRowCount(metricsContext: Context): Unit = {
      metricsContext.stats.increment(Name.IngressRowCount)
    }

    def reportDataSize(sizeBytes: Long, metricsContext: Context): Unit = {
      metricsContext.stats.histogram(Name.IngressDataSize, sizeBytes)
    }
  }
}
