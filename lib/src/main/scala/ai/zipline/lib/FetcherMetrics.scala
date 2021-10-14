package ai.zipline.lib

import ai.zipline.lib.Metrics.{Context, statsd}

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

   def reportRequestBatchSize(size: Int, metricsContext: Context): Unit = {
     statsd.histogram(Name.RequestBatchSize, size, metricsContext.toTags(): _*)
   }

   def reportResponseNumRows(size: Int, metricsContext: Context): Unit = {
     statsd.histogram(Name.ResponseSize, size, metricsContext.toTags():_*)
   }

   def reportFailure(exception: Exception, metricsContext: Context): Unit = {
     val metricName =
       if (metricsContext.groupBy != null) Name.FailureByGroupBy
       else if (metricsContext.join != null) Name.FailureByJoin
       else throw new RuntimeException("context must be set with either join or group by")
     statsd.increment(metricName,
       metricsContext.toTags(Metrics.Tag.Exception -> exception.getClass.toString): _*
     )
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