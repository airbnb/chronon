package ai.chronon.spark

object SparkConstants {

  val ChrononOutputParallelismOverride: String = "spark.chronon.outputParallelismOverride"
  val ChrononRowCountPerPartition: String = "spark.chronon.rowCountPerPartition"
  val ChrononJsonSamplingPercent: String = "spark.chronon.json.sampling_percent"
  val ChrononGroupByUploadSplits: String = "spark.chronon.groupByUpload.numSplits"
}
