package ai.chronon.spark

import org.apache.spark.sql.SparkSession

object SparkConstants {

  val ChrononOutputParallelismOverride: String = "spark.chronon.outputParallelismOverride"
  val ChrononRowCountPerPartition: String = "spark.chronon.rowCountPerPartition"

  def readSparkChrononConf(ss: SparkSession, confKey: String, unsetValue: String = "-1"): Option[String] = {
    val valOpt = ss.conf.getOption(confKey)
    if (valOpt.isDefined && !valOpt.contains(unsetValue)) {
      valOpt
    } else {
      None
    }
  }
}
