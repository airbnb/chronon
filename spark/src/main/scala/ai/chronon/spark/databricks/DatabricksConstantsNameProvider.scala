package ai.chronon.spark.databricks

import ai.chronon.api.Extensions.{WindowOps, WindowUtils}
import ai.chronon.api.{ConstantNameProvider, PartitionSpec}

/**
 * DatabricksConstantsNameProvider provides JVM constants used in our Databricks integration.
 * If you need any specific functionality pertaining to your Databricks JVM execution,
 * you can implement it here.
 */
class DatabricksConstantsNameProvider extends ConstantNameProvider with Serializable {
  override def TimeColumn: String = "_internal_time_column"

  override def DatePartitionColumn: String = "day"

  override def HourPartitionColumn: String = "hr"

  override def Partition: PartitionSpec = PartitionSpec(format = "yyyyMMdd", spanMillis = WindowUtils.Day.millis)
}
