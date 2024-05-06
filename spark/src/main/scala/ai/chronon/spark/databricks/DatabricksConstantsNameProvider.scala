package ai.chronon.spark.databricks

import ai.chronon.api.Extensions.{WindowOps, WindowUtils}
import ai.chronon.api.{ConstantNameProvider, PartitionSpec}

class DatabricksConstantsNameProvider extends ConstantNameProvider with Serializable {
  override def TimeColumn: String = "_internal_time_column"
  override def DatePartitionColumn: String = "day"

  override def HourPartitionColumn: String = "hr"
  override def Partition: PartitionSpec = PartitionSpec(format = "yyyyMMdd", spanMillis = WindowUtils.Day.millis)
}