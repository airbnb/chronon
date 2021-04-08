package ai.zipline.api

import ai.zipline.api.Extensions._

object Constants {
  val TimeColumn: String = "ts"
  val PartitionColumn: String = "ds"
  val TimePartitionColumn: String = "ts_ds"
  val ReversalColumn: String = "is_before"
  val MutationTimeColumn: String = "mutation_ts"
  val Partition: PartitionSpec =
    PartitionSpec(format = "yyyy-MM-dd", spanMillis = WindowUtils.Day.millis)
  val StartPartitionMacro = "[START_PARTITION]"
  val EndPartitionMacro = "[END_PARTITION]"
  val lineTab = "\n    "
  val JoinMetadataKey = "join"
}
