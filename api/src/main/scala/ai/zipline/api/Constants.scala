package ai.zipline.api

import ai.zipline.api.Extensions._

object Constants {
  val TimeColumn: String = "ts"
  val PartitionColumn: String = "ds"
  val TimePartitionColumn: String = "ts_ds"
  val ReversalColumn: String = "is_before"
  val MutationTimeColumn: String = "mutation_ts"
  val ReservedColumns: Seq[String] =
    Seq(TimeColumn, PartitionColumn, TimePartitionColumn, ReversalColumn, MutationTimeColumn)
  val Partition: PartitionSpec =
    PartitionSpec(format = "yyyy-MM-dd", spanMillis = WindowUtils.Day.millis)
  val StartPartitionMacro = "[START_PARTITION]"
  val EndPartitionMacro = "[END_PARTITION]"
  val GroupByServingInfoKey = "group_by_serving_info"
  val UTF8 = "UTF-8"
  val lineTab = "\n    "
  val JoinMetadataKey = "join"
  val StreamingInputTable = "input_table"
  val ZiplineMetadataKey = "ZIPLINE_METADATA"
  // Order is important when serializing deserializing mutation data.
  val TimeField = StructField(TimeColumn, LongType)
  val ReversalField = StructField(ReversalColumn, BooleanType)
  val MutationTimeField = StructField(MutationTimeColumn, LongType)
  val MutationFields: Seq[StructField] = Seq(MutationTimeField, ReversalField)
  val MutationAvroFields: Seq[StructField] = Seq(TimeField, ReversalField)
  val MutationColumns: Seq[String] = MutationFields.map(_.name)
  val MutationAvroColumns: Seq[String] = MutationAvroFields.map(_.name)
}
