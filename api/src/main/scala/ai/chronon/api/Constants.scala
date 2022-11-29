package ai.chronon.api

import ai.chronon.api.Extensions._

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
  val SemanticHashKey = "semantic_hash"
  val SchemaHash = "schema_hash"
  val ChrononDynamicTable = "chronon_dynamic_table"
  val StreamingInputTable = "input_table"
  val ChrononMetadataKey = "ZIPLINE_METADATA"
  val SchemaPublishEvent = "SCHEMA_PUBLISH_EVENT"
  val StatsKeySchemaKey = "key_schema"
  val StatsValueSchemaKey = "value_schema"
  val TimeField: StructField = StructField(TimeColumn, LongType)
  val ReversalField: StructField = StructField(ReversalColumn, BooleanType)
  val MutationTimeField: StructField = StructField(MutationTimeColumn, LongType)
  val MutationFields: Seq[StructField] = Seq(MutationTimeField, ReversalField)
  val ContextualSourceName: String = "contextual"
  val ContextualSourceKeys: String = "contextual_keys"
  val ContextualSourceValues: String = "contextual_values"
}
