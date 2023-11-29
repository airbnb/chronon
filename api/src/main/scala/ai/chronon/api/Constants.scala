package ai.chronon.api

import ai.chronon.api.Extensions._

import java.util.concurrent.atomic.AtomicReference

// Temporary provider setup to allow Stripe to override constant names used for Stripe's
// event sources. As the existing Chronon names are used extensively in the codebase, it's a
// heavier lift to switch out to providing the right constant names on a per-source basis.
trait ConstantNameProvider {
  def TimeColumn: String
  def DatePartitionColumn: String
  def HourPartitionColumn: String
  def Partition: PartitionSpec
}

class ChrononDefaultConstantNameProvider extends ConstantNameProvider {
  override def TimeColumn: String = "ts"
  override def DatePartitionColumn: String = "ds"

  override def HourPartitionColumn: String = "hr"
  override def Partition: PartitionSpec = PartitionSpec(format = "yyyy-MM-dd", spanMillis = WindowUtils.Day.millis)
}

object Constants {
  val constantNameProvider: AtomicReference[ConstantNameProvider] = new AtomicReference(new ChrononDefaultConstantNameProvider)

  def initConstantNameProvider(provider: ConstantNameProvider): Unit =
    constantNameProvider.set(provider)

  def TimeColumn: String = constantNameProvider.get().TimeColumn
  def PartitionColumn: String = constantNameProvider.get().DatePartitionColumn
  def HourPartitionColumn: String = constantNameProvider.get().HourPartitionColumn
  val LabelPartitionColumn: String = "label_ds"
  val TimePartitionColumn: String = "ts_ds"
  val ReversalColumn: String = "is_before"
  val MutationTimeColumn: String = "mutation_ts"
  def Partition: PartitionSpec = constantNameProvider.get().Partition
  def ReservedColumns(partitionColumn: String): Seq[String] =
    Seq(TimeColumn, partitionColumn, TimePartitionColumn, ReversalColumn, MutationTimeColumn)
  val StartPartitionMacro = "[START_PARTITION]"
  val EndPartitionMacro = "[END_PARTITION]"
  val GroupByServingInfoKey = "group_by_serving_info"
  val UTF8 = "UTF-8"
  val lineTab = "\n    "
  val SemanticHashKey = "semantic_hash"
  val SchemaHash = "schema_hash"
  val BootstrapHash = "bootstrap_hash"
  val MatchedHashes = "matched_hashes"
  val ChrononDynamicTable = "chronon_dynamic_table"
  val ChrononOOCTable: String = "chronon_ooc_table"
  val ChrononLogTable: String = "chronon_log_table"
  val StreamingInputTable = "input_table"
  val ChrononMetadataKey = "ZIPLINE_METADATA"
  val SchemaPublishEvent = "SCHEMA_PUBLISH_EVENT"
  val StatsBatchDataset = "CHRONON_STATS_BATCH"
  val TimeField: StructField = StructField(TimeColumn, LongType)
  val ReversalField: StructField = StructField(ReversalColumn, BooleanType)
  val MutationTimeField: StructField = StructField(MutationTimeColumn, LongType)
  val StatsKeySchema: StructType = StructType("keySchema", Array(StructField("JoinPath", StringType)))
  val MutationFields: Seq[StructField] = Seq(MutationTimeField, ReversalField)
  val ExternalPrefix: String = "ext"
  val ContextualSourceName: String = "contextual"
  val ContextualPrefix: String = s"${ExternalPrefix}_${ContextualSourceName}"
  val ContextualSourceKeys: String = "contextual_keys"
  val ContextualSourceValues: String = "contextual_values"
  val TeamOverride: String = "team_override"
  val LabelColumnPrefix: String = "label"
  val LabelViewPropertyFeatureTable: String = "feature_table"
  val LabelViewPropertyKeyLabelTable: String = "label_table"
  val AsyncLoggingCorePoolSize = 0
  val AsyncLoggingMaximumPoolSize = 10
  val AsyncLoggingThreadKeepAliveTime = 60L
  val AsyncLoggingQueueSize = 25
}
