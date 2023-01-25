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
  def ReservedColumns: Seq[String] =
    Seq(TimeColumn, PartitionColumn, TimePartitionColumn, ReversalColumn, MutationTimeColumn)
  def Partition: PartitionSpec = constantNameProvider.get().Partition
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
  val ExternalPrefix: String = "ext"
  val ContextualSourceName: String = "contextual"
  val ContextualSourceKeys: String = "contextual_keys"
  val ContextualSourceValues: String = "contextual_values"
}
