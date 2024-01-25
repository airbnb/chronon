/*
 *    Copyright (C) 2023 The Chronon Authors.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package ai.chronon.api

import ai.chronon.api.Extensions._

object Constants {
  val TimeColumn: String = "ts"
  val LabelPartitionColumn: String = "label_ds"
  val TimePartitionColumn: String = "ts_ds"
  val ReversalColumn: String = "is_before"
  val MutationTimeColumn: String = "mutation_ts"
  def ReservedColumns(partitionColumn: String): Seq[String] =
    Seq(TimeColumn, partitionColumn, TimePartitionColumn, ReversalColumn, MutationTimeColumn)
  val StartPartitionMacro = "[START_PARTITION]"
  val EndPartitionMacro = "[END_PARTITION]"
  val GroupByServingInfoKey = "group_by_serving_info"
  val UTF8 = "UTF-8"
  val TopicInvalidSuffix = "_invalid"
  val lineTab = "\n    "
  val SemanticHashKey = "semantic_hash"
  val SchemaHash = "schema_hash"
  val BootstrapHash = "bootstrap_hash"
  val MatchedHashes = "matched_hashes"
  val ChrononDynamicTable = "chronon_dynamic_table"
  val ChrononOOCTable: String = "chronon_ooc_table"
  val ChrononLogTable: String = "chronon_log_table"
  val ChrononMetadataKey = "ZIPLINE_METADATA"
  val SchemaPublishEvent = "SCHEMA_PUBLISH_EVENT"
  val StatsBatchDataset = "CHRONON_STATS_BATCH"
  val ConsistencyMetricsDataset = "CHRONON_CONSISTENCY_METRICS_STATS_BATCH"
  val LogStatsBatchDataset = "CHRONON_LOG_STATS_BATCH"
  val TimeField: StructField = StructField(TimeColumn, LongType)
  val ReversalField: StructField = StructField(ReversalColumn, BooleanType)
  val MutationTimeField: StructField = StructField(MutationTimeColumn, LongType)
  val MutationAvroFields: Seq[StructField] = Seq(TimeField, ReversalField)
  val MutationAvroColumns: Seq[String] = MutationAvroFields.map(_.name)
  val MutationFields: Seq[StructField] = Seq(MutationTimeField, ReversalField)
  val TimedKvRDDKeySchemaKey: String = "__keySchema"
  val TimedKvRDDValueSchemaKey: String = "__valueSchema"
  val StatsKeySchema: StructType = StructType("keySchema", Array(StructField("JoinPath", StringType)))
  val ExternalPrefix: String = "ext"
  val ContextualSourceName: String = "contextual"
  val ContextualPrefix: String = s"${ExternalPrefix}_${ContextualSourceName}"
  val ContextualSourceKeys: String = "contextual_keys"
  val ContextualSourceValues: String = "contextual_values"
  val TeamOverride: String = "team_override"
  val LabelColumnPrefix: String = "label"
  val LabelViewPropertyFeatureTable: String = "feature_table"
  val LabelViewPropertyKeyLabelTable: String = "label_table"
  val ChrononRunDs: String = "CHRONON_RUN_DS"
}
