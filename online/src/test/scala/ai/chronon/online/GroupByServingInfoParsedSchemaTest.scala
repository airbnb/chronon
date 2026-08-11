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

package ai.chronon.online

import ai.chronon.api.{Builders, GroupByServingInfo, PartitionSpec, StructType}
import ai.chronon.online.serde.AvroConversions
import org.junit.Assert.{assertEquals, assertTrue}
import org.junit.Test

/**
  * Pins the schema accessors on GroupByServingInfoParsed.
  *
  * `deriveFunc` previously computed its two arguments inline. It now reads them from the
  * `keyChrononSchema` and `baseValueChrononSchema` lazy vals so that `responseChrononSchema` can
  * share them without recomputing Catalyst work per request. `deriveFunc` sits on the live
  * `FetcherBase.fetchGroupBys` path, so these tests assert the refactor is value-preserving: if
  * the inputs are identical, the derivation behavior is unchanged by construction.
  */
class GroupByServingInfoParsedSchemaTest {

  private val keySchema = StructType.from("Key", Array("user" -> ai.chronon.api.StringType))
  private val valueSchema = StructType.from(
    "Value",
    Array("amount_dollars_sum_15d" -> ai.chronon.api.LongType, "amount_dollars_sum_30d" -> ai.chronon.api.LongType)
  )

  private def servingInfo(derivations: Seq[ai.chronon.api.Derivation] = null): GroupByServingInfoParsed = {
    val info = new GroupByServingInfo()
    // aggregations left null: exercises the no-aggregation branch, where the base value schema is
    // the selected schema rather than the aggregator output schema.
    info.setGroupBy(
      Builders.GroupBy(
        metaData = Builders.MetaData(name = "unit_test.serving_info_schemas"),
        keyColumns = Seq("user"),
        derivations = derivations
      ))
    info.setKeyAvroSchema(AvroConversions.fromChrononSchema(keySchema).toString(true))
    info.setSelectedAvroSchema(AvroConversions.fromChrononSchema(valueSchema).toString(true))
    info.setBatchEndDate("2026-08-10")
    new GroupByServingInfoParsed(info, PartitionSpec(format = "yyyy-MM-dd", spanMillis = 86400000L))
  }

  @Test
  def keyChrononSchemaMatchesTheKeyCodecSchemaItReplaced(): Unit = {
    // deriveFunc used to pass `keyCodec.chrononSchema.asInstanceOf[StructType]`; it now passes
    // `keyChrononSchema`. Both reduce to toChrononSchema(parse(keyAvroSchema)).
    val info = servingInfo()
    assertEquals(info.keyCodec.chrononSchema.asInstanceOf[StructType], info.keyChrononSchema)
  }

  @Test
  def baseValueChrononSchemaMatchesTheInlineExpressionItReplaced(): Unit = {
    // deriveFunc used to compute this inline as
    //   if (groupBy.aggregations == null) selectedChrononSchema else outputChrononSchema
    val info = servingInfo()
    assertTrue("test fixture should exercise the no-aggregation branch", info.groupBy.aggregations == null)
    assertEquals(info.selectedChrononSchema, info.baseValueChrononSchema)
  }

  @Test
  def responseSchemaIsTheBaseSchemaWithoutDerivations(): Unit = {
    val info = servingInfo()
    assertEquals(info.baseValueChrononSchema, info.responseChrononSchema)
  }

  @Test
  def responseSchemaReflectsRenameOnlyDerivations(): Unit = {
    // A rename-only derivation set avoids Catalyst, so this stays a pure schema assertion.
    val info = servingInfo(
      Seq(
        Builders.Derivation(name = "sum_15d", expression = "amount_dollars_sum_15d"),
        Builders.Derivation(name = "sum_30d", expression = "amount_dollars_sum_30d")
      ))
    val names = info.responseChrononSchema.fields.map(_.name).toSet
    assertEquals(Set("sum_15d", "sum_30d"), names)
  }

  @Test
  def schemaAccessorsAreStableAcrossCalls(): Unit = {
    // They are lazy vals now rather than recomputed expressions; a second read must be identical
    // (and, for responseChrononSchema, must not redo the derived-field resolution).
    val info = servingInfo()
    assertTrue(info.keyChrononSchema eq info.keyChrononSchema)
    assertTrue(info.baseValueChrononSchema eq info.baseValueChrononSchema)
    assertTrue(info.responseChrononSchema eq info.responseChrononSchema)
  }
}
