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

package ai.chronon.online.test

import ai.chronon.api.Builders.Derivation
import ai.chronon.api._
import ai.chronon.online.DerivationUtils
import junit.framework.TestCase
import org.junit.Assert.{assertEquals, assertTrue}
import org.junit.Test

import scala.util.ScalaJavaConversions.ListOps

class DerivationUtilsTest extends TestCase {

  @Test
  def testBuildDerivedFieldsWithDsIdentityDerivation(): Unit = {
    // Test case 1: User is using rename-only ds derivations
    // This tests that ds field can be accessed in derivations after adding timeFields
    val keySchema = StructType(
      "keys",
      Array(
        StructField("user_id", LongType)
      )
    )

    val baseValueSchema = StructType(
      "values",
      Array(
        StructField("amount", DoubleType),
        StructField("count", LongType)
      )
    )

    // Create a derivation that references ds (which is in timeFields)
    val derivations = List(
      Derivation(name = "ds", expression = "ds"),
      Derivation(name = "amount", expression = "amount")
    )

    // This should not throw an exception because ds is now available in allSchema
    val derivedFields = DerivationUtils.buildDerivedFields(
      derivations.toJava,
      keySchema,
      baseValueSchema
    )

    // Verify the derived fields
    assertEquals(2, derivedFields.size)
    assertEquals("ds", derivedFields(0).name)
    assertEquals(StringType, derivedFields(0).fieldType)
    assertEquals("amount", derivedFields(1).name)
    assertEquals(DoubleType, derivedFields(1).fieldType)
  }

  @Test
  def testBuildDerivedFieldsWithTsIdentityDerivation(): Unit = {
    // Test that ts field can also be accessed in derivations
    val keySchema = StructType(
      "keys",
      Array(
        StructField("user_id", LongType)
      )
    )

    val baseValueSchema = StructType(
      "values",
      Array(
        StructField("amount", DoubleType)
      )
    )

    val derivations = List(
      Derivation(name = "ts", expression = "ts"),
      Derivation(name = "user_id", expression = "user_id")
    )

    val derivedFields = DerivationUtils.buildDerivedFields(
      derivations.toJava,
      keySchema,
      baseValueSchema
    )

    assertEquals(2, derivedFields.size)
    assertEquals("ts", derivedFields(0).name)
    assertEquals(LongType, derivedFields(0).fieldType)
    assertEquals("user_id", derivedFields(1).name)
    assertEquals(LongType, derivedFields(1).fieldType)
  }

  @Test
  def testBuildDerivedFieldsWithWildcardDerivation(): Unit = {
    // Test case 2: User is using wildcard derivations
    // Wildcard should include all base fields but derivations with specific names
    // should still be able to reference ds and ts
    val keySchema = StructType(
      "keys",
      Array(
        StructField("user_id", LongType)
      )
    )

    val baseValueSchema = StructType(
      "values",
      Array(
        StructField("amount", DoubleType),
        StructField("count", LongType)
      )
    )

    val derivations = List(
      Derivation.star(),
      Derivation(name = "ds_copy", expression = "ds"),
      Derivation(name = "ts_copy", expression = "ts")
    )

    val derivedFields = DerivationUtils.buildDerivedFields(
      derivations.toJava,
      keySchema,
      baseValueSchema
    )

    // With wildcard, we should get:
    // - amount (from baseValueSchema)
    // - count (from baseValueSchema)
    // - ds_copy (derived from ds in timeFields)
    // - ts_copy (derived from ts in timeFields)
    assertEquals(4, derivedFields.size)

    // Verify the fields
    val fieldNames = derivedFields.map(_.name)
    assertTrue(fieldNames.contains("amount"))
    assertTrue(fieldNames.contains("count"))
    assertTrue(fieldNames.contains("ds_copy"))
    assertTrue(fieldNames.contains("ts_copy"))

    // Verify types for the derived fields
    val dsField = derivedFields.find(_.name == "ds_copy").get
    assertEquals(StringType, dsField.fieldType)

    val tsField = derivedFields.find(_.name == "ts_copy").get
    assertEquals(LongType, tsField.fieldType)
  }

  @Test
  def testBuildDerivedFieldsWithTimeFieldsInExpression(): Unit = {
    // Test derivations that use time fields in expressions (not just identity)
    val keySchema = StructType(
      "keys",
      Array(
        StructField("user_id", LongType)
      )
    )

    val baseValueSchema = StructType(
      "values",
      Array(
        StructField("created_ts", LongType)
      )
    )

    val derivations = List(
      Derivation(name = "time_diff", expression = "ts - created_ts"),
      Derivation(name = "ds_formatted", expression = "CONCAT('partition_', ds)")
    )

    val derivedFields = DerivationUtils.buildDerivedFields(
      derivations.toJava,
      keySchema,
      baseValueSchema
    )

    assertEquals(2, derivedFields.size)
    assertEquals("time_diff", derivedFields(0).name)
    assertEquals("ds_formatted", derivedFields(1).name)
  }

  @Test
  def testBuildDerivedFieldsWithComplexDerivations(): Unit = {
    // Test more complex derivations that combine base fields and time fields
    val keySchema = StructType(
      "keys",
      Array(
        StructField("user_id", LongType),
        StructField("listing_id", LongType)
      )
    )

    val baseValueSchema = StructType(
      "values",
      Array(
        StructField("amount_sum_30d", DoubleType),
        StructField("count_sum_30d", LongType)
      )
    )

    val derivations = List(
      Derivation.star(),
      Derivation(name = "avg_amount", expression = "amount_sum_30d / count_sum_30d"),
      Derivation(name = "ds", expression = "ds"),
      Derivation(name = "composite_key", expression = "CONCAT(CAST(user_id AS STRING), '_', CAST(listing_id AS STRING), '_', ds)")
    )

    val derivedFields = DerivationUtils.buildDerivedFields(
      derivations.toJava,
      keySchema,
      baseValueSchema
    )

    // Should have: amount_sum_30d, count_sum_30d, avg_amount, ds, composite_key
    assertEquals(5, derivedFields.size)

    val fieldNames = derivedFields.map(_.name)
    assertTrue(fieldNames.contains("amount_sum_30d"))
    assertTrue(fieldNames.contains("count_sum_30d"))
    assertTrue(fieldNames.contains("avg_amount"))
    assertTrue(fieldNames.contains("ds"))
    assertTrue(fieldNames.contains("composite_key"))
  }

  @Test
  def testBuildDerivedFieldsWithoutTimeFields(): Unit = {
    // Test that derivations without time fields still work correctly
    val keySchema = StructType(
      "keys",
      Array(
        StructField("user_id", LongType)
      )
    )

    val baseValueSchema = StructType(
      "values",
      Array(
        StructField("amount", DoubleType),
        StructField("count", LongType)
      )
    )

    val derivations = List(
      Derivation(name = "amount", expression = "amount"),
      Derivation(name = "avg", expression = "amount / count")
    )

    val derivedFields = DerivationUtils.buildDerivedFields(
      derivations.toJava,
      keySchema,
      baseValueSchema
    )

    assertEquals(2, derivedFields.size)
    assertEquals("amount", derivedFields(0).name)
    assertEquals(DoubleType, derivedFields(0).fieldType)
    assertEquals("avg", derivedFields(1).name)
    assertEquals(DoubleType, derivedFields(1).fieldType)
  }

  @Test
  def testDuplicateFieldNamesInNestedStructure(): Unit = {
    // Test case for duplicate field names: top-level and nested structure have same field name
    // This simulates the scenario where a join has "updated_at_ts" at top level and also
    // within a nested "experience_data" struct, causing _REPEATED_NAME_ suffix to be added
    val keySchema = StructType(
      "keys",
      Array(
        StructField("id_home_reservation", LongType),
        StructField("id_guest", LongType)
      )
    )

    val baseValueSchema = StructType(
      "values",
      Array(
        StructField("updated_at_ts", LongType),
        StructField("experience_data", StructType(
          "experience_data",
          Array(
            StructField("id_experience_reservation", LongType),
            StructField("updated_at_ts", LongType),  // Duplicate name!
            StructField("dim_guests", IntType)
          )
        ))
      )
    )

    // Test that derivations referencing nested fields work despite duplicate names
    val derivations = List(
      Derivation.star(),
      Derivation(name = "nested_updated_ts", expression = "experience_data.updated_at_ts")
    )

    val derivedFields = DerivationUtils.buildDerivedFields(
      derivations.toJava,
      keySchema,
      baseValueSchema
    )

    // Should have: updated_at_ts, experience_data, nested_updated_ts
    assertEquals(3, derivedFields.size)

    val fieldNames = derivedFields.map(_.name)
    assertTrue(fieldNames.contains("updated_at_ts"))
    assertTrue(fieldNames.contains("experience_data"))
    assertTrue(fieldNames.contains("nested_updated_ts"))

    // Verify the nested_updated_ts has correct type
    val nestedTsField = derivedFields.find(_.name == "nested_updated_ts").get
    assertEquals(LongType, nestedTsField.fieldType)
  }
}
