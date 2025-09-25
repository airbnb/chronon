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

package ai.chronon.api.test

import ai.chronon.api.{Accuracy, Builders, Constants, GroupBy}
import org.junit.Test
import ai.chronon.api.Extensions._
import org.junit.Assert.{assertEquals, assertFalse, assertNotEquals, assertTrue}
import org.mockito.Mockito.{spy, when}

import scala.util.ScalaJavaConversions.JListOps
import java.util.Arrays

class ExtensionsTest {

  @Test
  def testSubPartitionFilters(): Unit = {
    val source = Builders.Source.events(query = null, table = "db.table/system=mobile/currency=USD")
    assertEquals(
      Map("system" -> "mobile", "currency" -> "USD"),
      source.subPartitionFilters
    )
  }

  @Test
  def testOwningTeam(): Unit = {
    val metadata =
      Builders.MetaData(
        customJson = "{\"check_consistency\": true, \"lag\": 0, \"team_override\": \"ml_infra\"}",
        team = "chronon"
      )

    assertEquals(
      "ml_infra",
      metadata.owningTeam
    )

    assertEquals(
      "chronon",
      metadata.team
    )

    val metadata2 =
      Builders.MetaData(
        customJson = "{\"check_consistency\": true, \"lag\": 0}",
        team = "chronon"
      )

    assertEquals(
      "chronon",
      metadata2.owningTeam
    )

    assertEquals(
      "chronon",
      metadata2.team
    )

    val metadata3 =
      Builders.MetaData(
        customJson = null,
        team = "chronon"
      )

    assertEquals(
      "chronon",
      metadata3.owningTeam
    )

    assertEquals(
      "chronon",
      metadata3.team
    )
  }

  @Test
  def testRowIdentifier(): Unit = {
    val labelPart = Builders.LabelPart();
    val res = labelPart.rowIdentifier(Arrays.asList("yoyo", "yujia"), "ds")
    assertTrue(res.contains("ds"))
  }

  @Test
  def partSkewFilterShouldReturnNoneWhenNoSkewKey(): Unit = {
    val joinPart = Builders.JoinPart()
    val join = Builders.Join(joinParts = Seq(joinPart))
    assertTrue(join.partSkewFilter(joinPart).isEmpty)
  }

  @Test
  def partSkewFilterShouldReturnCorrectlyWithSkewKeys(): Unit = {
    val groupByMetadata = Builders.MetaData(name = "test")
    val groupBy = Builders.GroupBy(keyColumns = Seq("a", "c"), metaData = groupByMetadata)
    val joinPart = Builders.JoinPart(groupBy = groupBy)
    val join = Builders.Join(joinParts = Seq(joinPart), skewKeys = Map("a" -> Seq("b"), "c" -> Seq("d")))
    assertTrue(join.partSkewFilter(joinPart).nonEmpty)
    assertEquals("a NOT IN (b) OR c NOT IN (d)", join.partSkewFilter(joinPart).get)
  }

  @Test
  def partSkewFilterShouldReturnCorrectlyWithPartialSkewKeys(): Unit = {
    val groupByMetadata = Builders.MetaData(name = "test")
    val groupBy = Builders.GroupBy(keyColumns = Seq("c"), metaData = groupByMetadata)

    val joinPart = Builders.JoinPart(groupBy = groupBy)
    val join = Builders.Join(joinParts = Seq(joinPart), skewKeys = Map("a" -> Seq("b"), "c" -> Seq("d")))

    assertTrue(join.partSkewFilter(joinPart).nonEmpty)
    assertEquals("c NOT IN (d)", join.partSkewFilter(joinPart).get)
  }

  @Test
  def partSkewFilterShouldReturnCorrectlyWithSkewKeysWithMapping(): Unit = {
    val groupByMetadata = Builders.MetaData(name = "test")
    val groupBy = Builders.GroupBy(keyColumns = Seq("x", "c"), metaData = groupByMetadata)

    val joinPart = Builders.JoinPart(groupBy = groupBy, keyMapping = Map("a" -> "x"))
    val join = Builders.Join(joinParts = Seq(joinPart), skewKeys = Map("a" -> Seq("b"), "c" -> Seq("d")))

    assertTrue(join.partSkewFilter(joinPart).nonEmpty)
    assertEquals("x NOT IN (b) OR c NOT IN (d)", join.partSkewFilter(joinPart).get)
  }

  @Test
  def partSkewFilterShouldReturnNoneIfJoinPartHasNoRelatedKeys(): Unit = {
    val groupByMetadata = Builders.MetaData(name = "test")
    val groupBy = Builders.GroupBy(keyColumns = Seq("non_existent"), metaData = groupByMetadata)

    val joinPart = Builders.JoinPart(groupBy = groupBy)
    val join = Builders.Join(joinParts = Seq(joinPart), skewKeys = Map("a" -> Seq("b"), "c" -> Seq("d")))

    assertTrue(join.partSkewFilter(joinPart).isEmpty)
  }

  @Test
  def groupByKeysShouldContainPartitionColumn(): Unit = {
    val groupBy = spy(new GroupBy())
    val baseKeys = List("a", "b")
    val partitionColumn = "ds"
    groupBy.accuracy = Accuracy.SNAPSHOT
    groupBy.keyColumns = baseKeys.toJava
    when(groupBy.isSetKeyColumns).thenReturn(true)

    val keys = groupBy.keys(partitionColumn)
    assertTrue(baseKeys.forall(keys.contains(_)))
    assertTrue(keys.contains(partitionColumn))
    assertEquals(3, keys.size)
  }

  @Test
  def groupByKeysShouldContainTimeColumnForTemporalAccuracy(): Unit = {
    val groupBy = spy(new GroupBy())
    val baseKeys = List("a", "b")
    val partitionColumn = "ds"
    groupBy.accuracy = Accuracy.TEMPORAL
    groupBy.keyColumns = baseKeys.toJava
    when(groupBy.isSetKeyColumns).thenReturn(true)

    val keys = groupBy.keys(partitionColumn)
    assertTrue(baseKeys.forall(keys.contains(_)))
    assertTrue(keys.contains(partitionColumn))
    assertTrue(keys.contains(Constants.TimeColumn))
    assertEquals(4, keys.size)
  }

  @Test
  def testIsTilingEnabled(): Unit = {
    def buildGroupByWithCustomJson(customJson: String = null): GroupBy =
      Builders.GroupBy(
        metaData = Builders.MetaData(name = "featureGroupName", customJson = customJson)
      )

    // customJson not set defaults to false
    assertFalse(buildGroupByWithCustomJson().isTilingEnabled)
    assertFalse(buildGroupByWithCustomJson("{}").isTilingEnabled)

    assertTrue(buildGroupByWithCustomJson("{\"enable_tiling\": true}").isTilingEnabled)
    assertFalse(buildGroupByWithCustomJson("{\"enable_tiling\": false}").isTilingEnabled)
    assertFalse(buildGroupByWithCustomJson("{\"enable_tiling\": \"string instead of bool\"}").isTilingEnabled)
  }

  @Test
  def semanticHashWithoutChangesIsEqual(): Unit = {
    val metadata = Builders.MetaData(name = "test")
    val groupBy = Builders.GroupBy(
      sources = Seq(Builders.Source.events(query = null, table = "db.gb_table", topic = "test.gb_topic")),
      keyColumns = Seq("a", "c"),
      metaData = metadata)
    val join = Builders.Join(
      left = Builders.Source.events(query = null, table = "db.join_table", topic = "test.join_topic"),
      joinParts = Seq(Builders.JoinPart(groupBy = groupBy)),
      metaData = metadata
    )
    assertEquals(join.semanticHash(excludeTopic = true), join.semanticHash(excludeTopic = true))
    assertEquals(join.semanticHash(excludeTopic = false), join.semanticHash(excludeTopic = false))
  }

  @Test
  def semanticHashWithChangesIsDifferent(): Unit = {
    val metadata = Builders.MetaData(name = "test")
    val groupBy = Builders.GroupBy(
      sources = Seq(Builders.Source.events(query = null, table = "db.gb_table", topic = "test.gb_topic")),
      keyColumns = Seq("a", "c"),
      metaData = metadata)
    val join1 = Builders.Join(
      left = Builders.Source.events(query = null, table = "db.join_table", topic = "test.join_topic"),
      joinParts = Seq(Builders.JoinPart(groupBy = groupBy)),
      metaData = metadata
    )
    val join2 = join1.deepCopy()
    join2.joinParts.get(0).groupBy.setKeyColumns(Seq("b", "c").toJava)
    assertNotEquals(join1.semanticHash(excludeTopic = true), join2.semanticHash(excludeTopic = true))
    assertNotEquals(join1.semanticHash(excludeTopic = false), join2.semanticHash(excludeTopic = false))
  }

  @Test
  def semanticHashIgnoresMetadataDescriptions(): Unit = {
    val metadata = Builders.MetaData(name = "test", description = "metadata description")
    val groupBy = Builders.GroupBy(
      sources = Seq(Builders.Source.events(query = null, table = "db.gb_table", topic = "test.gb_topic")),
      keyColumns = Seq("a", "c"),
      metaData = metadata)
    val join1 = Builders.Join(
      left = Builders.Source.events(query = null, table = "db.join_table", topic = "test.join_topic"),
      joinParts = Seq(Builders.JoinPart(groupBy = groupBy)),
      metaData = metadata,
      derivations = Seq(Builders.Derivation(name = "*", expression = "*", metaData = metadata))
    )
    val updatedMetadata = Builders.MetaData(name = "test", description = "other description")
    val join2 = join1.deepCopy()
    join2.setMetaData(updatedMetadata)
    join2.joinParts.get(0).groupBy.setMetaData(updatedMetadata)
    join2.derivations.get(0).setMetaData(updatedMetadata)
    assertEquals(join1.semanticHash(excludeTopic = true), join2.semanticHash(excludeTopic = true))
    assertEquals(join1.semanticHash(excludeTopic = false), join2.semanticHash(excludeTopic = false))
  }

  @Test
  def semanticHashIgnoresMetadataAttributes(): Unit = {
    val metadata = Builders.MetaData(name = "test", attributes = Map("owner" -> "Lucie"))
    val groupBy = Builders.GroupBy(
      sources = Seq(Builders.Source.events(query = null, table = "db.gb_table", topic = "test.gb_topic")),
      keyColumns = Seq("a", "c"),
      metaData = metadata)
    val join1 = Builders.Join(
      left = Builders.Source.events(query = null, table = "db.join_table", topic = "test.join_topic"),
      joinParts = Seq(Builders.JoinPart(groupBy = groupBy)),
      metaData = metadata,
      derivations = Seq(Builders.Derivation(name = "*", expression = "*", metaData = metadata))
    )
    val updatedMetadata = Builders.MetaData(name = "test", attributes = Map("owner" -> "Katie"))
    val join2 = join1.deepCopy()
    join2.setMetaData(updatedMetadata)
    join2.joinParts.get(0).groupBy.setMetaData(updatedMetadata)
    join2.derivations.get(0).setMetaData(updatedMetadata)
    assertEquals(join1.semanticHash(excludeTopic = true), join2.semanticHash(excludeTopic = true))
    assertEquals(join1.semanticHash(excludeTopic = false), join2.semanticHash(excludeTopic = false))
  }
}
