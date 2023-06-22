package ai.chronon.api.test

import ai.chronon.api.{Accuracy, Builders, Constants, GroupBy}
import org.junit.Test
import ai.chronon.api.Extensions._
import org.junit.Assert.{assertEquals, assertTrue}
import org.mockito.Mockito
import org.mockito.Mockito.{doReturn, mock, spy, when}

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
    val join = Builders.Join(
      joinParts = Seq(joinPart),
      skewKeys = Map("a" -> Seq("b"), "c" -> Seq("d")))
    assertTrue(join.partSkewFilter(joinPart).nonEmpty)
    assertEquals("a NOT IN (b) OR c NOT IN (d)", join.partSkewFilter(joinPart).get)
  }

  @Test
  def partSkewFilterShouldReturnCorrectlyWithPartialSkewKeys(): Unit = {
    val groupByMetadata = Builders.MetaData(name = "test")
    val groupBy = Builders.GroupBy(keyColumns = Seq("c"), metaData = groupByMetadata)

    val joinPart = Builders.JoinPart(groupBy = groupBy)
    val join = Builders.Join(
      joinParts = Seq(joinPart),
      skewKeys = Map("a" -> Seq("b"), "c" -> Seq("d")))

    assertTrue(join.partSkewFilter(joinPart).nonEmpty)
    assertEquals("c NOT IN (d)", join.partSkewFilter(joinPart).get)
  }

  @Test
  def partSkewFilterShouldReturnCorrectlyWithSkewKeysWithMapping(): Unit = {
    val groupByMetadata = Builders.MetaData(name = "test")
    val groupBy = Builders.GroupBy(keyColumns = Seq("x", "c"), metaData = groupByMetadata)

    val joinPart = Builders.JoinPart(groupBy = groupBy, keyMapping = Map("a" -> "x"))
    val join = Builders.Join(
      joinParts = Seq(joinPart),
      skewKeys = Map("a" -> Seq("b"), "c" -> Seq("d")))

    assertTrue(join.partSkewFilter(joinPart).nonEmpty)
    assertEquals("x NOT IN (b) OR c NOT IN (d)", join.partSkewFilter(joinPart).get)
  }

  @Test
  def partSkewFilterShouldReturnNoneIfJoinPartHasNoRelatedKeys(): Unit = {
    val groupByMetadata = Builders.MetaData(name = "test")
    val groupBy = Builders.GroupBy(keyColumns = Seq("non_existent"), metaData = groupByMetadata)

    val joinPart = Builders.JoinPart(groupBy = groupBy)
    val join = Builders.Join(
      joinParts = Seq(joinPart),
      skewKeys = Map("a" -> Seq("b"), "c" -> Seq("d")))

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
}
