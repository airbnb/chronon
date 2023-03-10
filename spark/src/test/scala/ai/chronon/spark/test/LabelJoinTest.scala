package ai.chronon.spark.test

import ai.chronon.api.{Builders, Constants}
import ai.chronon.spark._
import org.apache.spark.sql.{Row, SparkSession}
import org.junit.Assert.{assertEquals, assertTrue}
import org.junit.Test

class LabelJoinTest {

  val spark: SparkSession = SparkSessionBuilder.build("LabelJoinTest", local = true)

  private val namespace = "label_join"
  private val tableName = "test_label_join"
  spark.sql(s"CREATE DATABASE IF NOT EXISTS $namespace")
  private val labelDS = "2022-10-30"
  private val tableUtils = TableUtils(spark)

  private val viewsGroupBy = TestUtils.createViewsGroupBy(namespace, spark)
  private val labelGroupBy = TestUtils.createRoomTypeGroupBy(namespace, spark)
  private val left = viewsGroupBy.groupByConf.sources.get(0)

  @Test
  def testLabelJoin(): Unit = {
    val labelGroupBy = TestUtils.createRoomTypeGroupBy(namespace, spark, "listing_attributes").groupByConf
    val labelJoinConf = createTestLabelJoin(30, 20, Seq(labelGroupBy))
    val joinConf = Builders.Join(
      Builders.MetaData(name = "test_label_join_single_label", namespace = namespace, team = "chronon"),
      left,
      labelPart = labelJoinConf
    )
    val runner = new LabelJoin(joinConf, tableUtils, labelDS)
    val computed = runner.computeLabelJoin(skipFinalJoin = true)
    println(" == Computed == ")
    computed.show()
    val expected = tableUtils.sql(s"""
                                     SELECT v.listing_id as listing,
                                        dim_room_type as listing_attributes_dim_room_type,
                                        a.ds as label_ds,
                                        v.ds
                                     FROM label_join.listing_views as v
                                     LEFT OUTER JOIN label_join.listing_attributes as a
                                     ON v.listing_id = a.listing_id
                                     WHERE a.ds = '2022-10-30'""".stripMargin)
    println(" == Expected == ")
    expected.show()
    assertEquals(computed.count(), expected.count())
    assertEquals(computed.select("label_ds").first().get(0), labelDS)

    val diff = Comparison.sideBySide(computed,
      expected,
      List("listing", "ds"))
    if (diff.count() > 0) {
      println(s"Actual count: ${computed.count()}")
      println(s"Expected count: ${expected.count()}")
      println(s"Diff count: ${diff.count()}")
      diff.show()
    }
    assertEquals(0, diff.count())
  }

  @Test
  def testLabelJoinMultiLabels(): Unit = {
    val labelGroupBy1 = TestUtils.createRoomTypeGroupBy(namespace, spark).groupByConf
    val labelGroupBy2 = TestUtils.createReservationGroupBy(namespace, spark).groupByConf
    val labelJoinConf = createTestLabelJoin(30, 20, Seq(labelGroupBy1, labelGroupBy2))
    val joinConf = Builders.Join(
      Builders.MetaData(name = tableName, namespace = namespace, team = "chronon"),
      left,
      labelPart = labelJoinConf
    )
    val runner = new LabelJoin(joinConf, tableUtils, labelDS)
    val computed = runner.computeLabelJoin(skipFinalJoin = true)
    println(" == Computed == ")
    computed.show()
    val expected = tableUtils.sql(s"""
                                     |SELECT listing,
                                     |       listing_attributes_room_dim_room_type,
                                     |       b.dim_reservations as listing_attributes_reservation_dim_reservations,
                                     |       label_ds,
                                     |       aa.ds
                                     |FROM (
                                     |  SELECT v.listing_id as listing,
                                     |         dim_room_type as listing_attributes_room_dim_room_type,
                                     |         a.ds as label_ds,
                                     |         v.ds
                                     |  FROM label_join.listing_views as v
                                     |  LEFT OUTER JOIN label_join.listing_attributes_room as a
                                     |  ON v.listing_id = a.listing_id
                                     |  WHERE a.ds = '2022-10-30'
                                     |) aa
                                     |LEFT OUTER JOIN (
                                     |  SELECT listing_id, dim_reservations
                                     |  FROM label_join.listing_attributes_reservation
                                     |  WHERE ds = '2022-10-30'
                                     |) b
                                     |ON aa.listing = b.listing_id
                                    """.stripMargin)
    println(" == Expected == ")
    expected.show()
    assertEquals(computed.count(), expected.count())
    assertEquals(computed.select("label_ds").first().get(0), labelDS)

    val diff = Comparison.sideBySide(computed, expected, List("listing", "ds"))
    if (diff.count() > 0) {
      println(s"Actual count: ${computed.count()}")
      println(s"Expected count: ${expected.count()}")
      println(s"Diff count: ${diff.count()}")
      diff.show()
    }
    assertEquals(0, diff.count())
  }

  @Test
  def testLabelDsDoesNotExist(): Unit = {
    val labelGroupBy = TestUtils.createRoomTypeGroupBy(namespace, spark, "listing_label_not_exist").groupByConf
    val labelJoinConf = createTestLabelJoin(30, 20, Seq(labelGroupBy))
    val joinConf = Builders.Join(
      Builders.MetaData(name = "test_null_label_ds", namespace = namespace, team = "chronon"),
      left,
      labelPart = labelJoinConf
    )
    // label ds does not exist in label table, labels should be null
    val runner = new LabelJoin(joinConf, tableUtils, "2022-11-01")
    val computed = runner.computeLabelJoin(skipFinalJoin = true)
    println(" == Computed == ")
    computed.show()
    assertEquals(computed.select("label_ds").first().get(0), "2022-11-01")
    assertEquals(computed
      .select("listing_attributes_dim_room_type")
      .first()
      .get(0),
      null)
  }

  @Test
  def testLabelRefresh(): Unit = {
    val labelGroupBy = TestUtils.createRoomTypeGroupBy(namespace, spark, "listing_attributes_refresh").groupByConf
    val labelJoinConf = createTestLabelJoin(60, 20, Seq(labelGroupBy))
    val joinConf = Builders.Join(
      Builders.MetaData(name = "label_refresh", namespace = namespace, team = "chronon"),
      left,
      labelPart = labelJoinConf
    )

    val runner = new LabelJoin(joinConf, tableUtils, labelDS)
    val computed = runner.computeLabelJoin(skipFinalJoin = true)
    println(" == Computed == ")
    computed.show()
    assertEquals(computed.count(), 6)
    val computedRows = computed.collect()
    // drop partition in middle to test hole logic
    tableUtils.dropPartitions(s"${namespace}.${tableName}",
                              Seq("2022-10-02"),
                              subPartitionFilters = Map(Constants.LabelPartitionColumn -> labelDS))

    val runner2 = new LabelJoin(joinConf, tableUtils, labelDS)
    val refreshed = runner2.computeLabelJoin(skipFinalJoin = true)
    println(" == Refreshed == ")
    refreshed.show()
    assertEquals(refreshed.count(), 6)
    assertTrue(computedRows sameElements (refreshed.collect()))
  }

  @Test
  def testLabelEvolution(): Unit = {
    val labelGroupBy = TestUtils.createRoomTypeGroupBy(namespace, spark, "listing_labels").groupByConf
    val labelJoinConf = createTestLabelJoin(30, 20, Seq(labelGroupBy))
    val tableName = "label_evolution"
    val joinConf = Builders.Join(
      Builders.MetaData(name = tableName, namespace = namespace, team = "chronon"),
      left,
      labelPart = labelJoinConf
    )
    val runner = new LabelJoin(joinConf, tableUtils, labelDS)
    val computed = runner.computeLabelJoin(skipFinalJoin = true)
    println(" == First Run == ")
    computed.show()
    assertEquals(computed.count(), 6)

    //add additional label column
    val updatedLabelGroupBy = TestUtils.createAttributesGroupByV2(namespace, spark, "listing_labels")
    val updatedLabelJoin = Builders.LabelPart(
      labels = Seq(
        Builders.JoinPart(groupBy = updatedLabelGroupBy.groupByConf)
      ),
      leftStartOffset = 35,
      leftEndOffset = 20
    )
    val updatedJoinConf = Builders.Join(
      Builders.MetaData(name = tableName, namespace = namespace, team = "chronon"),
      left,
      labelPart = updatedLabelJoin
    )
    val runner2 = new LabelJoin(updatedJoinConf, tableUtils, "2022-11-01")
    val updated = runner2.computeLabelJoin(skipFinalJoin = true)
    println(" == Updated Run == ")
    updated.show()
    assertEquals(updated.count(), 12)
    assertEquals(updated.where(updated("label_ds") === "2022-11-01").count(), 6)
    // expected
    // 3|  5|     15|   1|    PRIVATE_ROOM|   NEW_HOST|2022-11-11|2022-10-03|
    assertEquals(updated
                   .where(updated("label_ds") === "2022-11-01" && updated("listing") === "3")
                   .select("listing_labels_dim_host_type")
                   .first()
                   .get(0),
                 "NEW_HOST")
  }

  @Test(expected = classOf[AssertionError])
  def testLabelJoinInvalidSource(): Unit = {
    // Invalid left data model entities
    val labelJoin = Builders.LabelPart(
      labels = Seq(
        Builders.JoinPart(groupBy = labelGroupBy.groupByConf)
      ),
      leftStartOffset = 10,
      leftEndOffset = 3
    )
    val invalidLeft = labelGroupBy.groupByConf.sources.get(0)
    val invalidJoinConf = Builders.Join(
      Builders.MetaData(name = "test_invalid_label_join", namespace = namespace, team = "chronon"),
      invalidLeft,
      labelPart = labelJoin
    )
    new LabelJoin(invalidJoinConf, tableUtils, labelDS).computeLabelJoin()
  }

  @Test
  def testLabelAggregations(): Unit = {
    val rows = List(
      Row(1L, 2L, 20L, "2022-10-01 10:00:00", "2022-10-01"),
      Row(2L, 3L, 30L, "2022-10-02 10:00:00", "2022-10-02"),
      Row(3L, 1L, 10L, "2022-10-01 10:00:00", "2022-10-01"),
      Row(1L, 2L, 20L, "2022-10-02 10:00:00", "2022-10-02"),
      Row(2L, 3L, 35L, "2022-10-03 10:00:00", "2022-10-03"),
      Row(3L, 5L, 15L, "2022-10-04 10:00:00", "2022-10-03"))
    val leftSource = TestUtils.createViewsGroupBy(namespace, spark, customRows = rows)
      .groupByConf.sources.get(0)

    val labelJoinConf = createTestLabelJoinWithAgg(30, 20)
    val joinConf = Builders.Join(
      Builders.MetaData(name = tableName, namespace = namespace, team = "chronon"),
      leftSource,
      labelPart = labelJoinConf
    )
    val runner = new LabelJoin(joinConf, tableUtils, labelDS)
    val computed = runner.computeLabelJoin(skipFinalJoin = true)
    println(" == Computed == ")
    computed.show()
    val expected = tableUtils.sql(s"""
                                     SELECT v.listing_id as listing,
                                        dim_bedrooms as listing_attributes_dim_bedrooms,
                                        dim_room_type as listing_attributes_dim_room_type,
                                        a.ds as label_ds,
                                        v.ds
                                     FROM label_join.listing_views as v
                                     LEFT OUTER JOIN label_join.listing_attributes as a
                                     ON v.listing_id = a.listing_id
                                     WHERE a.ds = '2022-10-30'""".stripMargin)
    println(" == Expected == ")
    expected.show()
    assertEquals(computed.count(), expected.count())
    assertEquals(computed.select("label_ds").first().get(0), labelDS)

    val diff = Comparison.sideBySide(computed, expected, List("listing", "ds"))
    if (diff.count() > 0) {
      println(s"Actual count: ${computed.count()}")
      println(s"Expected count: ${expected.count()}")
      println(s"Diff count: ${diff.count()}")
      diff.show()
    }
    assertEquals(0, diff.count())
  }

  def createTestLabelJoin(startOffset: Int,
                          endOffset: Int,
                          groupBys: Seq[ai.chronon.api.GroupBy]): ai.chronon.api.LabelPart = {
    val labelJoinParts = groupBys.map(gb => Builders.JoinPart(groupBy = gb)).toList
    Builders.LabelPart(
      labels = labelJoinParts,
      leftStartOffset = startOffset,
      leftEndOffset = endOffset
    )
  }

  def createTestLabelJoinWithAgg(startOffset: Int,
                                 endOffset: Int,
                                 groupByTableName: String = "listing_labels"): ai.chronon.api.LabelPart = {
    val labelGroupBy = TestUtils.createLabelGroupByWithAgg(namespace, spark, groupByTableName)
    Builders.LabelPart(
      labels = Seq(
        Builders.JoinPart(groupBy = labelGroupBy.groupByConf)
      ),
      leftStartOffset = startOffset,
      leftEndOffset = endOffset
    )
  }
}
