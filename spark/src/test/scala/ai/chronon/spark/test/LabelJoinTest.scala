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

package ai.chronon.spark.test

import org.slf4j.LoggerFactory
import ai.chronon.api.{Accuracy, Builders, Constants, Operation, TimeUnit, Window}
import ai.chronon.spark._
import org.apache.spark.sql.{Row, SparkSession}
import org.junit.Assert.assertEquals
import org.junit.Test

import scala.util.Random

class LabelJoinTest {
  @transient private lazy val logger = LoggerFactory.getLogger(getClass)

  val spark: SparkSession = SparkSessionBuilder.build("LabelJoinTest", local = true)
  private val tableName = "test_label_join"
  private val labelDS = "2022-10-30"
  private val tableUtils = TableUtils(spark)

  def createViewsGroupBy(namespace: String, partitionColOpt: Option[String] = None): GroupByTestSuite = {
    TestUtils.createViewsGroupBy(namespace, spark, partitionColOpt=partitionColOpt)
  }
  @Test
  def testLabelJoin(): Unit = {
    val namespace = "label_join" + "_" + Random.alphanumeric.take(6).mkString
    tableUtils.createDatabase(namespace)
    val viewsGroupBy = createViewsGroupBy(namespace)
    val left = viewsGroupBy.groupByConf.sources.get(0)
    val labelGroupBy = TestUtils.createRoomTypeGroupBy(namespace, spark, "listing_attributes").groupByConf
    val labelJoinConf = createTestLabelJoin(30, 20, Seq(labelGroupBy))
    val joinConf = Builders.Join(
      Builders.MetaData(name = "test_label_join_single_label", namespace = namespace, team = "chronon"),
      left,
      labelPart = labelJoinConf
    )
    val runner = new LabelJoin(joinConf, tableUtils, labelDS)
    val computed = runner.computeLabelJoin(skipFinalJoin = true)
    logger.info(" == Computed == ")
    computed.show()
    val expected = tableUtils.sql(s"""
                                     SELECT v.listing_id as listing,
                                        dim_room_type as listing_attributes_dim_room_type,
                                        a.ds as label_ds,
                                        v.ds
                                     FROM ${namespace}.listing_views as v
                                     LEFT OUTER JOIN ${namespace}.listing_attributes as a
                                     ON v.listing_id = a.listing_id
                                     WHERE a.ds = '2022-10-30'""".stripMargin)
    logger.info(" == Expected == ")
    expected.show()
    assertEquals(computed.count(), expected.count())
    assertEquals(computed.select("label_ds").first().get(0), labelDS)

    val diff = Comparison.sideBySide(computed, expected, List("listing", "ds"))
    if (diff.count() > 0) {
      logger.info(s"Actual count: ${computed.count()}")
      logger.info(s"Expected count: ${expected.count()}")
      logger.info(s"Diff count: ${diff.count()}")
      diff.show()
    }
    assertEquals(0, diff.count())
  }

  @Test
  def testLabelJoinMultiLabels(): Unit = {
    val namespace = "label_join" + "_" + Random.alphanumeric.take(6).mkString
    tableUtils.createDatabase(namespace)
    val viewsGroupBy = createViewsGroupBy(namespace)
    val left = viewsGroupBy.groupByConf.sources.get(0)
    val labelGroupBy1 = TestUtils.createRoomTypeGroupBy(namespace, spark).groupByConf
    val labelGroupBy2 = TestUtils.createReservationGroupBy(namespace, spark).groupByConf
    val labelJoinConf = createTestLabelJoin(30, 20, Seq(labelGroupBy1, labelGroupBy2))
    val joinConf = Builders.Join(
      Builders.MetaData(name = tableName, namespace = namespace, team = "chronon"),
      left,
      joinParts = Seq.empty,
      labelPart = labelJoinConf
    )
    val runner = new LabelJoin(joinConf, tableUtils, labelDS)
    val computed = runner.computeLabelJoin(skipFinalJoin = true)
    logger.info(" == Computed == ")
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
                                     |  FROM ${namespace}.listing_views as v
                                     |  LEFT OUTER JOIN ${namespace}.listing_attributes_room as a
                                     |  ON v.listing_id = a.listing_id
                                     |  WHERE a.ds = '2022-10-30'
                                     |) aa
                                     |LEFT OUTER JOIN (
                                     |  SELECT listing_id, dim_reservations
                                     |  FROM ${namespace}.listing_attributes_reservation
                                     |  WHERE ds = '2022-10-30'
                                     |) b
                                     |ON aa.listing = b.listing_id
                                    """.stripMargin)
    logger.info(" == Expected == ")
    expected.show()
    assertEquals(computed.count(), expected.count())
    assertEquals(computed.select("label_ds").first().get(0), labelDS)

    val diff = Comparison.sideBySide(computed, expected, List("listing", "ds"))
    if (diff.count() > 0) {
      logger.info(s"Actual count: ${computed.count()}")
      logger.info(s"Expected count: ${expected.count()}")
      logger.info(s"Diff count: ${diff.count()}")
      diff.show()
    }
    assertEquals(0, diff.count())
  }

  @Test
  def testLabelJoinMultiLabelsWithDifferentPartitionColumns(): Unit = {
    val namespace = "label_join" + "_" + Random.alphanumeric.take(6).mkString
    tableUtils.createDatabase(namespace)
    val viewPartitionCol = "view_date"
    val viewsGroupBy = createViewsGroupBy(namespace, partitionColOpt = Some(viewPartitionCol))
    val left = viewsGroupBy.groupByConf.sources.get(0)

    val roomTypePartitionCol = "room_date"
    val labelGroupBy1 = TestUtils.createRoomTypeGroupBy(namespace, spark, partitionColOpt=Some(roomTypePartitionCol)).groupByConf
    val resTypePartitionCol = "res_date"
    val labelGroupBy2 = TestUtils.createReservationGroupBy(namespace, spark, partitionColOpt = Some(resTypePartitionCol)).groupByConf
    val labelJoinConf = createTestLabelJoin(30, 20, Seq(labelGroupBy1, labelGroupBy2))
    val joinConf = Builders.Join(
      Builders.MetaData(name = tableName, namespace = namespace, team = "chronon"),
      left,
      joinParts = Seq.empty,
      labelPart = labelJoinConf
    )
    val runner = new LabelJoin(joinConf, tableUtils, labelDS)
    val computed = runner.computeLabelJoin(skipFinalJoin = true)
    logger.info(" == Computed == ")
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
                                     |         a.$roomTypePartitionCol as label_ds,
                                     |         v.$viewPartitionCol as ds
                                     |  FROM ${namespace}.listing_views as v
                                     |  LEFT OUTER JOIN ${namespace}.listing_attributes_room as a
                                     |  ON v.listing_id = a.listing_id
                                     |  WHERE a.$roomTypePartitionCol = '2022-10-30'
                                     |) aa
                                     |LEFT OUTER JOIN (
                                     |  SELECT listing_id, dim_reservations
                                     |  FROM ${namespace}.listing_attributes_reservation
                                     |  WHERE $resTypePartitionCol = '2022-10-30'
                                     |) b
                                     |ON aa.listing = b.listing_id
                                    """.stripMargin)
    logger.info(" == Expected == ")
    expected.show()
    assertEquals(computed.count(), expected.count())
    assertEquals(computed.select("label_ds").first().get(0), labelDS)

    val diff = Comparison.sideBySide(computed, expected, List("listing", "ds"))
    if (diff.count() > 0) {
      logger.info(s"Actual count: ${computed.count()}")
      logger.info(s"Expected count: ${expected.count()}")
      logger.info(s"Diff count: ${diff.count()}")
      diff.show()
    }
    assertEquals(0, diff.count())
  }

  @Test
  def testLabelDsDoesNotExist(): Unit = {
    val namespace = "label_join" + "_" + Random.alphanumeric.take(6).mkString
    tableUtils.createDatabase(namespace)
    val viewsGroupBy = createViewsGroupBy(namespace)
    val left = viewsGroupBy.groupByConf.sources.get(0)
    val labelGroupBy = TestUtils.createRoomTypeGroupBy(namespace, spark, "listing_label_not_exist").groupByConf
    val labelJoinConf = createTestLabelJoin(30, 20, Seq(labelGroupBy))
    val joinConf = Builders.Join(
      Builders.MetaData(name = "test_null_label_ds", namespace = namespace, team = "chronon"),
      left,
      joinParts = Seq.empty,
      labelPart = labelJoinConf
    )
    // label ds does not exist in label table, labels should be null
    val runner = new LabelJoin(joinConf, tableUtils, "2022-11-01")
    val computed = runner.computeLabelJoin(skipFinalJoin = true)
    logger.info(" == Computed == ")
    computed.show()
    assertEquals(computed.select("label_ds").first().get(0), "2022-11-01")
    assertEquals(computed
                   .select("listing_label_not_exist_dim_room_type")
                   .first()
                   .get(0),
                 null)
  }

  @Test
  def testLabelRefresh(): Unit = {
    val namespace = "label_join" + "_" + Random.alphanumeric.take(6).mkString
    tableUtils.createDatabase(namespace)
    val viewsGroupBy = createViewsGroupBy(namespace)
    val left = viewsGroupBy.groupByConf.sources.get(0)
    val labelGroupBy = TestUtils.createRoomTypeGroupBy(namespace, spark, "listing_attributes_refresh").groupByConf
    val labelJoinConf = createTestLabelJoin(60, 20, Seq(labelGroupBy))
    val joinConf = Builders.Join(
      Builders.MetaData(name = "label_refresh", namespace = namespace, team = "chronon"),
      left,
      joinParts = Seq.empty,
      labelPart = labelJoinConf
    )

    val runner = new LabelJoin(joinConf, tableUtils, labelDS)
    val computed = runner.computeLabelJoin(skipFinalJoin = true)
    logger.info(" == Computed == ")
    computed.show()
    assertEquals(computed.count(), 6)
    val computedRows = computed.collect()
    // drop partition in middle to test hole logic
    tableUtils.dropPartitions(s"${namespace}.${tableName}",
                              Seq("2022-10-02"),
                              subPartitionFilters = Map(Constants.LabelPartitionColumn -> labelDS))

    val runner2 = new LabelJoin(joinConf, tableUtils, labelDS)
    val refreshed = runner2.computeLabelJoin(skipFinalJoin = true)
    logger.info(" == Refreshed == ")
    refreshed.show()
    assertEquals(refreshed.count(), 6)
    val refreshedRows = refreshed.collect()
    assertEquals(computedRows.toSet, refreshedRows.toSet)
  }

  @Test
  def testLabelEvolution(): Unit = {
    val namespace = "label_join" + "_" + Random.alphanumeric.take(6).mkString
    tableUtils.createDatabase(namespace)
    val viewsGroupBy = createViewsGroupBy(namespace)
    val left = viewsGroupBy.groupByConf.sources.get(0)
    val labelGroupBy = TestUtils.createRoomTypeGroupBy(namespace, spark, "listing_labels").groupByConf
    val labelJoinConf = createTestLabelJoin(30, 20, Seq(labelGroupBy))
    val tableName = "label_evolution"
    val joinConf = Builders.Join(
      Builders.MetaData(name = tableName, namespace = namespace, team = "chronon"),
      left,
      joinParts = Seq.empty,
      labelPart = labelJoinConf
    )
    val runner = new LabelJoin(joinConf, tableUtils, labelDS)
    val computed = runner.computeLabelJoin(skipFinalJoin = true)
    logger.info(" == First Run == ")
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
      joinParts = Seq.empty,
      labelPart = updatedLabelJoin
    )
    val runner2 = new LabelJoin(updatedJoinConf, tableUtils, "2022-11-01")
    val updated = runner2.computeLabelJoin(skipFinalJoin = true)
    logger.info(" == Updated Run == ")
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
    val namespace = "label_join" + "_" + Random.alphanumeric.take(6).mkString
    tableUtils.createDatabase(namespace)
    val labelGroupBy = TestUtils.createRoomTypeGroupBy(namespace, spark)
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
      joinParts = Seq.empty,
      labelPart = labelJoin
    )
    new LabelJoin(invalidJoinConf, tableUtils, labelDS).computeLabelJoin()
  }

  @Test(expected = classOf[AssertionError])
  def testLabelJoinInvalidLabelGroupByDataModal(): Unit = {
    val namespace = "label_join" + "_" + Random.alphanumeric.take(6).mkString
    tableUtils.createDatabase(namespace)
    val viewsGroupBy = createViewsGroupBy(namespace)
    val labelGroupBy = TestUtils.createRoomTypeGroupBy(namespace, spark)
    val left = viewsGroupBy.groupByConf.sources.get(0)
    // Invalid data model entities with aggregations, expected Events
    val agg_label_conf = Builders.GroupBy(
      sources = Seq(labelGroupBy.groupByConf.sources.get(0)),
      keyColumns = Seq("listing"),
      aggregations = Seq(
        Builders.Aggregation(
          inputColumn = "is_active",
          operation = Operation.MAX,
          windows = Seq(new Window(5, TimeUnit.DAYS), new Window(10, TimeUnit.DAYS))
        )),
      accuracy = Accuracy.SNAPSHOT,
      metaData = Builders.MetaData(name = s"${tableName}", namespace = namespace, team = "chronon")
    )

    val labelJoin = Builders.LabelPart(
      labels = Seq(
        Builders.JoinPart(groupBy = agg_label_conf)
      ),
      leftStartOffset = 10,
      leftEndOffset = 3
    )

    val invalidJoinConf = Builders.Join(
      Builders.MetaData(name = "test_invalid_label_join", namespace = namespace, team = "chronon"),
      left,
      joinParts = Seq.empty,
      labelPart = labelJoin
    )
    new LabelJoin(invalidJoinConf, tableUtils, labelDS).computeLabelJoin()
  }

  @Test(expected = classOf[AssertionError])
  def testLabelJoinInvalidAggregations(): Unit = {
    val namespace = "label_join" + "_" + Random.alphanumeric.take(6).mkString
    tableUtils.createDatabase(namespace)
    val viewsGroupBy = createViewsGroupBy(namespace)
    val labelGroupBy = TestUtils.createRoomTypeGroupBy(namespace, spark)
    // multi window aggregations
    val agg_label_conf = Builders.GroupBy(
      sources = Seq(labelGroupBy.groupByConf.sources.get(0)),
      keyColumns = Seq("listing"),
      aggregations = Seq(
        Builders.Aggregation(
          inputColumn = "is_active",
          operation = Operation.MAX,
          windows = Seq(new Window(5, TimeUnit.DAYS), new Window(10, TimeUnit.DAYS))
        )),
      accuracy = Accuracy.SNAPSHOT,
      metaData = Builders.MetaData(name = s"${tableName}", namespace = namespace, team = "chronon")
    )

    val labelJoin = Builders.LabelPart(
      labels = Seq(
        Builders.JoinPart(groupBy = agg_label_conf)
      ),
      leftStartOffset = 5,
      leftEndOffset = 5
    )

    val invalidJoinConf = Builders.Join(
      Builders.MetaData(name = "test_invalid_label_join", namespace = namespace, team = "chronon"),
      viewsGroupBy.groupByConf.sources.get(0),
      joinParts = Seq.empty,
      labelPart = labelJoin
    )
    new LabelJoin(invalidJoinConf, tableUtils, labelDS).computeLabelJoin()
  }

  @Test
  def testLabelAggregations(): Unit = {
    val namespace = "label_join" + "_" + Random.alphanumeric.take(6).mkString
    tableUtils.createDatabase(namespace)
    // left : listing_id, _, _, ts, ds
    val rows = List(
      Row(1L, 20L, "2022-10-02 11:00:00", "2022-10-02"),
      Row(2L, 30L, "2022-10-02 11:00:00", "2022-10-02"),
      Row(3L, 10L, "2022-10-02 11:00:00", "2022-10-02"),
      Row(1L, 20L, "2022-10-03 11:00:00", "2022-10-03"),
      Row(2L, 35L, "2022-10-04 11:00:00", "2022-10-04"),
      Row(3L, 15L, "2022-10-05 11:00:00", "2022-10-05")
    )
    val leftSource = TestUtils
      .createViewsGroupBy(namespace, spark, tableName = "listing_view_agg", customRows = rows)
      .groupByConf
      .sources
      .get(0)

    // 5 day window
    val labelJoinConf = createTestLabelJoinWithAgg(5, namespace=namespace)
    val joinConf = Builders.Join(
      Builders.MetaData(name = "test_label_agg", namespace = namespace, team = "chronon"),
      leftSource,
      joinParts = Seq.empty,
      labelPart = labelJoinConf
    )
    val runner = new LabelJoin(joinConf, tableUtils, "2022-10-06")
    val computed = runner.computeLabelJoin(skipFinalJoin = true)
    logger.info(" == computed == ")
    computed.show()
    val expected =
      tableUtils.sql(s"""
           |SELECT listing, ds, listing_label_group_by_is_active_max_5d, DATE_ADD(ds, 4) as label_ds
           |FROM(
           | SELECT v.listing_id as listing,
           |       v.ds,
           |       MAX(is_active) as listing_label_group_by_is_active_max_5d
           | FROM ${namespace}.listing_view_agg as v
           | LEFT JOIN ${namespace}.listing_label_group_by as a
           |   ON v.listing_id = a.listing_id AND
           |     a.ds >= v.ds AND a.ds < DATE_ADD(v.ds, 5)
           | WHERE v.ds == '2022-10-02'
           | GROUP BY v.listing_id, v.ds)
           |""".stripMargin)
    logger.info(" == Expected == ")
    expected.show()
    assertEquals(computed.count(), expected.count())
    assertEquals(computed.select("label_ds").first().get(0), "2022-10-06")

    val diff = Comparison.sideBySide(computed, expected, List("listing", "ds"))
    if (diff.count() > 0) {
      logger.info(s"Actual count: ${computed.count()}")
      logger.info(s"Expected count: ${expected.count()}")
      logger.info(s"Diff count: ${diff.count()}")
      diff.show()
    }
    assertEquals(0, diff.count())
  }

  @Test
  def testLabelAggregationsWithLargerDataset(): Unit = {
    val namespace = "label_join" + "_" + Random.alphanumeric.take(6).mkString
    tableUtils.createDatabase(namespace)
    val viewsGroupBy = createViewsGroupBy(namespace)
    val labelTableName = s"$namespace.listing_status"
    val listingTableName = s"$namespace.listing_views_agg_left"
    val listingTable = TestUtils.buildListingTable(spark, listingTableName)
    val joinConf = Builders.Join(
      Builders.MetaData(name = "test_label_agg_large", namespace = namespace, team = "chronon"),
      left = Builders.Source.events(
        table = listingTable,
        query = Builders.Query()
      ),
      joinParts = Seq.empty,
      labelPart = Builders.LabelPart(
        labels = Seq(
          Builders.JoinPart(groupBy =
            TestUtils.buildLabelGroupBy(namespace, spark, windowSize = 5, tableName = labelTableName))
        ),
        leftStartOffset = 5,
        leftEndOffset = 5
      )
    )

    val now = System.currentTimeMillis()
    val today = tableUtils.partitionSpec.at(now)
    val runner = new LabelJoin(joinConf, tableUtils, today)
    val computed = runner.computeLabelJoin(skipFinalJoin = true)
    logger.info(" == computed == ")
    computed.show()

    // For window based label, given specific label_ds and window, only one ds will be updated with label.
    // The expected query would filter on this ds.
    val expected =
      tableUtils.sql(s"""
           |SELECT listing_id, ds, listing_label_table_active_status_max_5d, DATE_ADD(ds, 4) as label_ds
           |FROM(
           | SELECT v.listing_id,
           |       v.ds,
           |       MAX(active_status) as listing_label_table_active_status_max_5d
           | FROM $listingTableName as v
           | LEFT JOIN $labelTableName as a
           |   ON v.listing_id = a.listing_id AND
           |     a.ds >= v.ds AND a.ds < DATE_ADD(v.ds, 5)
           | WHERE v.ds == DATE_SUB(from_unixtime(round($now / 1000), 'yyyy-MM-dd'), 4)
           | GROUP BY v.listing_id, v.ds)
           |""".stripMargin)
    logger.info(" == Expected == ")
    expected.show()
    val diff = Comparison.sideBySide(computed, expected, List("listing_id", "ds"))
    if (diff.count() > 0) {
      logger.info(s"Actual count: ${computed.count()}")
      logger.info(s"Expected count: ${expected.count()}")
      logger.info(s"Diff count: ${diff.count()}")
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

  def createTestLabelJoinWithAgg(windowSize: Int,
                                 groupByTableName: String = "listing_label_group_by",
                                 namespace: String): ai.chronon.api.LabelPart = {
    val labelGroupBy = TestUtils.createOrUpdateLabelGroupByWithAgg(namespace, spark, windowSize, groupByTableName)
    Builders.LabelPart(
      labels = Seq(
        Builders.JoinPart(groupBy = labelGroupBy.groupByConf)
      ),
      leftStartOffset = windowSize,
      leftEndOffset = windowSize
    )
  }
}
