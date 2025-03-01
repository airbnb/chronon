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
import ai.chronon.api.Extensions.{LabelPartOps, MetadataOps}
import ai.chronon.api.{Builders, LongType, StringType, StructField, StructType}
import ai.chronon.spark.{Comparison, LabelJoin, SparkSessionBuilder, TableUtils}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions.{max, min}
import org.junit.Assert.assertEquals
import org.junit.Test

import scala.util.Random

class FeatureWithLabelJoinTest {
  @transient lazy val logger = LoggerFactory.getLogger(getClass)
  val spark: SparkSession = SparkSessionBuilder.build("FeatureWithLabelJoinTest", local = true)
  private val tableName = "test_feature_label_join"
  private val tableUtils = TableUtils(spark)
  private val labelDS = "2022-10-30"

  @Test
  def testFinalViews(): Unit = {
    val namespace = "final_join" + "_" + Random.alphanumeric.take(6).mkString
    tableUtils.createDatabase(namespace)
    val viewsGroupBy = TestUtils.createViewsGroupBy(namespace, spark)
    val left = viewsGroupBy.groupByConf.sources.get(0)
    // Create feature to be joned
    createTestFeatureTable(namespace, tableName = tableName)

    val labelJoinConf = createTestLabelJoin(50, 20,namespace=namespace)
    val joinConf = Builders.Join(
      Builders.MetaData(name = tableName, namespace = namespace, team = "chronon"),
      left,
      joinParts = Seq.empty,
      labelPart = labelJoinConf
    )

    val runner = new LabelJoin(joinConf, tableUtils, labelDS)
    val labelDf = runner.computeLabelJoin()
    logger.info(" == First Run Label version 2022-10-30 == ")
    prefixColumnName(labelDf, exceptions = labelJoinConf.rowIdentifier(null, tableUtils.partitionColumn))
      .show()
    val featureDf = tableUtils.sparkSession.table(joinConf.metaData.outputTable)
    logger.info(" == Features == ")
    featureDf.show()
    val computed = tableUtils.sql(s"select * from ${joinConf.metaData.outputFinalView}")
    val expectedFinal = featureDf.join(
      prefixColumnName(labelDf, exceptions = labelJoinConf.rowIdentifier(null, tableUtils.partitionColumn)),
      labelJoinConf.rowIdentifier(null, tableUtils.partitionColumn),
      "left_outer"
    )
    assertResult(computed, expectedFinal)

    // add another label version
    val secondRun = new LabelJoin(joinConf, tableUtils, "2022-11-11")
    val secondLabel = secondRun.computeLabelJoin()
    logger.info(" == Second Run Label version 2022-11-11 == ")
    secondLabel.show()
    val view = tableUtils.sql(s"select * from ${joinConf.metaData.outputFinalView} order by label_ds")
    view.show()
    // listing 4 should not have any 2022-11-11 version labels
    assertEquals(null,
                 view
                   .where(view("label_ds") === "2022-11-11" && view("listing") === "4")
                   .select("label_listing_labels_dim_room_type")
                   .first()
                   .get(0))
    // 11-11 label record number should be same as 10-30 label version record number
    assertEquals(view.where(view("label_ds") === "2022-10-30").count(),
                 view.where(view("label_ds") === "2022-11-11").count())
    // listing 5 should not not have any label
    assertEquals(null,
                 view
                   .where(view("listing") === "5")
                   .select("label_ds")
                   .first()
                   .get(0))

    //validate the latest label view
    val latest = tableUtils.sql(s"select * from ${joinConf.metaData.outputLatestLabelView} order by label_ds")
    latest.show()
    // latest label should be all same "2022-11-11"
    assertEquals(latest.agg(max("label_ds")).first().getString(0), latest.agg(min("label_ds")).first().getString(0))
    assertEquals("2022-11-11", latest.agg(max("label_ds")).first().getString(0))
  }

  @Test
  def testFinalViewsGroupByDifferentPartitionColumn(): Unit = {
    val namespace = "final_join" + "_" + Random.alphanumeric.take(6).mkString
    tableUtils.createDatabase(namespace)
    val viewPartitionCol = "view_date"
    val viewsGroupBy = TestUtils.createViewsGroupBy(namespace, spark, partitionColOpt = Some(viewPartitionCol))
    val left = viewsGroupBy.groupByConf.sources.get(0)
    // Create feature to be joined
    createTestFeatureTable(namespace, tableName = tableName)

    val labelJoinConf = createTestLabelJoin(50, 20,namespace=namespace)
    val joinConf = Builders.Join(
      Builders.MetaData(name = tableName, namespace = namespace, team = "chronon"),
      left,
      joinParts = Seq.empty,
      labelPart = labelJoinConf
    )

    val runner = new LabelJoin(joinConf, tableUtils, labelDS)
    val labelDf = runner.computeLabelJoin()
    logger.info(" == First Run Label version 2022-10-30 == ")
    prefixColumnName(labelDf, exceptions = labelJoinConf.rowIdentifier(null, tableUtils.partitionColumn))
      .show()
    val featureDf = tableUtils.sparkSession.table(joinConf.metaData.outputTable)
    logger.info(" == Features == ")
    featureDf.show()
    val computed = tableUtils.sql(s"select * from ${joinConf.metaData.outputFinalView}")
    val expectedFinal = featureDf.join(
      prefixColumnName(labelDf, exceptions = labelJoinConf.rowIdentifier(null, tableUtils.partitionColumn)),
      labelJoinConf.rowIdentifier(null, tableUtils.partitionColumn),
      "left_outer"
    )
    assertResult(computed, expectedFinal)
  }

  @Test
  def testFinalViewsWithAggLabel(): Unit = {
    val namespace = "final_join" + "_" + Random.alphanumeric.take(6).mkString
    tableUtils.createDatabase(namespace)
    // create test feature join table
    val tableName = "label_agg_table"
    val featureRows = List(
      Row(1L, 24L, "US", "2022-10-02", "2022-10-02 16:00:00"),
      Row(1L, 20L, "US", "2022-10-03", "2022-10-03 10:00:00"),
      Row(2L, 38L, "US", "2022-10-02", "2022-10-02 11:00:00"),
      Row(3L, 41L, "US", "2022-10-02", "2022-10-02 22:00:00"),
      Row(3L, 19L, "CA", "2022-10-03", "2022-10-03 08:00:00"),
      Row(4L, 2L, "MX", "2022-10-02", "2022-10-02 18:00:00")
    )
    createTestFeatureTable(namespace, tableName, featureRows)

    val rows = List(
      Row(1L, 20L, "2022-10-02 11:00:00", "2022-10-02"),
      Row(2L, 30L, "2022-10-02 11:00:00", "2022-10-02"),
      Row(3L, 10L, "2022-10-02 11:00:00", "2022-10-02"),
      Row(1L, 20L, "2022-10-03 11:00:00", "2022-10-03"),
      Row(2L, 35L, "2022-10-03 11:00:00", "2022-10-03"),
      Row(3L, 15L, "2022-10-03 11:00:00", "2022-10-03")
    )
    val leftSource = TestUtils
      .createViewsGroupBy(namespace, spark, tableName = "listing_view_agg", customRows = rows)
      .groupByConf
      .sources
      .get(0)
    val labelJoinConf = createTestAggLabelJoin(5, "listing_labels_agg", namespace=namespace)
    val joinConf = Builders.Join(
      Builders.MetaData(name = tableName, namespace = namespace, team = "chronon"),
      leftSource,
      joinParts = Seq.empty,
      labelPart = labelJoinConf
    )

    val runner = new LabelJoin(joinConf, tableUtils, "2022-10-06")
    val labelDf = runner.computeLabelJoin()
    logger.info(" == Label DF == ")
    prefixColumnName(labelDf, exceptions = labelJoinConf.rowIdentifier(null, tableUtils.partitionColumn))
      .show()
    val featureDf = tableUtils.sparkSession.table(joinConf.metaData.outputTable)
    logger.info(" == Features DF == ")
    featureDf.show()
    val computed = tableUtils.sql(s"select * from ${joinConf.metaData.outputFinalView}")
    val expectedFinal = featureDf.join(
      prefixColumnName(labelDf, exceptions = labelJoinConf.rowIdentifier(null, tableUtils.partitionColumn)),
      labelJoinConf.rowIdentifier(null, tableUtils.partitionColumn),
      "left_outer"
    )
    assertResult(computed, expectedFinal)

    // add new labels
    val newLabelRows = List(
      Row(1L, 0, "2022-10-07", "2022-10-07 11:00:00"),
      Row(2L, 2, "2022-10-07", "2022-10-07 11:00:00"),
      Row(3L, 2, "2022-10-07", "2022-10-07 11:00:00")
    )
    TestUtils.createOrUpdateLabelGroupByWithAgg(namespace, spark, 5, "listing_labels_agg", newLabelRows)
    val runner2 = new LabelJoin(joinConf, tableUtils, "2022-10-07")
    val updatedLabelDf = runner2.computeLabelJoin()
    updatedLabelDf.show()

    //validate the label view
    val latest = tableUtils.sql(s"select * from ${joinConf.metaData.outputLatestLabelView} order by label_ds")
    latest.show()
    assertEquals(2,
                 latest
                   .where(latest("listing") === "3" && latest("ds") === "2022-10-03")
                   .select("label_listing_labels_agg_is_active_max_5d")
                   .first()
                   .get(0))
    assertEquals("2022-10-07",
                 latest
                   .where(latest("listing") === "1" && latest("ds") === "2022-10-03")
                   .select("label_ds")
                   .first()
                   .get(0))
  }

  private def assertResult(computed: DataFrame, expected: DataFrame): Unit = {
    logger.info(" == Computed == ")
    computed.show()
    logger.info(" == Expected == ")
    expected.show()
    val diff = Comparison.sideBySide(computed, expected, List("listing", "ds", "label_ds"))
    if (diff.count() > 0) {
      logger.info(s"Actual count: ${computed.count()}")
      logger.info(s"Expected count: ${expected.count()}")
      logger.info(s"Diff count: ${diff.count()}")
      logger.info(s"diff result rows")
      diff.show()
    }
    assertEquals(0, diff.count())
  }

  private def prefixColumnName(df: DataFrame,
                               prefix: String = "label_",
                               exceptions: Array[String] = null): DataFrame = {
    logger.info("exceptions")
    logger.info(exceptions.mkString(", "))
    val renamedColumns = df.columns
      .map(col => {
        if (exceptions.contains(col) || col.startsWith(prefix)) {
          df(col)
        } else {
          df(col).as(s"$prefix$col")
        }
      })
    df.select(renamedColumns: _*)
  }

  def createTestLabelJoin(startOffset: Int,
                          endOffset: Int,
                          groupByTableName: String = "listing_labels",
                          namespace: String): ai.chronon.api.LabelPart = {
    val labelGroupBy = TestUtils.createRoomTypeGroupBy(namespace, spark, groupByTableName)
    Builders.LabelPart(
      labels = Seq(
        Builders.JoinPart(groupBy = labelGroupBy.groupByConf)
      ),
      leftStartOffset = startOffset,
      leftEndOffset = endOffset
    )
  }

  def createTestAggLabelJoin(windowSize: Int,
                             groupByTableName: String = "listing_labels_agg",
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

  def createTestFeatureTable(namespace: String, tableName: String = tableName, customRows: List[Row] = List.empty): String = {
    val schema = StructType(
      tableName,
      Array(
        StructField("listing", LongType),
        StructField("feature_review", LongType),
        StructField("feature_locale", StringType),
        StructField("ds", StringType),
        StructField("ts", StringType)
      )
    )
    val rows = if (customRows.isEmpty) {
      List(
        Row(1L, 20L, "US", "2022-10-01", "2022-10-01 10:00:00"),
        Row(2L, 38L, "US", "2022-10-02", "2022-10-02 11:00:00"),
        Row(3L, 19L, "CA", "2022-10-01", "2022-10-01 08:00:00"),
        Row(4L, 2L, "MX", "2022-10-02", "2022-10-02 18:00:00"),
        Row(5L, 139L, "EU", "2022-10-01", "2022-10-01 22:00:00"),
        Row(1L, 24L, "US", "2022-10-02", "2022-10-02 16:00:00")
      )
    } else customRows
    val df = TestUtils.makeDf(spark, schema, rows)
    val fullTableName = s"$namespace.$tableName"
    TestUtils.saveOnPartitionOpt(df, fullTableName, None)
    fullTableName
  }
}
