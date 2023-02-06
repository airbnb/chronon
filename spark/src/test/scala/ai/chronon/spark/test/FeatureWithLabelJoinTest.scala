package ai.chronon.spark.test

import ai.chronon.api.Extensions.{LabelPartOps, MetadataOps}
import ai.chronon.api.{Builders, LongType, StringType, StructField, StructType}
import ai.chronon.spark.{Comparison, LabelJoin, SparkSessionBuilder, TableUtils}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions.{max, min}
import org.junit.Assert.assertEquals
import org.junit.Test

class FeatureWithLabelJoinTest {
  val spark: SparkSession = SparkSessionBuilder.build("FeatureWithLabelJoinTest", local = true)

  private val namespace = "final_join"
  private val tableName = "test_feature_label_join"
  spark.sql(s"CREATE DATABASE IF NOT EXISTS $namespace")
  private val tableUtils = TableUtils(spark)

  private val labelDS = "2022-10-30"
  private val viewsGroupBy = TestUtils.createViewsGroupBy(namespace, spark)
  private val left = viewsGroupBy.groupByConf.sources.get(0)

  @Test
  def testFinalViews(): Unit = {
    // create test feature join table
    val featureTable = s"${namespace}.${tableName}"
    createTestFeatureTable().write.saveAsTable(featureTable)

    val labelJoinConf = createTestLabelJoin(50, 20)
    val joinConf = Builders.Join(
      Builders.MetaData(name = tableName, namespace = namespace, team = "chronon"),
      left,
      labelPart = labelJoinConf
    )

    val runner = new LabelJoin(joinConf, tableUtils, labelDS)
    val labelDf = runner.computeLabelJoin()
    println(" == First Run Label version 2022-10-30 == ")
    prefixColumnName(labelDf, exceptions = labelJoinConf.rowIdentifier()).show()
    val featureDf = tableUtils.sparkSession.table(joinConf.metaData.outputTable)
    println(" == Features == ")
    featureDf.show()
    val computed = tableUtils.sql(s"select * from ${joinConf.metaData.outputFinalView}")
    computed.show()
    val expectedFinal = featureDf.join(prefixColumnName(labelDf, exceptions = labelJoinConf.rowIdentifier()),
      labelJoinConf.rowIdentifier(),
      "left_outer")
    println(" == Expected == ")
    expectedFinal.show()
    val diff = Comparison.sideBySide(computed,
      expectedFinal,
      List("listing",
        "ds",
        "label_ds"))
    if (diff.count() > 0) {
      println(s"Actual count: ${computed.count()}")
      println(s"Expected count: ${expectedFinal.count()}")
      println(s"Diff count: ${diff.count()}")
      println(s"diff result rows")
      diff.show()
    }
    assertEquals(0, diff.count())

    // add another label version
    val secondRun = new LabelJoin(joinConf, tableUtils, "2022-11-11")
    val secondLabel = secondRun.computeLabelJoin()
    println(" == Second Run Label version 2022-11-11 == ")
    secondLabel.show()
    val view = tableUtils.sql(s"select * from ${joinConf.metaData.outputFinalView} order by label_ds")
    view.show()
    // listing 4 should not have any 2022-11-11 version labels
    assertEquals(null, view.where(view("label_ds") === "2022-11-11" && view("listing") === "4")
      .select("label_listing_labels_dim_bedrooms").first().get(0))
    // 11-11 label record number should be same as 10-30 label version record number
    assertEquals(view.where(view("label_ds") === "2022-10-30").count(),
      view.where(view("label_ds") === "2022-11-11").count())
    // listing 5 should not not have any label
    assertEquals(null, view.where(view("listing") === "5")
      .select("label_ds").first().get(0))

    //validate the latest label view
    val latest = tableUtils.sql(s"select * from ${joinConf.metaData.outputLatestLabelView} order by label_ds")
    latest.show()
    // latest label should be all same "2022-11-11"
    assertEquals(latest.agg(max("label_ds")).first().getString(0),
                 latest.agg(min("label_ds")).first().getString(0))
    assertEquals("2022-11-11", latest.agg(max("label_ds")).first().getString(0))
  }

  private def prefixColumnName(df: DataFrame,
                               prefix: String = "label_",
                               exceptions: Array[String] = null): DataFrame = {
    println("exceptions")
    println(exceptions.mkString(", "))
    val renamedColumns = df.columns
      .map(col => {
        if(exceptions.contains(col) || col.startsWith(prefix)) {
          df(col)
        } else {
          df(col).as(s"$prefix$col")
        }
      })
    df.select(renamedColumns: _*)
  }

  def createTestLabelJoin(startOffset: Int,
                          endOffset: Int,
                          groupByTableName: String = "listing_labels"): ai.chronon.api.LabelPart = {
    val labelGroupBy = TestUtils.createAttributesGroupBy(namespace, spark, groupByTableName)
    Builders.LabelPart(
      labels = Seq(
        Builders.JoinPart(groupBy = labelGroupBy.groupByConf)
      ),
      leftStartOffset = startOffset,
      leftEndOffset = endOffset
    )
  }

  def createTestFeatureTable(): DataFrame = {
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
    val rows = List(
      Row(1L, 20L, "US", "2022-10-01", "2022-10-01 10:00:00"),
      Row(2L, 38L, "US", "2022-10-02", "2022-10-02 11:00:00"),
      Row(3L, 19L, "CA", "2022-10-01", "2022-10-01 08:00:00"),
      Row(4L, 2L, "MX", "2022-10-02", "2022-10-02 18:00:00"),
      Row(5L, 139L, "EU", "2022-10-01", "2022-10-01 22:00:00"),
      Row(1L, 24L, "US", "2022-10-02", "2022-10-02 16:00:00")
    )
    TestUtils.makeDf(spark, schema, rows)
  }
}
