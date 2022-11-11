package ai.chronon.spark.test


import ai.chronon.api.{Builders, Constants, Join}
import ai.chronon.spark._
import org.apache.spark.sql.{SparkSession}
import org.junit.Assert.{assertEquals, assertTrue}
import org.junit.Test

class LabelJoinTest {

  val spark: SparkSession = SparkSessionBuilder.build("JoinTest", local = true)

  private val namespace = "label_join"
  private val tableName = "test_label_join"
  spark.sql(s"CREATE DATABASE IF NOT EXISTS $namespace")
  private val labelDS = "2022-10-30"
  private val tableUtils = TableUtils(spark)

  private val viewsGroupBy = TestUtils.createViewsGroupBy(namespace, spark)
  private val labelGroupBy = TestUtils.createAttributesGroupBy(namespace, spark)

  @Test
  def testLabelJoin(): Unit = {
    val joinConf = createTestJoin(namespace)
    val runner = new LabelJoin(joinConf, 30,20, tableUtils, labelDS)
    val computed = runner.computeLabelJoin()
    println(" == Computed == ")
    computed.show()
    val expected = tableUtils.sql(s"""
                                     SELECT v.listing_id as listing,
                                        ts,
                                        m_guests,
                                        m_views,
                                        dim_bedrooms as listing_attributes_dim_bedrooms,
                                        dim_room_type as listing_attributes_dim_room_type,
                                        v.ds
                                     FROM label_join.listing_views as v
                                     LEFT OUTER JOIN label_join.listing_attributes as a
                                     ON v.ds = a.ds AND v.listing_id = a.listing_id""".stripMargin)
    println(" == Expected == ")
    expected.show()
    assertEquals(computed.count(), expected.count())
    assertEquals(computed.select("label_ds").first().get(0), labelDS)

    val diff = Comparison.sideBySide(computed.drop("label_ds"), expected,
      List("listing", "ds", "m_guests", "m_views",
        "listing_attributes_dim_room_type", "listing_attributes_dim_bedrooms"))
    if (diff.count() > 0) {
      println(s"Actual count: ${computed.count()}")
      println(s"Expected count: ${expected.count()}")
      println(s"Diff count: ${diff.count()}")
      println(s"diff result rows")
      diff.show()
    }
    assertEquals(0, diff.count())
  }

  @Test
  def testLabelRefresh(): Unit = {
    val joinConf = createTestJoin(namespace)
    val runner = new LabelJoin(joinConf, 60,20, tableUtils, labelDS)
    val computed = runner.computeLabelJoin()
    println(" == Computed == ")
    computed.show()
    assertEquals(computed.count(),6)
    val computedRows = computed.collect()
    // drop partition in middle to test hole logic
    tableUtils.dropPartitions(s"${namespace}.${tableName}",
      Seq("2022-10-02"),
      labelPartition = Option(s"${labelDS}"))

    val runner2 = new LabelJoin(joinConf, 60,20, tableUtils, labelDS)
    val refreshed = runner2.computeLabelJoin()
    println(" == Refreshed == ")
    refreshed.show()
    assertEquals(refreshed.count(),6)
    assertTrue(computedRows sameElements(refreshed.collect()))
  }

  @Test(expected = classOf[AssertionError])
  def testLabelJoinInvalidSource(): Unit = {
     // Invalid left data model entities
    val joinConf = Builders.Join(
      left = labelGroupBy.groupByConf.sources.get(0),
      joinParts = Seq(
        Builders.JoinPart(groupBy = labelGroupBy.groupByConf)
      ),
      metaData = Builders.MetaData(name = "test_invalid_label_join", namespace = namespace, team = "chronon")
    )
    new LabelJoin(joinConf, 30,20, tableUtils, labelDS).computeLabelJoin()
  }

  @Test(expected = classOf[AssertionError])
  def testLabelJoinInvalidGroupBy(): Unit = {
    // Invalid label data model
    val joinConf = Builders.Join(
      left = viewsGroupBy.groupByConf.sources.get(0), //events
      joinParts = Seq(
        Builders.JoinPart(groupBy = viewsGroupBy.groupByConf)
      ),
      metaData = Builders.MetaData(name = "test_invalid_label_join", namespace = namespace, team = "chronon")
    )
    new LabelJoin(joinConf, 30, 20, tableUtils, labelDS).computeLabelJoin()
  }

  def createTestJoin(namespace: String): Join = {
    Builders.Join(
      left = viewsGroupBy.groupByConf.sources.get(0), //events
      joinParts = Seq(
        Builders.JoinPart(groupBy = labelGroupBy.groupByConf)
      ),
      metaData = Builders.MetaData(name = tableName, namespace = namespace, team = "chronon")
    )
  }
}
