package ai.chronon.spark.test

import ai.chronon.aggregator.test.Column
import ai.chronon.api.Extensions._
import ai.chronon.api._
import ai.chronon.spark.Extensions._
import ai.chronon.spark.{Comparison, SparkSessionBuilder, StagingQuery, TableUtils}
import org.apache.spark.sql.SparkSession
import org.junit.Assert.assertEquals
import org.junit.Test

class StagingQueryTest {
  lazy val spark: SparkSession = SparkSessionBuilder.build("StagingQueryTest", local = true)
  implicit private val tableUtils: TableUtils = TableUtils(spark)

  private val today = tableUtils.partitionSpec.at(System.currentTimeMillis())
  private val ninetyDaysAgo = tableUtils.partitionSpec.minus(today, new Window(90, TimeUnit.DAYS))
  private val namespace = "staging_query_chronon_test"
  spark.sql(s"CREATE DATABASE IF NOT EXISTS $namespace")

  @Test
  def testStagingQuery(): Unit = {
    val schema = List(
      Column("user", StringType, 10),
      Column("session_length", IntType, 1000)
    )

    val df = DataFrameGen
      .events(spark, schema, count = 100000, partitions = 100)
      .dropDuplicates("ts") // duplicates can create issues in comparisons
    println("Generated staging query data:")
    df.show()
    val viewName = s"$namespace.test_staging_query_compare"
    df.save(viewName)

    val stagingQueryConf = Builders.StagingQuery(
      query = s"select * from $viewName WHERE ds BETWEEN '{{ start_date }}' AND '{{ end_date }}'",
      startPartition = ninetyDaysAgo,
      setups = Seq("create temporary function temp_replace_a as 'org.apache.hadoop.hive.ql.udf.UDFRegExpReplace'"),
      metaData = Builders.MetaData(name = "test.user_session_features",
                                   namespace = namespace,
                                   tableProperties = Map("key" -> "val"),
                                   customJson = "{\"additional_partition_cols\": [\"user\"]}")
    )

    val stagingQuery = new StagingQuery(stagingQueryConf, today, tableUtils)
    stagingQuery.computeStagingQuery(stepDays = Option(30))
    val expected =
      tableUtils.sql(s"select * from $viewName where ds between '$ninetyDaysAgo' and '$today' AND user IS NOT NULL")

    val computed = tableUtils.sql(s"select * from ${stagingQueryConf.metaData.outputTable} WHERE user IS NOT NULL")
    val diff = Comparison.sideBySide(expected, computed, List("user", "ts", "ds"))
    if (diff.count() > 0) {
      println(s"Actual count: ${expected.count()}")
      println(expected.show())
      println(s"Computed count: ${computed.count()}")
      println(computed.show())
      println(s"Diff count: ${diff.count()}")
      println(s"diff result rows")
      diff.show()
    }
    assertEquals(0, diff.count())
  }

  /** Test Staging Query update with new feature/column added to the query.
    */
  @Test
  def testStagingQueryAutoExpand(): Unit = {
    val schema = List(
      Column("user", StringType, 10),
      Column("session_length", IntType, 50),
      Column("new_feature", StringType, 50)
    )

    val df = DataFrameGen
      .events(spark, schema, count = 30, partitions = 8)
      .dropDuplicates("ts") // duplicates can create issues in comparisons
    println("Generated staging query data:")
    df.show()
    val viewName = s"$namespace.test_staging_query_view"
    df.save(viewName)

    val fiveDaysAgo = tableUtils.partitionSpec.minus(today, new Window(5, TimeUnit.DAYS))
    val stagingQueryConf = Builders.StagingQuery(
      query =
        s"select user, session_length, ds, ts from $viewName WHERE ds BETWEEN '{{ start_date }}' AND '{{ end_date }}'",
      startPartition = ninetyDaysAgo,
      setups = Seq("create temporary function temp_replace_b as 'org.apache.hadoop.hive.ql.udf.UDFRegExpReplace'"),
      metaData = Builders.MetaData(name = "test.user_auto_expand",
                                   namespace = namespace,
                                   tableProperties = Map("key" -> "val"),
                                   customJson = "{\"additional_partition_cols\": [\"user\"]}")
    )

    val stagingQuery = new StagingQuery(stagingQueryConf, fiveDaysAgo, tableUtils)
    stagingQuery.computeStagingQuery(stepDays = Option(30))
    val expected =
      tableUtils.sql(
        s"select user, session_length, ds, ts from $viewName where ds between '$ninetyDaysAgo' and '$fiveDaysAgo' AND user IS NOT NULL")

    val computed = tableUtils.sql(s"select * from ${stagingQueryConf.metaData.outputTable} WHERE user IS NOT NULL")
    val diff = Comparison.sideBySide(expected, computed, List("user", "ts", "ds"))
    assertEquals(0, diff.count())

    // Add new feature to the query
    val stagingQueryConfUpdated = Builders.StagingQuery(
      query =
        s"select user, session_length, new_feature, ds, ts from $viewName WHERE ds BETWEEN '{{ start_date }}' AND '{{ end_date }}'",
      startPartition = fiveDaysAgo,
      metaData = Builders.MetaData(name = "test.user_auto_expand",
                                   namespace = namespace,
                                   tableProperties = Map("key" -> "val"),
                                   customJson = "{\"additional_partition_cols\": [\"user\"]}")
    )
    val stagingQueryUpdated = new StagingQuery(stagingQueryConfUpdated, today, tableUtils)
    stagingQueryUpdated.computeStagingQuery(stepDays = Option(30))
    val fourDaysAgo = tableUtils.partitionSpec.minus(today, new Window(4, TimeUnit.DAYS))
    val expectedUpdated =
      tableUtils.sql(
        s"select user, session_length, new_feature, ds, ts from $viewName where ds between '$fourDaysAgo' and '$today' AND user IS NOT NULL")
    expectedUpdated.show()
    val computedUpdated = tableUtils.sql(
      s"select * from ${stagingQueryConfUpdated.metaData.outputTable} WHERE user IS NOT NULL and ds between '$fourDaysAgo' and '$today'")
    computedUpdated.show()

    val diffV2 = Comparison.sideBySide(expectedUpdated, computedUpdated, List("user", "ts", "ds"))
    if (diffV2.count() > 0) {
      println(s"Actual count: ${expectedUpdated.count()}")
      println(expectedUpdated.show())
      println(s"Computed count: ${computedUpdated.count()}")
      println(computedUpdated.show())
      println(s"Diff count: ${diffV2.count()}")
      println(s"diff result rows")
      diffV2.show()
    }
    assertEquals(0, diffV2.count())
  }

  /** Test that latest date is not changed between step ranges.
    * Compute in several step ranges a trivial query and for the first step range (first partition) the latest_date
    * value should be that of the latest partition (today).
    */
  @Test
  def testStagingQueryLatestDate(): Unit = {
    val schema = List(
      Column("user", StringType, 10),
      Column("session_length", IntType, 1000)
    )

    val df = DataFrameGen
      .events(spark, schema, count = 1000, partitions = 100)
      .dropDuplicates("ts") // duplicates can create issues in comparisons
    val viewName = s"$namespace.test_staging_query_latest_date"
    df.save(viewName)

    val stagingQueryConf = Builders.StagingQuery(
      query = s"""
            |SELECT
            |  *
            |  , '{{ latest_date }}' AS latest_ds
            |FROM $viewName
            |WHERE ds BETWEEN '{{ start_date }}' AND '{{ end_date }}'""".stripMargin,
      startPartition = ninetyDaysAgo,
      metaData = Builders.MetaData(name = "test.staging_latest_date",
                                   namespace = namespace,
                                   tableProperties = Map("key" -> "val"))
    )
    val stagingQuery = new StagingQuery(stagingQueryConf, today, tableUtils)
    stagingQuery.computeStagingQuery(stepDays = Option(30))
    val expected =
      tableUtils.sql(s"""
                   |SELECT
                   |  *
                   |  , '$today' as latest_ds
                   |FROM $viewName
                   |WHERE ds = '$ninetyDaysAgo' AND user IS NOT NULL""".stripMargin)

    val computed = tableUtils.sql(s"""
      |SELECT * FROM ${stagingQueryConf.metaData.outputTable}
      |WHERE user IS NOT NULL AND ds = '$ninetyDaysAgo'
      |""".stripMargin)
    val diff = Comparison.sideBySide(expected, computed, List("user", "ts", "ds"))
    if (diff.count() > 0) {
      println(s"Actual count: ${expected.count()}")
      println(expected.show())
      println(s"Computed count: ${computed.count()}")
      println(computed.show())
      println(s"Diff count: ${diff.count()}")
      println(s"diff result rows")
      diff.show()
    }
    assertEquals(0, diff.count())
  }

  @Test
  def testStagingQueryMaxDate(): Unit = {
    val schema = List(
      Column("user", StringType, 10),
      Column("session_length", IntType, 1000)
    )

    val df = DataFrameGen
      .events(spark, schema, count = 1000, partitions = 100)
      .dropDuplicates("ts") // duplicates can create issues in comparisons
    val viewName = s"$namespace.test_staging_query_max_date"
    df.save(viewName)
    val maxDate = tableUtils.partitions(viewName).max

    val stagingQueryConf = Builders.StagingQuery(
      query = s"""
                 |SELECT
                 |  *
                 |  , '{{ max_date(table=$viewName) }}' AS latest_ds
                 |FROM $viewName
                 |WHERE ds BETWEEN '{{ start_date }}' AND '{{ end_date }}'""".stripMargin,
      startPartition = ninetyDaysAgo,
      metaData =
        Builders.MetaData(name = "test.staging_max_date", namespace = namespace, tableProperties = Map("key" -> "val"))
    )
    val stagingQuery = new StagingQuery(stagingQueryConf, today, tableUtils)
    stagingQuery.computeStagingQuery(stepDays = Option(30))
    val expected =
      tableUtils.sql(s"""
                        |SELECT
                        |  *
                        |  , '$maxDate' as latest_ds
                        |FROM $viewName
                        |WHERE ds = '$ninetyDaysAgo' AND user IS NOT NULL""".stripMargin)

    val computed = tableUtils.sql(s"""
                                     |SELECT * FROM ${stagingQueryConf.metaData.outputTable}
                                     |WHERE user IS NOT NULL AND ds = '$ninetyDaysAgo'
                                     |""".stripMargin)
    val diff = Comparison.sideBySide(expected, computed, List("user", "ts", "ds"))
    if (diff.count() > 0) {
      println(s"Actual count: ${expected.count()}")
      println(expected.show())
      println(s"Computed count: ${computed.count()}")
      println(computed.show())
      println(s"Diff count: ${diff.count()}")
      println(s"diff result rows")
      diff.show()
    }
    assertEquals(0, diff.count())
  }
}
