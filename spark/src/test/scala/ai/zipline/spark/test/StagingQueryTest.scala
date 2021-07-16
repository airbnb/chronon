package ai.zipline.spark.test

import ai.zipline.aggregator.base.{IntType, StringType}
import ai.zipline.aggregator.test.Column
import ai.zipline.api.Extensions._
import ai.zipline.spark.Extensions._
import ai.zipline.api.{Builders, Constants, TimeUnit, Window}
import ai.zipline.spark.{Comparison, SparkSessionBuilder, StagingQuery, TableUtils}
import org.apache.spark.sql.SparkSession
import org.junit.Assert.assertEquals
import org.junit.Test

class StagingQueryTestBase {
  lazy val spark: SparkSession = SparkSessionBuilder.build("StagingQueryTest", local = true)
  protected val today = Constants.Partition.at(System.currentTimeMillis())
  protected val ninetyDaysAgo = Constants.Partition.minus(today, new Window(90, TimeUnit.DAYS))
  protected val namespace = "staging_query_zipline_test"
  spark.sql(s"CREATE DATABASE IF NOT EXISTS $namespace")
  protected val tableUtils = TableUtils(spark)
}

class StagingQueryTest extends StagingQueryTestBase {

  @Test
  def testStagingQuery(): Unit = {
    val schema = List(
      Column("user", StringType, 10),
      Column("session_length", IntType, 1000)
    )

    val df = DataFrameGen.events(spark, schema, count = 100000, partitions = 100)
    println("Generated staging query data:")
    df.show()
    val viewName = s"$namespace.test_staging_query_compare"
    df.save(viewName)

    val stagingQueryConf = Builders.StagingQuery(
      query = s"select * from $viewName WHERE ds BETWEEN '{{ start_date }}' AND '{{ end_date }}'",
      startPartition = ninetyDaysAgo,
      setups = Seq("create temporary function temp_replace_a as 'org.apache.hadoop.hive.ql.udf.UDFRegExpReplace'"),
      metaData = Builders.MetaData(name = "test.user_session_features", namespace = namespace)
    )

    val stagingQuery = new StagingQuery(stagingQueryConf, today, tableUtils)
    stagingQuery.computeStagingQuery(stepDays = Option(30))
    val expected = tableUtils.sql(s"select * from $viewName where ds between '$ninetyDaysAgo' and '$today'")

    val computed = tableUtils.sql(
      s"select * from ${stagingQueryConf.metaData.outputNamespace}.${stagingQueryConf.metaData.cleanName}")
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
    assertEquals(diff.count(), 0)
  }
}

class StagingQueryChangedStartDate extends StagingQueryTestBase {

  /* Inspired by a null partition.
   * Generate some data.
   * Run the staging query to produce production data.
   * Create a hole in the data.
   * Change the start date in the config.
   * Run the staging query for a future date with a start partition.
   */
  @Test
  def testStagingQueryWithHolesChangedStartDate(): Unit = {
    val schema = List(
      Column("user", StringType, 10),
      Column("session_length", IntType, 1000)
    )

    val df = DataFrameGen.events(spark, schema, count = 100000, partitions = 100)
    println("Generated staging query data:")
    df.show()
    val viewName = s"$namespace.test_staging_query_with_holes"
    df.save(viewName)

    val stagingQueryConf = Builders.StagingQuery(
      query = s"select * from $viewName WHERE ds BETWEEN '{{ start_date }}' AND '{{ end_date }}'",
      startPartition = ninetyDaysAgo,
      metaData = Builders.MetaData(name = "test.user_session_features", namespace = namespace)
    )
    val sixtyDaysAgo = Constants.Partition.minus(today, new Window(60, TimeUnit.DAYS))
    val stagingQuery = new StagingQuery(stagingQueryConf, sixtyDaysAgo, tableUtils)
    stagingQuery.computeStagingQuery()

    val newConf = Builders.StagingQuery(
      query = s"select * from $viewName WHERE ds BETWEEN '{{ start_date }}' AND '{{ end_date }}'",
      startPartition = today,
      metaData = Builders.MetaData(name = "test.user_session_features", namespace = namespace)
    )
    val newStagingQuery = new StagingQuery(newConf, today, tableUtils)
    newStagingQuery.computeStagingQuery()

    val expected = tableUtils.sql(s"select * from $viewName where ds between '$today' and '$today'")
    val computed = tableUtils.sql(
      s"select * from ${stagingQueryConf.metaData.outputNamespace}.${stagingQueryConf.metaData.cleanName} where ds between '$today' and '$today'")
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
    assertEquals(diff.count(), 0)
  }
}
