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
  private val today = Constants.Partition.at(System.currentTimeMillis())
  private val ninetyDaysAgo = Constants.Partition.minus(today, new Window(90, TimeUnit.DAYS))
  private val namespace = "staging_query_chronon_test"
  spark.sql(s"CREATE DATABASE IF NOT EXISTS $namespace")
  private val tableUtils = TableUtils(spark)
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
}
