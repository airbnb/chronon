package ai.zipline.spark.test

import ai.zipline.aggregator.base.{IntType, StringType}
import ai.zipline.api.Extensions._
import ai.zipline.api.{Builders, Constants, TimeUnit, Window}
import ai.zipline.spark.{Comparison, SparkSessionBuilder, StagingQuery, TableUtils}
import org.apache.spark.sql.SparkSession
import org.junit.Assert.assertEquals
import org.junit.{AfterClass, BeforeClass, Test}

// clean needs to be a static method
object StagingQueryTest {
  @BeforeClass
  @AfterClass
  def clean(): Unit = {
    SparkSessionBuilder.cleanData()
  }
}

// !!!DO NOT extend Junit.TestCase!!!
// Or the @BeforeClass and @AfterClass annotations fail to run
class StagingQueryTest {
  lazy val spark: SparkSession = SparkSessionBuilder.build("StagingQueryTest", local = true)
  private val today = Constants.Partition.at(System.currentTimeMillis())
  private val tenDaysAgo = Constants.Partition.minus(today, new Window(10, TimeUnit.DAYS))
  private val namespace = "staging_query_zipline_test"
  spark.sql(s"CREATE DATABASE IF NOT EXISTS $namespace")
  private val tableUtils = TableUtils(spark)
  @Test
  def testStagingQuery(): Unit = {
    val schema = List(
      DataGen.Column("user", StringType, 10), // ts = last 10 days
      DataGen.Column("session_length", IntType, 2)
    )

    val outputDates = DataGen.genPartitions(10)

    val df = DataGen.events(spark, schema, count = 100000, partitions = 100)
    val viewName = "test_staging_query"
    df.createOrReplaceTempView(viewName)

    val stagingQueryConf = Builders.StagingQuery(
      query = s"select * from $viewName WHERE ds BETWEEN '{{ start_date }}' AND '{{ end_date }}'",
      startPartition = tenDaysAgo,
      setups =
        Seq("create temporary function temp_replace_right_a as 'org.apache.hadoop.hive.ql.udf.UDFRegExpReplace'"),
      metaData = Builders.MetaData(name = "test.user_session_features", namespace = namespace)
    )

    val stagingQuery = new StagingQuery(stagingQueryConf, today, tableUtils)
    stagingQuery.computeStagingQuery()
    val actual = tableUtils.sql(s"select * from $viewName where ds between $tenDaysAgo and $today")

    val expected = tableUtils.sql(
      s"select * from ${stagingQueryConf.metaData.outputNamespace}.${stagingQueryConf.metaData.cleanName} ")
    val diff = Comparison.sideBySide(actual, expected, List("user", "ts", "ds"))
    if (diff.count() > 0) {
      println(s"Actual count: ${actual.count()}")
      println(s"Expected count: ${expected.count()}")
      println(s"Diff count: ${diff.count()}")
      println(s"Queries count: ${expected.count()}")
      println(s"diff result rows")
      diff.show()
    }
    assertEquals(diff.count(), 0)
  }
}
