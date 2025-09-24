package ai.chronon.spark.test

import ai.chronon.aggregator.test.Column
import ai.chronon.api
import ai.chronon.api.StructField
import ai.chronon.api.Builders.Derivation
import ai.chronon.api.{
  Accuracy,
  Builders,
  Constants,
  JoinPart,
  LongType,
  Operation,
  PartitionSpec,
  StringType,
  TimeUnit,
  Window
}
import ai.chronon.api.Extensions._
import ai.chronon.spark.Extensions._
import ai.chronon.spark.GroupBy.{logger, renderDataSourceQuery}
import ai.chronon.spark.SemanticHashUtils.{tableHashesChanged, tablesToRecompute}
import ai.chronon.spark._
import ai.chronon.spark.stats.SummaryJob
import com.google.gson.Gson
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StructType, LongType => SparkLongType, StringType => SparkStringType}
import org.apache.spark.sql.{AnalysisException, DataFrame, Row, SparkSession}
import org.junit.Assert._
import org.junit.Test
import org.scalatest.Assertions.intercept
import org.apache.spark.sql.types._
import org.apache.spark.sql.Row

import scala.util.Random
import scala.util.ScalaJavaConversions.ListOps
import scala.util.Try

class JoinBloomFilterTest {
  val dummySpark: SparkSession = SparkSessionBuilder.build("JoinBloomFilterTest", local = true)
  private val dummyTableUtils = TableUtils(dummySpark)
  private val today = dummyTableUtils.partitionSpec.at(System.currentTimeMillis())
  private val monthAgo = dummyTableUtils.partitionSpec.minus(today, new Window(30, TimeUnit.DAYS))
  private val yearAgo = dummyTableUtils.partitionSpec.minus(today, new Window(365, TimeUnit.DAYS))
  private val dayAndMonthBefore = dummyTableUtils.partitionSpec.before(monthAgo)

  @Test
  def testSkipBloomFilterJoinBackfill(): Unit = {
    val spark: SparkSession =
      SparkSessionBuilder.build("JoinTest" + "_" + Random.alphanumeric.take(6).mkString, local = true)
    val tableUtils = TableUtils(spark)
    val namespace = "test_namespace_jointest" + "_" + Random.alphanumeric.take(6).mkString
    tableUtils.createDatabase(namespace)
    val testSpark: SparkSession = SparkSessionBuilder.build(
      "JoinTest",
      local = true,
      additionalConfig = Some(Map("spark.chronon.backfill.bloomfilter.threshold" -> "100")))
    val testTableUtils = TableUtils(testSpark)
    val viewsSchema = List(
      Column("user", api.StringType, 10000),
      Column("item", api.StringType, 100),
      Column("time_spent_ms", api.LongType, 5000)
    )

    val viewsTable = s"$namespace.view_events_bloom_test"
    DataFrameGen.events(testSpark, viewsSchema, count = 1000, partitions = 200).drop("ts").save(viewsTable)

    val viewsSource = Builders.Source.events(
      query = Builders.Query(selects = Builders.Selects("time_spent_ms"), startPartition = yearAgo),
      table = viewsTable
    )

    val viewsGroupBy = Builders.GroupBy(
      sources = Seq(viewsSource),
      keyColumns = Seq("item"),
      aggregations = Seq(
        Builders.Aggregation(operation = Operation.AVERAGE, inputColumn = "time_spent_ms")
      ),
      metaData = Builders.MetaData(name = "bloom_test.item_views", namespace = namespace),
      accuracy = Accuracy.SNAPSHOT
    )

    // left side
    val itemQueries = List(Column("item", api.StringType, 100))
    val itemQueriesTable = s"$namespace.item_queries_bloom_test"
    DataFrameGen
      .events(testSpark, itemQueries, 1000, partitions = 100)
      .save(itemQueriesTable)

    val start = testTableUtils.partitionSpec.minus(today, new Window(100, TimeUnit.DAYS))
    val joinConf = Builders.Join(
      left = Builders.Source.events(Builders.Query(startPartition = start), table = itemQueriesTable),
      joinParts = Seq(Builders.JoinPart(groupBy = viewsGroupBy, prefix = "user")),
      metaData = Builders.MetaData(name = "test.item_snapshot_bloom_test", namespace = namespace, team = "chronon")
    )
    val skipBloomComputed = new Join(joinConf, today, testTableUtils).computeJoin()
    val leftSideCount = testSpark.sql(s"SELECT item, ts, ds from $itemQueriesTable where ds >= '$start'").count()
    val skippedBloomCount = skipBloomComputed.count()
    logger.debug("computed count: " + skippedBloomCount)
    assertEquals(leftSideCount, skippedBloomCount)
  }
}
