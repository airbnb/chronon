package ai.chronon.spark.test

import ai.chronon.aggregator.test.Column
import ai.chronon.api
import ai.chronon.api.{Builders, Constants, QueryUtils, Source}
import ai.chronon.api.Builders.Query
import ai.chronon.spark.Extensions._
import ai.chronon.spark.{PartitionRange, SparkSessionBuilder, TableUtils}
import org.apache.spark.sql.SparkSession
import org.junit.Assert.assertEquals
import org.junit.Test

class DataRangeTest {
  val spark: SparkSession = SparkSessionBuilder.build("DataRangeTest", local = true)
  private val tableUtils = TableUtils(spark)


  @Test
  def testGenScanQueryBasedOnTime(): Unit = {
    val namespace = "date_range_test_namespace"
    spark.sql(s"CREATE DATABASE IF NOT EXISTS $namespace")
    val testTable = s"$namespace.test_table"

    val viewsSchema = List(
      Column("col_1", api.StringType, 1),
      Column("col_2", api.StringType, 1),
    )

    DataFrameGen
      .events(spark, viewsSchema, count = 1000, partitions = 200)
      .drop("ds")
      .save(testTable, partitionColumns = Seq())

    val partitionRange: PartitionRange = PartitionRange("2024-03-01", "2024-04-01")(tableUtils)

    val source: Source = Builders.Source.events(
      query = Builders.Query(
        selects = Builders.Selects("col_1", "col_2"),
        wheres = Seq("col_1 = 'TEST'"),
        timeColumn = "ts"
      ),
      table = testTable,
    )


    val result: String = partitionRange.genScanQueryBasedOnTime(
      source.getEvents.query,
      testTable,
      Seq(Constants.TimeColumn -> Option(source.getEvents.query).map(_.timeColumn).orNull).toMap
    )


    val expected: String = QueryUtils.build(
        selects = Builders.Selects("ts", "col_1", "col_2"),
        from = testTable,
        wheres = Seq("col_1 = 'TEST'", "ts >= 1709251200000", "ts < 1712016000000"),
        isLocalized = false
      )

    assertEquals(expected.stripMargin, result.stripMargin)
  }

  @Test
  def testIntersect(): Unit = {
    val range1 = PartitionRange(null, null)(tableUtils)
    val range2 = PartitionRange("2023-01-01", "2023-01-02")(tableUtils)
    assertEquals(range2, range1.intersect(range2))
  }
}
