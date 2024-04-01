package ai.chronon.spark.test

import ai.chronon.api.{Builders, Constants, QueryUtils, Source}
import ai.chronon.api.Builders.Query
import ai.chronon.spark.{PartitionRange, SparkSessionBuilder, TableUtils}
import org.apache.spark.sql.SparkSession
import org.junit.Assert.assertEquals
import org.junit.Test

class DataRangeTest {
  val spark: SparkSession = SparkSessionBuilder.build("DataRangeTest", local = true)
  private val tableUtils = TableUtils(spark)


  @Test
  def testGenScanQueryBasedOnTime(): Unit = {
    val partitionRange: PartitionRange = PartitionRange("2024-03-01", "2024-04-01")(tableUtils)

    val source: Source = Builders.Source.events(
      query = Builders.Query(
        selects = Builders.Selects("col_1", "col_2"),
        wheres = Seq("col_1 = 'TEST'"),
        timeColumn = "ts"
      ),
      table = "test_table",
    )


    val result: String = partitionRange.genScanQueryBasedOnTime(
      source.getEvents.query,
      "test_table",
      Seq(Constants.TimeColumn -> Option(source.getEvents.query).map(_.timeColumn).orNull).toMap
    )


    val expected: String = QueryUtils.build(
        selects = Builders.Selects("ts", "col_1", "col_2"),
        from = "test_table",
        wheres = Seq("col_1 = 'TEST'", "ts >= 1709251200000", "ts < 1712016000000"),
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
