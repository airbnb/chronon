package ai.chronon.spark.test

import ai.chronon.spark.{PartitionRange, SparkSessionBuilder, TableUtils}
import org.apache.spark.sql.SparkSession
import org.junit.Assert.assertEquals
import org.junit.Test

class DataRangeTest {
  val spark: SparkSession = SparkSessionBuilder.build("DataRangeTest", local = true)
  private val tableUtils = TableUtils(spark)

  @Test
  def testIntersect(): Unit = {
    val range1 = PartitionRange(null, null)(tableUtils)
    val range2 = PartitionRange("2023-01-01", "2023-01-02")(tableUtils)
    assertEquals(range2, range1.intersect(range2))
  }
}
