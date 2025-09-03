package ai.chronon.spark.test

import ai.chronon.spark.Extensions._
import ai.chronon.spark.{Comparison, PartitionRange, SparkSessionBuilder, TableUtils}
import org.apache.spark.sql.SparkSession
import org.junit.Assert.assertEquals
import org.junit.Test

class ExtensionsTest {

  lazy val spark: SparkSession = SparkSessionBuilder.build("ExtensionsTest", local = true)

  import spark.implicits._

  private implicit val tableUtils = TableUtils(spark)

  @Test
  def testPrunePartitionTest(): Unit = {
    val df = Seq(
      (1, "2024-01-03"),
      (2, "2024-01-04"),
      (3, "2024-01-04"),
      (4, "2024-01-05"),
      (5, "2024-01-05"),
      (6, "2024-01-06"),
      (7, "2024-01-07"),
      (8, "2024-01-08"),
      (9, "2024-01-08"),
      (10, "2024-01-09")
    ).toDF("key", "ds")

    val prunedDf = df.prunePartition(PartitionRange("2024-01-05", "2024-01-07"))

    val expectedDf = Seq(
      (4, "2024-01-05"),
      (5, "2024-01-05"),
      (6, "2024-01-06"),
      (7, "2024-01-07")
    ).toDF("key", "ds")
    val diff = Comparison.sideBySide(expectedDf, prunedDf, List("key"))
    if (diff.count() != 0) {
      diff.show()
    }
    assertEquals(0, diff.count())
  }

  @Test
  def testDfWithStatsLongPartition(): Unit = {
    val df = Seq(
      (1, 20240103L),
      (2, 20240104L),
      (3, 20240104L)
    ).toDF("key", "ds")

    val dfWithStats: DfWithStats = DfWithStats(df)
    val stats = dfWithStats.stats

    assertEquals(3L, stats.count)
  }

}
