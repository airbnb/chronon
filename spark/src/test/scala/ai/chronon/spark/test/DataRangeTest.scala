package ai.chronon.spark.test

import ai.chronon.spark.PartitionRange
import org.junit.Assert.assertEquals
import org.junit.Test

class DataRangeTest {

  @Test
  def testIntersect(): Unit = {
    val range1 = PartitionRange(null, null)
    val range2 = PartitionRange("2023-01-01", "2023-01-02")
    assertEquals(range2, range1.intersect(range2))
  }
}
