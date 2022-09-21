package ai.chronon.spark.test

import ai.chronon.spark.stats.EditDistance
import org.junit.Assert.assertEquals
import org.junit.Test

class EditDistanceTest {

  @Test
  def basic(): Unit = {
    def of(a: Any, b: Any) = EditDistance.between(a, b)
    def ofString(a: String, b: String) = EditDistance.betweenStrings(a, b)

    assertEquals(0, of(null, null).total)
    assertEquals(0, of(Seq.empty[String], Seq.empty[String]).total)
    assertEquals(2, of(Seq("abc", "def"), null).total)
    assertEquals(2, of(Seq("abc", "def"), Seq.empty[String]).total)
    assertEquals(0, of(Seq("abc", "def"), Seq("abc", "def")).total)
    assertEquals(2, of(Seq(3, 1), Seq(4, 3, 1, 2)).delete)

    // 2 deletes from & 3 inserts into right - to make it like left
    assertEquals(2, of(Seq(1, 2, 3, 4), Seq(5, 6, 2)).delete)
    assertEquals(3, of(Seq(1, 2, 3, 4), Seq(5, 6, 2)).insert)
    assertEquals(6, ofString("abc", "def").total)
    assertEquals(4, ofString("abc", "dbf").total)
  }
}
