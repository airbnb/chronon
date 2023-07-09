package ai.chronon.online.test

import ai.chronon.online.JoinCodec
import org.junit.Assert.assertEquals
import org.junit.Test

class JoinCodecTest {
  @Test
  def testAdjustException(): Unit = {

    val preDerived = Map("group_by_2_exception" -> "ex", "group_by_1_exception" -> "ex", "group_by_4_exception" -> "ex")
    val derived = Map(
      "group_by_1_feature1" -> "val1",
      "group_by_2_feature1" -> "val1",
      "group_by_2_feature2" -> "val2",
      "group_by_3_feature1" -> "val1",
      "derived1" -> "val1",
      "derived2" -> "val2"
    )

    val result = JoinCodec.adjustExceptions(derived, preDerived)
    val expected = Map(
      "group_by_3_feature1" -> "val1",
      "derived1" -> "val1",
      "derived2" -> "val2",
      "group_by_2_exception" -> "ex",
      "group_by_1_exception" -> "ex",
      "group_by_4_exception" -> "ex"
    )
    assertEquals(expected, result)
  }
}
