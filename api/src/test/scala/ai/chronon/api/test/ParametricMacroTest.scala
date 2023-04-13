package ai.chronon.api.test

import ai.chronon.api.ParametricMacro
import org.junit.Assert.assertEquals
import org.junit.Test

class ParametricMacroTest {
  @Test
  def testSubstitution(): Unit = {
    val mc = ParametricMacro("something", { x => "st:" + x.keys.mkString("/") + "|" + x.values.mkString("/") })
    val str = "something nothing-{{ something:a_1=b,3.1:c=d }}-something after-{{ thing:a1=b1 }}{{ something }}"
    val replaced = mc.replace(str)
    val expected = "something nothing-st:a_1/c|b,3.1/d-something after-{{ thing:a1=b1 }}st:|"
    assertEquals(expected, replaced)
    val invalidArg = "something nothing-{{ something:a_1=b,3+1:c=d }}-something after-{{ thing:a1=b1 }}{{ something }}"
    val replacedInvalid = mc.replace(invalidArg)
    val expectedInvalidArg = "something nothing-{{ something:a_1=b,3+1:c=d }}-something after-{{ thing:a1=b1 }}st:|"
    assertEquals(expectedInvalidArg, replacedInvalid)
  }
}
