package ai.chronon.online.test

import ai.chronon.online.CatalystUtil
import junit.framework.TestCase
import org.junit.Assert.assertEquals
import org.junit.Test

class CatalystUtilHiveUDFTest extends TestCase with CatalystUtilTestSparkSQLStructs {

  @Test
  def testHiveUDFsViaSetupsShouldWork(): Unit = {
    val setups = Seq(
      "CREATE FUNCTION MINUS_ONE AS 'ai.chronon.online.test.Minus_One'",
      "CREATE FUNCTION CAT_STR AS 'ai.chronon.online.test.Cat_Str'",
    )
    val selects = Seq(
      "a" -> "MINUS_ONE(int32_x)",
      "b" -> "CAT_STR(string_x)"
    )
    val cu = new CatalystUtil(expressions = selects, inputSchema = CommonScalarsStruct, setups = setups)
    val res = cu.sqlTransform(CommonScalarsRow)
    assertEquals(res.get.size, 2)
    assertEquals(res.get("a"), Int.MaxValue - 1)
    assertEquals(res.get("b"), "hello123")
  }
}
