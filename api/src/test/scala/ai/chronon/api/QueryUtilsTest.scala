package ai.chronon.api;


import org.junit.Assert.assertEquals
import org.junit.Test

class QueryUtilsTest {

  @Test
  def testBuild_isLocalizedTrue(): Unit = {
    assertEquals(QueryUtils.build(selects = Map("foo" -> "foo"), from = "foobar", isLocalized = true, wheres = Seq("bar")),
      """SELECT
  foo as `foo`
FROM foobar
WHERE
  (bar) AND ((locality_zone = 'DEFAULT' or locality_zone is null))"""
    )
  }

  @Test
  def testBuild_isLocalizedFalse(): Unit = {
    assertEquals(QueryUtils.build(selects = Map("foo" -> "foo"), from = "foobar", isLocalized = false, wheres = Seq("bar")),
      """SELECT
  foo as `foo`
FROM foobar
WHERE
  (bar)"""
    )
  }
}
