package ai.chronon.api.test

import ai.chronon.api.Builders
import org.junit.Test
import ai.chronon.api.Extensions._
import org.junit.Assert.assertEquals

class ExtensionsTest {

  @Test
  def testSubPartitionFilters(): Unit = {
    val source = Builders.Source.events(query = null, table = "db.table/system=mobile/currency=USD")
    assertEquals(
      Map("system" -> "mobile", "currency" -> "USD"),
      source.subPartitionFilters
    )
  }
}
