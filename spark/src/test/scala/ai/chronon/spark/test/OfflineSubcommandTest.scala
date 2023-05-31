package ai.chronon.spark.test

import ai.chronon.spark.Driver.OfflineSubcommand
import org.junit.Assert.{assertEquals, assertTrue}
import org.junit.Test
import org.rogach.scallop.ScallopConf


class OfflineSubcommandTest {

  class TestArgs(args: Array[String]) extends ScallopConf(args) with OfflineSubcommand {
    verify()
  }

  @Test
  def basicIsParsedCorrectly(): Unit = {
    val confPath = "joins/team/example_join.v1"
    val args = new TestArgs(Seq("--conf-path", confPath).toArray)
    assertEquals(confPath, args.confPath())
    assertTrue(args.localTableMapping.isEmpty)
  }

  @Test
  def localTableMappingIsParsedCorrectly(): Unit = {
    val confPath = "joins/team/example_join.v1"
    val endData = "2023-03-03"
    val argList = Seq(
      "--local-table-mapping", "a=b", "c=d",
      "--conf-path", "joins/team/example_join.v1",
      "--end-date", endData)
    val args = new TestArgs(argList.toArray)
    assertTrue(args.localTableMapping.nonEmpty)
    assertEquals("b", args.localTableMapping("a"))
    assertEquals("d", args.localTableMapping("c"))
    assertEquals(confPath, args.confPath())
    assertEquals(endData, args.endDate())
  }
}
