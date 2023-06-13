package ai.chronon.spark.test

import ai.chronon.spark.Driver.OfflineSubcommand
import ai.chronon.spark.SparkSessionBuilder
import org.apache.spark.sql.SparkSession
import ai.chronon.spark.{LocalTableExporter, TableUtils}
import org.junit.Assert.{assertEquals, assertTrue}
import org.junit.Test
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.{doNothing, mock, never, times, verify}
import org.rogach.scallop.ScallopConf

class OfflineSubcommandTest {

  class TestArgs(args: Array[String], localTableExporter: LocalTableExporter)
      extends ScallopConf(args)
      with OfflineSubcommand {
    verify()

    override def subcommandName: String = "test"

    override def buildSparkSession(): SparkSession = SparkSessionBuilder.build(subcommandName, local = true)
        
    protected override def buildLocalTableExporter(tableUtils: TableUtils): LocalTableExporter = localTableExporter

  }

  @Test
  def basicIsParsedCorrectly(): Unit = {
    val confPath = "joins/team/example_join.v1"
    val args = new TestArgs(Seq("--conf-path", confPath).toArray, null)
    assertEquals(confPath, args.confPath())
    assertTrue(args.localTableMapping.isEmpty)
  }

  @Test
  def localTableMappingIsParsedCorrectly(): Unit = {
    val confPath = "joins/team/example_join.v1"
    val endData = "2023-03-03"
    val argList = Seq("--local-table-mapping", "a=b", "c=d", "--conf-path", confPath, "--end-date", endData)
    val args = new TestArgs(argList.toArray, null)
    assertTrue(args.localTableMapping.nonEmpty)
    assertEquals("b", args.localTableMapping("a"))
    assertEquals("d", args.localTableMapping("c"))
    assertEquals(confPath, args.confPath())
    assertEquals(endData, args.endDate())
  }

  @Test
  def localTableExporterIsNotUsedWhenNotInLocalMode(): Unit = {
    val argList = Seq("--conf-path", "joins/team/example_join.v1", "--end-date", "2023-03-03")
    val exporter = mock(classOf[LocalTableExporter])
    val tableUtils = mock(classOf[TableUtils])
    val args = new TestArgs(argList.toArray, exporter)
    args.exportTableToLocalIfNecessary("test.test_table", tableUtils)
    verify(exporter, never()).exportTable(any())
  }

  @Test
  def localTableExporterIsNotUsedWhenNotExportPathIsNotSpecified(): Unit = {
    val argList =
      Seq("--conf-path", "joins/team/example_join.v1", "--end-date", "2023-03-03", "--local-data-path", "somewhere")
    val exporter = mock(classOf[LocalTableExporter])
    val tableUtils = mock(classOf[TableUtils])
    val args = new TestArgs(argList.toArray, exporter)
    args.exportTableToLocalIfNecessary("test.test_table", tableUtils)
    verify(exporter, never()).exportTable(any())
  }

  @Test
  def localTableExporterIsUsedWhenNecessary(): Unit = {
    val targetOutputPath = "path/to/somewhere"
    val targetFormat = "parquet"
    val prefix = "test_prefix"
    val argList = Seq(
      "--conf-path",
      "joins/team/example_join.v1",
      "--end-date",
      "2023-03-03",
      "--local-data-path",
      "somewhere",
      "--local-table-export-path",
      targetOutputPath,
      "--local-table-export-format",
      targetFormat,
      "--local-table-export-prefix",
      prefix
    )
    val exporter = mock(classOf[LocalTableExporter])
    val tableUtils = mock(classOf[TableUtils])
    doNothing().when(exporter).exportTable(any())
    val args = new TestArgs(argList.toArray, exporter)

    assertEquals(targetOutputPath, args.localTableExportPath())
    assertEquals(targetFormat, args.localTableExportFormat())
    assertEquals(prefix, args.localTableExportPrefix())

    args.exportTableToLocalIfNecessary("test.test_table", tableUtils)
    verify(exporter, times(1)).exportTable(any())
  }
}
