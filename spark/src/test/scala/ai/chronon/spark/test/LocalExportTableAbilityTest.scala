package ai.chronon.spark.test

import ai.chronon.spark.Driver.{LocalExportTableAbility, OfflineSubcommand}
import ai.chronon.spark.{LocalTableExporter, SparkSessionBuilder, TableUtils}
import org.apache.spark.sql.SparkSession
import org.junit.Assert.{assertEquals, assertFalse, assertTrue}
import org.junit.Test
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.{doNothing, mock, times, verify}
import org.rogach.scallop.ScallopConf

class LocalExportTableAbilityTest {
  class TestArgs(args: Array[String], localTableExporter: LocalTableExporter)
      extends ScallopConf(args)
      with OfflineSubcommand
      with LocalExportTableAbility {
    verify()

    override def subcommandName: String = "test"

    override def buildSparkSession(): SparkSession = SparkSessionBuilder.build(subcommandName, local = true)

    protected override def buildLocalTableExporter(tableUtils: TableUtils): LocalTableExporter = localTableExporter
  }

  @Test
  def localTableExporterIsNotUsedWhenNotInLocalMode(): Unit = {
    val argList = Seq("--conf-path", "joins/team/example_join.v1", "--end-date", "2023-03-03")
    val args = new TestArgs(argList.toArray, mock(classOf[LocalTableExporter]))
    assertFalse(args.shouldExport())
  }

  @Test
  def localTableExporterIsNotUsedWhenNotExportPathIsNotSpecified(): Unit = {
    val argList =
      Seq("--conf-path", "joins/team/example_join.v1", "--end-date", "2023-03-03", "--local-data-path", "somewhere")
    val args = new TestArgs(argList.toArray, mock(classOf[LocalTableExporter]))
    assertFalse(args.shouldExport())
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

    assertTrue(args.shouldExport())

    args.exportTableToLocal("test.test_table", tableUtils)
    verify(exporter, times(1)).exportTable(any())
  }
}
