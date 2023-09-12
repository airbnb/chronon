package ai.chronon.spark

import ai.chronon.spark.LocalTableExporter.DefaultNamespace
import com.google.common.io.Files
import org.apache.commons.io.FileUtils
import org.apache.spark.sql.SaveMode

import java.io.File

object LocalTableExporter {
  val SupportedExportFormat: Set[String] = Set("csv", "parquet", "json")
  val DefaultNamespace: String = "default"
}

class LocalTableExporter(tableUtils: TableUtils, exportDir: String, formatParam: String, prefix: Option[String]) {

  private val format: String = formatParam.toLowerCase

  def exportTable(namespaceAndTable: String): Unit = {
    val tmpOutputDir = Files.createTempDir()

    try {
      val outputFile = new File(buildOutputFilePath(namespaceAndTable))

      val writer = tableUtils
        .loadEntireTable(namespaceAndTable)
        .coalesce(1)
        .write
        .format(format)
        .mode(SaveMode.Overwrite)

      if (format.equals("csv")) {
        writer.option("header", "true")
      }

      writer.save(tmpOutputDir.getAbsolutePath)

      val rawOutputs = tmpOutputDir.listFiles.filter(_.getName.endsWith(format))
      assert(rawOutputs.size == 1,
             s"Unexpected number of raw output files: ${rawOutputs.map(_.getName).mkString("[", ", ", "]")}")

      FileUtils.moveFile(rawOutputs.head.getAbsoluteFile, outputFile)
    } finally {
      // make sure the tmp directory is cleaned up
      FileUtils.deleteDirectory(tmpOutputDir)
    }
  }

  private def buildOutputFilePath(namespaceAndTable: String): String = {
    val baseFileName = s"$namespaceAndTable.${format.toLowerCase}"
    val fileName = prefix.map(p => s"$p.$baseFileName").getOrElse(baseFileName)
    val stripedDir =
      if (exportDir.last == '/')
        exportDir.substring(0, exportDir.length - 1)
      else
        exportDir

    s"$stripedDir/$fileName"
  }
}
