package ai.chronon.spark.test

import ai.chronon.aggregator.test.Column
import ai.chronon.api
import ai.chronon.spark.Extensions._
import ai.chronon.spark.{MetadataExporter, SparkSessionBuilder, TableUtils}
import com.google.common.io.Files
import junit.framework.TestCase
import org.apache.spark.sql.SparkSession

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import scala.io.Source
import java.io.File
import scala.io.Source

class MetadataExporterTest extends TestCase {

  val sessionName = "MetadataExporter"
  val spark: SparkSession = SparkSessionBuilder.build(sessionName, local = true)
  val tableUtils: TableUtils = TableUtils(spark)

  def printFilesInDirectory(directoryPath: String): Unit = {
    val directory = new File(directoryPath)

    if (directory.exists && directory.isDirectory) {
      println("Valid Directory")
      val files = directory.listFiles

      for (file <- files) {
        println(file.getPath)
        if (file.isFile) {
          println(s"File: ${file.getName}")
          val source = Source.fromFile(file)
          val fileContents = source.getLines.mkString("\n")
          source.close()
          println(fileContents)
          println("----------------------------------------")
        }
      }
    } else {
      println("Invalid directory path!")
    }
  }

  def testMetadataExport(): Unit = {
    // Create the tables.
    val namespace = "example_namespace"
    val tablename = "table"
    tableUtils.sql(s"CREATE DATABASE IF NOT EXISTS $namespace")
    val sampleData = List(
      Column("a", api.StringType, 10),
      Column("b", api.StringType, 10),
      Column("c", api.LongType, 100),
      Column("d", api.LongType, 100),
      Column("e", api.LongType, 100)
    )
    val sampleTable = s"$namespace.$tablename"
    val sampleDf = DataFrameGen
      .events(spark, sampleData, 10000, partitions = 30)
    sampleDf.save(sampleTable)
    val confResource = getClass.getResource("/")
    val tmpDir: File = Files.createTempDir()
    MetadataExporter.run(confResource.getPath, tmpDir.getAbsolutePath)
    printFilesInDirectory(s"${confResource.getPath}/joins/team")
    printFilesInDirectory(s"${tmpDir.getAbsolutePath}/joins")
    // Check that the stats field is there.
    val jsonString = Source.fromFile(s"${tmpDir.getAbsolutePath}/joins/example_join.v1").getLines().mkString("\n")
    val objectMapper = new ObjectMapper()
    objectMapper.registerModule(DefaultScalaModule)
    val jsonNode = objectMapper.readTree(jsonString)
    assert(jsonNode.has("stats"), "Failed to find 'stats' field in exported metadata")
  }
}
