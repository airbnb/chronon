/*
 *    Copyright (C) 2023 The Chronon Authors.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package ai.chronon.spark.test

import org.slf4j.LoggerFactory
import ai.chronon.aggregator.test.Column
import ai.chronon.api
import ai.chronon.spark.Extensions._
import ai.chronon.spark.{MetadataExporter, SparkSessionBuilder, TableUtils}
import com.google.common.io.Files
import junit.framework.TestCase
import org.apache.spark.sql.SparkSession
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.junit.Assert.assertEquals

import scala.io.Source
import java.io.File
import java.net.URL

class MetadataExporterTest extends TestCase {
  @transient lazy val logger = LoggerFactory.getLogger(getClass)

  val sessionName = "MetadataExporter"
  val spark: SparkSession = SparkSessionBuilder.build(sessionName, local = true)
  val tableUtils: TableUtils = TableUtils(spark)

  def printFilesInDirectory(directoryPath: String): Unit = {
    val directory = new File(directoryPath)

    if (directory.exists && directory.isDirectory) {
      logger.info("Valid Directory")
      val files = directory.listFiles

      for (file <- files) {
        logger.info(file.getPath)
        if (file.isFile) {
          logger.info(s"File: ${file.getName}")
          val source = Source.fromFile(file)
          val fileContents = source.getLines.mkString("\n")
          source.close()
          logger.info(fileContents)
          logger.info("----------------------------------------")
        }
      }
    } else {
      logger.info("Invalid directory path!")
    }
  }

  def testMetadataExport(): Unit = {
    // Create the tables.
    val namespace = "example_namespace"
    val tablename = "table"
    tableUtils.createDatabase(namespace)
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
    // Read the files.
    val file = Source.fromFile(s"${tmpDir.getAbsolutePath}/joins/example_join.v1")
    val jsonString = file.getLines().mkString("\n")
    val objectMapper = new ObjectMapper()
    objectMapper.registerModule(DefaultScalaModule)
    val jsonNode = objectMapper.readTree(jsonString)
    assertEquals(jsonNode.get("metaData").get("name").asText(), "team.example_join.v1")
  }
}
