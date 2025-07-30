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

package ai.chronon.spark

import org.slf4j.LoggerFactory

import java.io.{BufferedWriter, File, FileWriter}
import ai.chronon.api
import ai.chronon.api.ThriftJsonCodec
import ai.chronon.online.Metrics
import ai.chronon.online.Metrics.Environment
import java.nio.file.{Files, Paths}

import collection.mutable.ListBuffer
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.commons.lang.exception.ExceptionUtils

object MetadataExporter {
  @transient implicit lazy val logger = LoggerFactory.getLogger(getClass)

  val GROUPBY_PATH_SUFFIX = "/group_bys"
  val JOIN_PATH_SUFFIX = "/joins"

  val mapper = new ObjectMapper()
  mapper.registerModule(DefaultScalaModule)
  val tableUtils = TableUtils(SparkSessionBuilder.build("metadata_exporter"))
  private val today = tableUtils.partitionSpec.at(System.currentTimeMillis())
  private val yesterday = tableUtils.partitionSpec.before(today)

  def getFilePaths(inputPath: String): Seq[String] = {
    val rootDir = new File(inputPath)
    if (!rootDir.exists()) {
      throw new Exception(f"Directory $inputPath does not exist!")
    }
    val fileBuffer = new ListBuffer[String]()

    def traverseDirectory(currentDir: File): Unit = {
      if (currentDir.isDirectory) {
        val files = currentDir.listFiles()
        if (files != null) {
          for (file <- files) {
            if (file.isFile) {
              fileBuffer += file.getAbsolutePath
            } else if (file.isDirectory) {
              traverseDirectory(file)
            }
          }
        }
      } else {
        fileBuffer += currentDir.getAbsolutePath
      }
    }

    traverseDirectory(rootDir)
    fileBuffer.toList
  }

  def enrichMetadata(path: String): String = {
    val configData = mapper.readValue(new File(path), classOf[Map[String, Any]])
    val analyzer = new Analyzer(tableUtils, path, yesterday, today, silenceMode = true, skipTimestampCheck = true)

    def handleException(exception: Throwable, entityType: String): Map[String, Any] = {
      val exceptionMessage = ExceptionUtils.getStackTrace(exception)
      logger.error(s"Exception while processing $entityType $path: $exceptionMessage")
      configData + ("exception" -> exception.getMessage)
    }

    val enrichedData: Map[String, Any] = path match {
      case p if p.contains(GROUPBY_PATH_SUFFIX) =>
        try {
          val groupBy = ThriftJsonCodec.fromJsonFile[api.GroupBy](path, check = false)
          try {
            val featureMetadata =
              analyzer.analyzeGroupBy(groupBy, validateTablePermission = false).outputMetadata.map(_.asMap)
            configData + { "features" -> featureMetadata }
          } catch {
            // Exception while analyzing groupBy
            case exception: Throwable =>
              val context = Metrics.Context(environment = Environment.groupByMetadataExport, groupBy = groupBy)
              context.incrementException(exception)
              handleException(exception, "group_by")
          }
        } catch {
          // Unable to parse groupBy file
          case exception: Throwable => handleException(exception, "group_by")
        }

      case p if p.contains(JOIN_PATH_SUFFIX) =>
        try {
          val join = ThriftJsonCodec.fromJsonFile[api.Join](path, check = false)
          try {
            val joinAnalysis = analyzer.analyzeJoin(join, validateTablePermission = false)
            val featureMetadata: Seq[Map[String, String]] = joinAnalysis.finalOutputMetadata.toSeq.map(_.asMap)
            configData + { "features" -> featureMetadata }
          } catch {
            // Exception while analyzing join
            case exception: Throwable =>
              val context = Metrics.Context(environment = Environment.joinMetadataExport, join = join)
              context.incrementException(exception)
              handleException(exception, "join")
          }
        } catch {
          // Unable to parse join file
          case exception: Throwable => handleException(exception, "join")
        }

      case _ =>
        val errorMessage = s"Unknown entity type for $path"
        logger.error(errorMessage)
        configData + ("exception" -> errorMessage)
    }

    mapper.writeValueAsString(enrichedData)
  }

  def writeOutput(data: String, path: String, outputDirectory: String): Unit = {
    Files.createDirectories(Paths.get(outputDirectory))
    val file = new File(outputDirectory + "/" + path.split("/").last)
    file.createNewFile()
    val writer = new BufferedWriter(new FileWriter(file))
    writer.write(data)
    writer.close()
    logger.info(s"${path} : Wrote to output directory successfully")
  }

  def processEntities(inputPath: String, outputPath: String): Unit = {
    val processSuccess = getFilePaths(inputPath).map { path =>
      try {
        val data = enrichMetadata(path)
        if (path.contains(GROUPBY_PATH_SUFFIX)) {
          writeOutput(data, path, outputPath + GROUPBY_PATH_SUFFIX)
        } else if (path.contains(JOIN_PATH_SUFFIX)) {
          writeOutput(data, path, outputPath + JOIN_PATH_SUFFIX)
        }
        (path, true, None)
      } catch {
        case exception: Throwable => (path, false, ExceptionUtils.getStackTrace(exception))
      }
    }
    val failuresAndTraces = processSuccess.filter(!_._2)
    logger.info(
      s"Successfully processed ${processSuccess.filter(_._2).length} from $inputPath \n " +
        s"Failed to process ${failuresAndTraces.length}: \n ${failuresAndTraces.mkString("\n")}")
  }

  def run(inputPath: String, outputPath: String): Unit = {
    processEntities(inputPath, outputPath)
  }
}
