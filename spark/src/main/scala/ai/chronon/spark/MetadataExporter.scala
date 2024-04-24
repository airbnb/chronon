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
import ai.chronon.api.{DataType, ThriftJsonCodec}
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.commons.lang.exception.ExceptionUtils

import java.nio.file.Files
import java.nio.file.Paths
import scala.collection.immutable.Map

object MetadataExporter {
  @transient lazy val logger = LoggerFactory.getLogger(getClass)

  val GROUPBY_PATH_SUFFIX = "/group_bys"
  val JOIN_PATH_SUFFIX = "/joins"

  val mapper = new ObjectMapper()
  mapper.registerModule(DefaultScalaModule)
  val tableUtils = TableUtils(SparkSessionBuilder.build("metadata_exporter"))
  private val today = tableUtils.partitionSpec.at(System.currentTimeMillis())
  private val yesterday = tableUtils.partitionSpec.before(today)

  def getFilePaths(inputPath: String): Seq[String] = {
    val rootDir = new File(inputPath)
    rootDir.listFiles
      .filter(!_.isFile)
      .flatMap(_.listFiles())
      .map(_.getPath)
  }

  def enrichMetadata(path: String): String = {
    val configData = mapper.readValue(new File(path), classOf[Map[String, Any]])
    val analyzer = new Analyzer(tableUtils, path, yesterday, today, silenceMode = true)
    val enrichedData: Map[String, Any] =
      try {
        if (path.contains(GROUPBY_PATH_SUFFIX)) {
          val groupBy = ThriftJsonCodec.fromJsonFile[api.GroupBy](path, check = false)
          configData + { "features" -> analyzer.analyzeGroupBy(groupBy)._1.map(_.asMap) }
        } else {
          val join = ThriftJsonCodec.fromJsonFile[api.Join](path, check = false)
          val joinAnalysis = analyzer.analyzeJoin(join, validateTablePermission = false)
          val featureMetadata: Seq[Map[String, String]] = joinAnalysis._2.toSeq.map(_.asMap)
          configData + { "features" -> featureMetadata }
        }
      } catch {
        case exception: Throwable =>
          logger.error(s"Exception while processing entity $path: ${ExceptionUtils.getStackTrace(exception)}")
          configData
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

  def processEntities(inputPath: String, outputPath: String, suffix: String): Unit = {
    val processSuccess = getFilePaths(inputPath + suffix).map { path =>
      try {
        val data = enrichMetadata(path)
        writeOutput(data, path, outputPath + suffix)
        (path, true, None)
      } catch {
        case exception: Throwable => (path, false, ExceptionUtils.getStackTrace(exception))
      }
    }
    val failuresAndTraces = processSuccess.filter(!_._2)
    logger.info(
      s"Successfully processed ${processSuccess.filter(_._2).length} from $suffix \n " +
        s"Failed to process ${failuresAndTraces.length}: \n ${failuresAndTraces.mkString("\n")}")
  }

  def run(inputPath: String, outputPath: String): Unit = {
    processEntities(inputPath, outputPath, GROUPBY_PATH_SUFFIX)
    processEntities(inputPath, outputPath, JOIN_PATH_SUFFIX)
  }
}
