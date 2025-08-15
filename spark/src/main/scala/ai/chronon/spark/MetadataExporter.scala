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
import org.apache.spark.sql.functions._

import collection.mutable.ListBuffer
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.commons.lang.exception.ExceptionUtils

object MetadataExporter {
  @transient implicit lazy val logger = LoggerFactory.getLogger(getClass)

  val GROUPBY_PATH_SUFFIX = "/group_bys"
  val JOIN_PATH_SUFFIX = "/joins"
  val STAGING_QUERY_PATH_SUFFIX = "/staging_queries"

  val mapper = new ObjectMapper()
  mapper.registerModule(DefaultScalaModule)
  val sparkSession = SparkSessionBuilder.build("metadata_exporter")
  val tableUtils = TableUtils(sparkSession)
  private val today = tableUtils.partitionSpec.at(System.currentTimeMillis())
  private val yesterday = tableUtils.partitionSpec.before(today)

  private val partitionColumns = Seq("entityType", "ds")

  def getFilePaths(inputPath: String): Seq[String] = {
    val rootDir = new File(inputPath)
    if (!rootDir.exists()) {
      throw new Exception(f"Directory $inputPath does not exist!")
    }
    val fileBuffer = new ListBuffer[String]()

    def isConfigFile(file: File): Boolean = {
      val path = file.getAbsolutePath
      val fileName = file.getName
      // Only process files that are in group_bys, joins, or staging_queries directories
      // and exclude known non-config files
      (path.contains(GROUPBY_PATH_SUFFIX) || path.contains(JOIN_PATH_SUFFIX) || path.contains(STAGING_QUERY_PATH_SUFFIX)) &&
        !fileName.startsWith(".") &&
        !fileName.endsWith(".csv") &&
        !fileName.endsWith(".bazel") &&
        fileName != "BUILD" &&
        fileName != "BUILD.bazel"
    }

    def traverseDirectory(currentDir: File): Unit = {
      if (currentDir.isDirectory) {
        val files = currentDir.listFiles()
        if (files != null) {
          for (file <- files) {
            if (file.isFile && isConfigFile(file)) {
              fileBuffer += file.getAbsolutePath
            } else if (file.isDirectory) {
              traverseDirectory(file)
            }
          }
        }
      } else if (isConfigFile(rootDir)) {
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

  def writeOutputToFile(data: String, path: String, outputDirectory: String): Unit = {
    Files.createDirectories(Paths.get(outputDirectory))
    val file = new File(outputDirectory + "/" + path.split("/").last)
    file.createNewFile()
    val writer = new BufferedWriter(new FileWriter(file))
    writer.write(data)
    writer.close()
    logger.info(s"${path} : Wrote to output directory successfully")
  }

  def writeOutputToTable(dataWithPath: Seq[(String, String)],
                         tableName: String,
                         ds: String,
                         outputTablePropertiesJson: Option[String]): Unit = {
    import sparkSession.implicits._
    // Step 1: Convert to DataFrame
    val dfRaw = dataWithPath.toDF("path", "metadata")
    val tableProperties = outputTablePropertiesJson
      .map(json => mapper.readValue(json, classOf[Map[String, String]]))
      .getOrElse(Map.empty[String, String])

    // Step 2: Add ds and entityType (extracted from path)
    val dfWithPartitions = dfRaw
      .withColumn("ds", lit(ds))
      .withColumn(
        "entityType",
        when($"path".contains(GROUPBY_PATH_SUFFIX), "groupBy")
          .when($"path".contains(JOIN_PATH_SUFFIX), "join")
          .when($"path".contains(STAGING_QUERY_PATH_SUFFIX), "stagingQueries")
          .otherwise("unknown")
      )

    // Step 3: insert data into hive table
    tableUtils.insertPartitions(
      dfWithPartitions,
      tableName,
      tableProperties,
      partitionColumns
    )

    logger.info(s"${tableName} : Wrote to output table successfully")
  }

  def processEntities(inputPath: String, outputPath: String): Unit = {
    val processSuccess = getFilePaths(inputPath).map { path =>
      try {
        val data = enrichMetadata(path)
        if (path.contains(GROUPBY_PATH_SUFFIX)) {
          writeOutputToFile(data, path, outputPath + GROUPBY_PATH_SUFFIX)
        } else if (path.contains(JOIN_PATH_SUFFIX)) {
          writeOutputToFile(data, path, outputPath + JOIN_PATH_SUFFIX)
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

  def processEntities(inputPath: String,
                      outputTableName: String,
                      ds: String,
                      extraOutputTablePropertiesJson: Option[String]): Unit = {
    val processedData = getFilePaths(inputPath).map { path =>
      try {
        val data = enrichMetadata(path)
        (path, true, data)
      } catch {
        case exception: Throwable => (path, false, ExceptionUtils.getStackTrace(exception))
      }
    }
    val failuresAndTraces = processedData.filter(!_._2)
    logger.info(
      s"Successfully processed ${processedData.filter(_._2).length} from $inputPath \n " +
        s"Failed to process ${failuresAndTraces.length}: \n ${failuresAndTraces.mkString("\n")}")
    val processedSucceededData = processedData.filter(_._2).map(x => (x._1, x._3))
    writeOutputToTable(processedSucceededData, outputTableName, ds, extraOutputTablePropertiesJson)
  }

  def run(inputPath: String,
          outputPathOpt: Option[String] = None,
          outputTableNameOpt: Option[String] = None,
          ds: Option[String] = None,
          outputTablePropertiesJson: Option[String] = None): Unit = {
    if (outputPathOpt.isDefined) {
      processEntities(inputPath, outputPathOpt.get)
    } else if (outputTableNameOpt.isDefined) {
      if (ds.isEmpty) {
        throw new IllegalArgumentException(
          s"`outputTableName` was provided (${outputTableNameOpt.get}), but `ds` (DataStore) is missing.\n" +
            s"To write to a Hive table, both `outputTableName` and `ds` must be provided."
        )
      }
      processEntities(inputPath, outputTableNameOpt.get, ds.get, outputTablePropertiesJson)
    } else {
      throw new IllegalArgumentException(
        s"Missing output destination: either `outputRootPath` or `outputTableName` must be provided.\n" +
          s"To fix this, specify one of the following:\n" +
          s"  --outputRootPath <path>   (to write output files to the file system)\n" +
          s"  --outputTableName <name>  (to write output to a Hive table — requires `ds` to be passed as well)"
      )
    }
  }
}
