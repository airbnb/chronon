package ai.chronon.spark

import java.io.{BufferedWriter, File, FileWriter}
import ai.chronon.api
import ai.chronon.api.{Constants, ThriftJsonCodec}
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.commons.lang.exception.ExceptionUtils
import java.nio.file.Files
import java.nio.file.Paths

object MetadataExporter {

  val GROUPBY_PATH_SUFFIX = "/group_bys"

  def getGroupByPaths(inputPath: String): Seq[String] = {
    val rootDir = new File(inputPath)
    rootDir
      .listFiles
      .filter(!_.isFile)
      .flatMap(_.listFiles())
      .map(_.getPath)
  }

  def getEnrichedGroupByMetadata(groupByPath: String): String = {
    val mapper = new ObjectMapper()
    mapper.registerModule(DefaultScalaModule)
    val configData = mapper.readValue(new File(groupByPath), classOf[Map[String, Any]])
    val tableUtils = TableUtils(SparkSessionBuilder.build("metadata_exporter"))
    val today = Constants.Partition.at(System.currentTimeMillis())
    val analyzer = new Analyzer(tableUtils, groupByPath, today, today)
    val groupBy = ThriftJsonCodec.fromJsonFile[api.GroupBy](groupByPath, check = false)
    val featureMetadata = analyzer.analyzeGroupBy(groupBy).map{ featureCol =>
      Map(
        "name" -> featureCol.name,
        "window" -> featureCol.window,
        "columnType" -> featureCol.columnType,
        "inputColumn" -> featureCol.inputColumn,
        "operation" -> featureCol.operation
      )
    }
    val enrichedData = configData + {"features" -> featureMetadata}
    mapper.writeValueAsString(enrichedData)
  }

  def writeGroupByOutput(groupByPath: String, outputDirectory: String): Unit = {
    val data = getEnrichedGroupByMetadata(groupByPath)
    println(s"${groupByPath} : Metadata generated. Writing to output directory... ")
    Files.createDirectories(Paths.get(outputDirectory))
    val file = new File(outputDirectory + "/" + groupByPath.split("/").last)
    file.createNewFile()
    val writer = new BufferedWriter(new FileWriter(file))
    writer.write(data)
    writer.close()
    println(s" ${groupByPath} : Wrote to output directory successfully")
  }

  def processGroupBys(inputPath: String, outputPath: String): Unit = {
    val processSuccess = getGroupByPaths(inputPath + GROUPBY_PATH_SUFFIX).map{ path =>
      try {
        writeGroupByOutput(path, outputPath + GROUPBY_PATH_SUFFIX)
        (path, true, None)
      } catch {
        case exception: Throwable => (path, false, ExceptionUtils.getStackTrace(exception))
      }
    }
    val failuresAndTraces = processSuccess.filter(!_._2)
    println(s"Successfully processed ${processSuccess.filter(_._2).length} GroupBys \n " +
      s"Failed to process ${failuresAndTraces.length} GroupBys: \n ${failuresAndTraces.mkString("\n")}")
  }

  def run(inputPath: String, outputPath: String): Unit = {
    processGroupBys(inputPath, outputPath)
  }
}
