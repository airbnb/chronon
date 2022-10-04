package ai.chronon.spark

import java.io.{BufferedWriter, File, FileWriter}

import ai.chronon.api
import ai.chronon.spark.Driver.parseConf
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule

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
    val analyzer = new Analyzer(tableUtils, groupByPath, "2022-09-01", "2022-09-02")
    val featureMetadata = analyzer.analyzeGroupBy(parseConf[api.GroupBy](groupByPath)).map{ featureCol =>
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
    val file = new File(outputDirectory + GROUPBY_PATH_SUFFIX + groupByPath.split("/").last)
    file.createNewFile()
    val writer = new BufferedWriter(new FileWriter(file))
    writer.write(getEnrichedGroupByMetadata(groupByPath))
    writer.close()
  }

  def processGroupBys(inputPath: String, outputPath: String): Unit = {

    val processSuccess = getGroupByPaths(inputPath + GROUPBY_PATH_SUFFIX).map{ path =>
      try {
        writeGroupByOutput(path, outputPath + GROUPBY_PATH_SUFFIX)
        (path, true, "")
      } catch {
        case exception: Throwable => (path, false, exception.getMessage)
      }
    }
    val failedGroupBys = processSuccess.filter(!_._2)
    println(s"Successfully processed ${processSuccess.filter(_._2).length} GroupBys \n " +
      s"Failed to process ${failedGroupBys.length} GroupBys: \n $failedGroupBys")
  }

  def run(inputPath: String, outputPath: String): Unit = {
    processGroupBys(inputPath, outputPath)
  }
}
