package ai.chronon.spark

import java.io.{BufferedWriter, File, FileWriter}

import ai.chronon.spark.Driver.parseConf
import com.fasterxml.jackson.databind.ObjectMapper

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
    val configData = mapper.readValue(groupByPath, classOf[Map[String, Any]])
    val tableUtils = TableUtils(SparkSessionBuilder.build("metadata_exporter"))
    val analyzer = new Analyzer(tableUtils, groupByPath, null, null)
    val featureMetadata = analyzer.analyzeGroupBy(parseConf(groupByPath)).map{ featureCol =>
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
    val writer = new BufferedWriter(new FileWriter(file))
    writer.write(getEnrichedGroupByMetadata(groupByPath))
    writer.close()
  }

  def processGroupBys(inputPath: String, outputPath: String): Unit = {
    getGroupByPaths(inputPath + GROUPBY_PATH_SUFFIX).foreach{ path =>
      writeGroupByOutput(path, outputPath + GROUPBY_PATH_SUFFIX)
    }
  }

  def run(inputPath: String, outputPath: String): Unit = {
    processGroupBys(inputPath, outputPath)
  }
}
