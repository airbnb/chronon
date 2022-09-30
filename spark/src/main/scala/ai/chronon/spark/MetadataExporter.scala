package ai.chronon.spark

import java.io.{BufferedWriter, File, FileWriter}
import com.fasterxml.jackson.databind.json.JsonMapper
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

  def writeGroupByOutput(groupByPath: String, outputDirectory: String): Unit = {
    val mapper = JsonMapper.builder()
      .addModule(DefaultScalaModule)
      .build()
    val configData = mapper.readValue(groupByPath, classOf[Map[String, Any]])
    val enrichedData = configData + {"features" -> "TODO"} // Integrate with new analyzer
    val outputFileName = outputDirectory + GROUPBY_PATH_SUFFIX + "/" + groupByPath.split("/").last
    val file = new File(outputFileName)
    val writer = new BufferedWriter(new FileWriter(file))
    writer.write(mapper.writeValueAsString(enrichedData))
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
