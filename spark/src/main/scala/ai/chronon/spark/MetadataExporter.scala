package ai.chronon.spark

import java.io.{BufferedWriter, File, FileWriter}
import ai.chronon.api
import ai.chronon.api.{DataType, ThriftJsonCodec}
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.commons.lang.exception.ExceptionUtils

import java.nio.file.Files
import java.nio.file.Paths

object MetadataExporter {

  val GROUPBY_PATH_SUFFIX = "/group_bys"
  val JOIN_PATH_SUFFIX = "/joins"

  val mapper = new ObjectMapper()
  mapper.registerModule(DefaultScalaModule)
  val tableUtils = TableUtils(SparkSessionBuilder.build("metadata_exporter"))

  def getFilePaths(inputPath: String): Seq[String] = {
    val rootDir = new File(inputPath)
    rootDir.listFiles
      .filter(!_.isFile)
      .flatMap(_.listFiles())
      .map(_.getPath)
  }

  def enrichMetadata(path: String): String = {
    val configData = mapper.readValue(new File(path), classOf[Map[String, Any]])
    val analyzer = new Analyzer(tableUtils, path, null, null, silenceMode = true)
    val analyzerOutput: Seq[analyzer.AggregationMetadata] = try {
      if (path.contains(GROUPBY_PATH_SUFFIX)) {
        val groupBy = ThriftJsonCodec.fromJsonFile[api.GroupBy](path, check = false)
        analyzer.analyzeGroupBy(groupBy)
      } else {
        val join = ThriftJsonCodec.fromJsonFile[api.Join](path, check = false)
        analyzer.analyzeJoin(join)._2.toSeq
      }
    } catch {
      case exception: Throwable =>
        println(s"Exception while processing entity $path: ${ExceptionUtils.getStackTrace(exception)}")
        Seq.empty
    }

    val featureMetadata: Seq[Map[String, String]] = analyzerOutput.map { featureCol =>
      Map(
        "name" -> featureCol.name,
        "window" -> featureCol.window,
        "columnType" -> DataType.toString(featureCol.columnType),
        "inputColumn" -> featureCol.inputColumn,
        "operation" -> featureCol.operation,
        "groupBy" -> featureCol.groupByName
      )
    }
    val enrichedData = configData + { "features" -> featureMetadata }
    mapper.writeValueAsString(enrichedData)
  }
  
  def writeOutput(data: String, path: String, outputDirectory: String): Unit = {
    Files.createDirectories(Paths.get(outputDirectory))
    val file = new File(outputDirectory + "/" + path.split("/").last)
    file.createNewFile()
    val writer = new BufferedWriter(new FileWriter(file))
    writer.write(data)
    writer.close()
    println(s"${path} : Wrote to output directory successfully")
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
    println(s"Successfully processed ${processSuccess.filter(_._2).length} from $suffix \n " +
      s"Failed to process ${failuresAndTraces.length}: \n ${failuresAndTraces.mkString("\n")}")
  }

  def run(inputPath: String, outputPath: String): Unit = {
    processEntities(inputPath, outputPath, GROUPBY_PATH_SUFFIX)
    processEntities(inputPath, outputPath, JOIN_PATH_SUFFIX)
  }
}
