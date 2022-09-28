package ai.chronon.spark

import java.io.File

import scala.io.Source
import scala.reflect.io.File

import ai.chronon.api
import ai.chronon.api.GroupBy
import ai.chronon.spark.Driver.parseConf
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

    val configData = mapper.readValue(groupByPath, Map.getClass)
    ///

import com.fasterxml.jackson.databind.json.JsonMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import scala.io.Source
import java.io.File

val m = JsonMapper.builder().addModule(DefaultScalaModule).build()

val source = new File("/home/varant_zanoyan/test.txt")
val d = m.readValue(source, classOf[Map[String, Any]])
    
m.writeValueAsString(d)
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
