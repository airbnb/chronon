package ai.chronon.spark

import java.io.{BufferedWriter, FileWriter}

import scala.io.Source
import scala.jdk.CollectionConverters.mapAsScalaMapConverter

import ai.chronon.spark.Driver.logger
import com.google.gson.{Gson, GsonBuilder}
import com.google.gson.reflect.TypeToken


object SampleHelper {

  val MANIFEST_FILE_NAME = "manifest.json"
  val MANIFEST_SERDE_TYPE_TOKEN = new TypeToken[java.util.Map[String, Integer]](){}.getType

  def getPriorRunManifestMetadata(outputDir: String): Map[String, Int] = {
    val gson = new Gson()
    try {
      val metadata = Source.fromFile(s"$outputDir/$MANIFEST_FILE_NAME").getLines.mkString
      val javaMap: java.util.Map[String, Integer]  = gson.fromJson(metadata, MANIFEST_SERDE_TYPE_TOKEN)
      // Convert to Scala
      javaMap.asScala.mapValues(_.intValue()).toMap
    } catch {
      case _: java.io.FileNotFoundException =>
        logger.info(s"No manifest found, running full sample.")
        Map.empty[String, Int]
      case err: Throwable =>
        logger.error(s"Manifest Deserialization error: ")
        err.printStackTrace()
        Map.empty[String, Int]
    }
  }

  def writeManifestMetadata(outputDir: String, metadata: Map[String, Int]): Unit = {
    val gson = new GsonBuilder().serializeNulls().setPrettyPrinting().create()

    // Convert Scala Map[String, Int] to a Java map
    val javaMap: java.util.Map[String, Integer] = {
      val tempMap = new java.util.HashMap[String, Integer]()
      metadata.foreach { case (key, value) => tempMap.put(key, value.asInstanceOf[Integer]) }
      tempMap
    }

    val jsonString = gson.toJson(javaMap, MANIFEST_SERDE_TYPE_TOKEN)
    val bw = new BufferedWriter(new FileWriter(s"$outputDir/$MANIFEST_FILE_NAME"))
    try {
      bw.write(jsonString)
    } finally {
      bw.close()
    }
  }

}
