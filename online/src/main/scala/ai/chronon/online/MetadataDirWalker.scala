package ai.chronon.online

import org.slf4j.LoggerFactory
import ai.chronon.online.MetadataEndPoint
import com.google.gson.Gson

import java.io.File
import java.nio.file.{Files, Paths}

class MetadataDirWalker(dirPath: String, metadataEndPointNames: List[String]) {
  @transient implicit lazy val logger = LoggerFactory.getLogger(getClass)
  private def listFiles(base: File, recursive: Boolean = true): Seq[File] = {
    if (base.isFile) {
      Seq(base)
    } else {
      val files = base.listFiles
      val result = files.filter(_.isFile)
      result ++
        files
          .filter(_.isDirectory)
          .filter(_ => recursive)
          .flatMap(listFiles(_, recursive))
    }
  }

  private def parseName(path: String): Option[String] = {
    val gson = new Gson()
    val reader = Files.newBufferedReader(Paths.get(path))
    try {
      val map = gson.fromJson(reader, classOf[java.util.Map[String, AnyRef]])
      Option(map.get("metaData"))
        .map(_.asInstanceOf[java.util.Map[String, AnyRef]])
        .map(_.get("name"))
        .flatMap(Option(_))
        .map(_.asInstanceOf[String])
    } catch {
      case ex: Throwable =>
        logger.error(s"Failed to parse Chronon config file at $path as JSON", ex)
        ex.printStackTrace()
        None
    }
  }

  lazy val fileList: Seq[File] = {
    val configFile = new File(dirPath)
    assert(configFile.exists(), s"$configFile does not exist")
    logger.info(s"Uploading Chronon configs from $dirPath")
    listFiles(configFile)
  }

  lazy val nonEmptyFileList: Seq[File] = {
    fileList
      .filter { file =>
        val name = parseName(file.getPath)
        if (name.isEmpty) logger.info(s"Skipping invalid file ${file.getPath}")
        name.isDefined
      }
  }

  def extractKVPair(metadataEndPointName: String, filePath: String): (Option[String], Option[String]) = {
    val endPoint: MetadataEndPoint = MetadataEndPoint.NameToEndPoint(metadataEndPointName)
    val (key, value) = endPoint.extractFn(filePath)
    (key, value)
  }

  def run: Map[String, Map[String, List[String]]] = {
    nonEmptyFileList.foldLeft(Map.empty[String, Map[String, List[String]]]) {
      (acc, file) =>
      val kvPairToEndPoint: List[(String, (Option[String], Option[String]))] = metadataEndPointNames.map { metadataEndPointName =>
        (metadataEndPointName, extractKVPair(metadataEndPointName, file.getPath))
      }
      kvPairToEndPoint.flatMap(
        kvPair => {
          val endPoint = kvPair._1
          val (key, value) = kvPair._2
          if (value.isDefined && key.isDefined) {
            val map = acc.getOrElse(endPoint, Map.empty[String, List[String]])
            val list = map.getOrElse(key.get, List.empty[String]) ++ List(value.get)
            acc.updated(endPoint, map.updated(key.get, list))
          }
          else acc
        }
      ).toMap
    }
  }
}
