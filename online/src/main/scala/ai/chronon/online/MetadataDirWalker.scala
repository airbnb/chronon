package ai.chronon.online

import ai.chronon.api.ThriftJsonCodec
import ai.chronon.api
import org.slf4j.LoggerFactory
import com.google.gson.Gson
import org.apache.thrift.TBase

import java.io.File
import java.nio.file.{Files, Paths}
import scala.reflect.ClassTag

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

  private def loadJsonToConf[T <: TBase[_, _]: Manifest: ClassTag](file: String): Option[T] = {
    try {
      val configConf = ThriftJsonCodec.fromJsonFile[T](file, check = true)
      Some(configConf)
    } catch {
      case e: Throwable =>
        logger.error(s"Failed to parse compiled Chronon config file: $file, \nerror=${e.getMessage}")
        None
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

  /**
    * Iterate over the list of files and extract the key value pairs for each file
    * @return Map of endpoint -> (Map of key -> List of values)
    *         e.g. (
    *            CHRONON_METADATA_BY_TEAM -> (team -> List("join1", "join2")),
    *            CHRONON_METADATA -> (teams/joins/join1 -> config1)
    *         )
    */
  def run: Map[String, Map[String, List[String]]] = {
    nonEmptyFileList.foldLeft(Map.empty[String, Map[String, List[String]]]) { (acc, file) =>
      // For each end point we apply the extractFn to the file path to extract the key value pair
      val filePath = file.getPath
      val optConf =
        try {
          filePath match {
            case value if value.contains("joins/")           => loadJsonToConf[api.Join](filePath)
            case value if value.contains("group_bys/")       => loadJsonToConf[api.GroupBy](filePath)
            case value if value.contains("staging_queries/") => loadJsonToConf[api.StagingQuery](filePath)
          }
        } catch {
          case e: Throwable =>
            logger.error(s"Failed to parse compiled team from file path: $filePath, \nerror=${e.getMessage}")
            None
        }
      if (optConf.isDefined) {
        val kvPairToEndPoint: List[(String, (String, String))] = metadataEndPointNames.map { endPointName =>
          val conf = optConf.get
          val kVPair = filePath match {
            case value if value.contains("joins/") =>
              MetadataEndPoint.getEndPoint[api.Join](endPointName).extractFn(filePath, conf.asInstanceOf[api.Join])
            case value if value.contains("group_bys/") =>
              MetadataEndPoint
                .getEndPoint[api.GroupBy](endPointName)
                .extractFn(filePath, conf.asInstanceOf[api.GroupBy])
            case value if value.contains("staging_queries/") =>
              MetadataEndPoint
                .getEndPoint[api.StagingQuery](endPointName)
                .extractFn(filePath, conf.asInstanceOf[api.StagingQuery])
          }
          (endPointName, kVPair)
        }

        kvPairToEndPoint
          .map(kvPair => {
            val endPoint = kvPair._1
            val (key, value) = kvPair._2
            val map = acc.getOrElse(endPoint, Map.empty[String, List[String]])
            val list = map.getOrElse(key, List.empty[String]) ++ List(value)
            (endPoint, map.updated(key, list))
          })
          .toMap
      } else {
        logger.info(s"Skipping invalid file ${file.getPath}")
        acc
      }
    }
  }
}
