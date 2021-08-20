package ai.zipline.fetcher

import ai.zipline.api.Extensions.{JoinOps, StringOps}
import ai.zipline.api.KVStore.PutRequest
import ai.zipline.api._
import org.apache.thrift.TBase

import java.io.File
import scala.concurrent.ExecutionContext
import scala.reflect.ClassTag

class MetadataStore(kvStore: KVStore, val dataset: String = "ZIPLINE_METADATA", timeoutMillis: Long) {
  implicit val executionContext: ExecutionContext = kvStore.executionContext
  lazy val getJoinConf: TTLCache[String, JoinOps] = new TTLCache[String, JoinOps]({ name =>
    new JoinOps(
      ThriftJsonCodec
        .fromJsonStr[Join](kvStore.getString(s"joins/$name", dataset, timeoutMillis), check = true, classOf[Join]))
  })

  def putJoinConf(join: Join): Unit = {
    kvStore.put(
      PutRequest(s"joins/${join.metaData.name}".getBytes(Constants.UTF8),
                 ThriftJsonCodec.toJsonStr(join).getBytes(Constants.UTF8),
                 dataset))
  }

  lazy val getGroupByServingInfo: TTLCache[String, GroupByServingInfoParsed] =
    new TTLCache[String, GroupByServingInfoParsed]({ name =>
      val batchDataset = s"${name.sanitize.toUpperCase()}_BATCH"
      val metaData =
        kvStore.getString(Constants.GroupByServingInfoKey, batchDataset, timeoutMillis)
      println(s"Fetched ${Constants.GroupByServingInfoKey} from : $batchDataset\n$metaData")
      val groupByServingInfo = ThriftJsonCodec
        .fromJsonStr[GroupByServingInfo](metaData, check = true, classOf[GroupByServingInfo])
      new GroupByServingInfoParsed(groupByServingInfo)
    })

  /**
    * This method uploads the materialized JSONs to KV store
    * @param configDirectoryPath the root directory file path includes all the materialized JSON configs, for example: zipline/production
    */
  def putZiplineConf(configDirectoryPath: String = "/Users/pengyu_hou/workspace/ml_models/zipline/production/") = {
    val configDirectory = new File(configDirectoryPath)
    if (configDirectory.exists() && !configDirectory.isDirectory) {
      throw new IllegalArgumentException(
        s"the configDirectoryPath $configDirectoryPath is not a directory, please double check your config")
    }
    val fileList = listFiles(configDirectory)
    val metadataList: List[Option[String]] = fileList.map { file =>
      val path = file.getPath
      path match {
        case value if value.contains("staging_queries") => loadJson[StagingQuery](value)
        case value if value.contains("joins")           => loadJson[Join](value)
        case value if value.contains("group_bys")       => loadJson[GroupBy](value)
        case _ =>
          println(s"unknown config type, file=$file")
          None
      }
    }.toList
    metadataList.filter(_.isDefined).map(_.get)
  }

  // list file recursively
  private def listFiles(base: File, recursive: Boolean = true): Seq[File] = {
    val files = base.listFiles
    val result = files.filter(_.isFile)
    result ++
      files
        .filter(_.isDirectory)
        .filter(_ => recursive)
        .flatMap(listFiles(_, recursive))
  }

  // process zipline v2.1 configs only. v2.0 will be ignored
  private def loadJson[T <: TBase[_, _]: Manifest: ClassTag](file: String): Option[String] = {
    try {
      val configConf = ThriftJsonCodec.fromJsonFile[T](file, check = true)
      Some(ThriftJsonCodec.toJsonStr(configConf))
    } catch {
      case _: Throwable =>
        println(s"skip Zipline v2.0 configs, file path = $file")
        None
    }
  }
}
