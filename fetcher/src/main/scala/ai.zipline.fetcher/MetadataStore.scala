package ai.zipline.fetcher

import ai.zipline.api.Constants.ZiplineMetadataKey
import ai.zipline.api.Extensions.{JoinOps, StringOps}
import ai.zipline.api.KVStore.PutRequest
import ai.zipline.api._
import org.apache.thrift.TBase
import com.jayway.jsonpath.JsonPath

import java.io.File
import scala.concurrent.ExecutionContext
import scala.reflect.ClassTag

class MetadataStore(kvStore: KVStore, val dataset: String = ZiplineMetadataKey, timeoutMillis: Long) {
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
    * key = [joins|group_bys|staging_queries]/team_name/config_file_name in bytes
    * value = json string in bytes
    * data set = ZIPLINE_METADATA
    * @param configDirectoryPath the root directory file path includes all the materialized JSON configs, for example: zipline/production
    */
  def putZiplineConf(configDirectoryPath: String): Unit = {
    val configDirectory = new File(configDirectoryPath)
    assert(configDirectory.exists(), s"$configDirectory does not exist")
    assert(configDirectory.isDirectory, s"$configDirectory is not a directory")
    val fileList = listFiles(configDirectory)

    val puts = fileList.flatMap { file =>
      val path = file.getPath
      if (JsonPath.read(file, "$.metaData.name") == null) {
        println(s"Ignoring file $path")
        return None
      }
      // capture <conf_type>/<team>/<conf_name> as key
      val keyParts = path.split("/").takeRight(3)
      val confJsonOpt = path match {
        case value if value.startsWith("staging_queries/") => loadJson[StagingQuery](value)
        case value if value.startsWith("joins/")           => loadJson[Join](value)
        case value if value.startsWith("group_bys/")       => loadJson[GroupBy](value)
        case _                                             => println(s"unknown config type in file $path"); None
      }
      confJsonOpt
        .map(conf =>
          PutRequest(keyBytes = keyParts.mkString("/").getBytes(), valueBytes = conf.getBytes(), dataset = dataset))
    }
    kvStore.multiPut(puts)
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

  // process zipline v2.1 configs only. others will be ignored
  // todo: add metrics
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
