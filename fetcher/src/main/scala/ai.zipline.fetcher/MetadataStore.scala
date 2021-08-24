package ai.zipline.fetcher

import ai.zipline.api.Constants.ZiplineMetadataKey
import ai.zipline.api.Extensions.{JoinOps, StringOps}
import ai.zipline.api.KVStore.PutRequest
import ai.zipline.api._
import org.apache.thrift.TBase
import com.jayway.jsonpath.JsonPath
import com.jayway.jsonpath.Configuration
import com.jayway.jsonpath.{Option => JsonPathOption}
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

  // upload the materialized JSONs to KV store:
  // key = <conf_type>/<team>/<conf_name> in bytes e.g joins/team/team.example_join.v1 value = materialized json string in bytes
  def putZiplineConf(configDirectoryPath: String): Unit = {
    val configDirectory = new File(configDirectoryPath)
    assert(configDirectory.exists(),
           s"$configDirectory does not exist, current path= ${System.getProperty("user.dir")}")
    assert(configDirectory.isDirectory, s"$configDirectory is not a directory")
    val fileList = listFiles(configDirectory)

    val configuration = Configuration.builder.options(JsonPathOption.DEFAULT_PATH_LEAF_TO_NULL).build
    val puts = fileList
      .filter(
        // the current Zipline config should have metaData.name field
        // if this field doesn't exist, we will simply skip for further parsing validation
        JsonPath.parse(_, configuration).read("$.metaData.name") != null
      )
      .flatMap { file =>
        val path = file.getPath
        // capture <conf_type>/<team>/<conf_name> as key e.g joins/team/team.example_join.v1
        val key = path.split("/").takeRight(3).mkString("/")
        val confJsonOpt = path match {
          case value if value.contains("staging_queries/") => loadJson[StagingQuery](value)
          case value if value.contains("joins/")           => loadJson[Join](value)
          case value if value.contains("group_bys/")       => loadJson[GroupBy](value)
          case _                                           => println(s"unknown config type in file $path"); None
        }
        confJsonOpt
          .map(conf => PutRequest(keyBytes = key.getBytes(), valueBytes = conf.getBytes(), dataset = dataset))
      }
    kvStore.multiPut(puts)
    println(s"Successfully put ${puts.size} config to KV Store")
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

  // process zipline configs only. others will be ignored
  // todo: add metrics
  private def loadJson[T <: TBase[_, _]: Manifest: ClassTag](file: String): Option[String] = {
    try {
      val configConf = ThriftJsonCodec.fromJsonFile[T](file, check = true)
      Some(ThriftJsonCodec.toJsonStr(configConf))
    } catch {
      case _: Throwable =>
        println(s"Failed to parse JSON to Zipline configs, file path = $file")
        None
    }
  }
}
