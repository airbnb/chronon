package ai.zipline.online

import ai.zipline.api.Extensions.{JoinOps, MetadataOps, StringOps}
import ai.zipline.api.Constants.{UTF8, ZiplineMetadataKey}
import ai.zipline.api.Extensions.{JoinOps, StringOps}
import ai.zipline.api._
import ai.zipline.online.KVStore.{GetRequest, PutRequest, TimedValue}
import com.google.gson.{Gson, GsonBuilder}
import org.apache.thrift.TBase

import java.io.File
import java.nio.file.{Files, Paths}
import scala.collection.JavaConverters.{mapAsJavaMapConverter, mapAsScalaMapConverter}
import scala.collection.immutable.SortedMap
import scala.concurrent.{ExecutionContext, Future}
import scala.reflect.ClassTag
import scala.util.{Failure, Success, Try}

// [timestamp -> {metric name -> metric value}]
case class DataMetrics(series: Seq[(Long, SortedMap[String, Any])])

class MetadataStore(kvStore: KVStore, val dataset: String = ZiplineMetadataKey, timeoutMillis: Long) {
  implicit val executionContext: ExecutionContext = kvStore.executionContext

  lazy val getJoinConf: TTLCache[String, Try[JoinOps]] = new TTLCache[String, Try[JoinOps]]({ name =>
    val startTimeMs = System.currentTimeMillis()
    val joinConf = kvStore.getString(s"joins/$name", dataset, timeoutMillis)
    if (joinConf.isFailure) {
      Failure(
        new RuntimeException(
          s"Couldn't fetch join conf for $name, please make sure that metadata-upload was successful",
          joinConf.failed.get))
    } else {
      val joinOps: JoinOps = new JoinOps(ThriftJsonCodec.fromJsonStr[Join](joinConf.get, check = true, classOf[Join]))
      MetadataMetrics.reportJoinConfRequestMetric(System.currentTimeMillis() - startTimeMs,
                                                  Metrics.Context(join = name))
      Success(joinOps)
    }
  })

  def putJoinConf(join: Join): Unit = {
    println(s"uploading join conf to dataset: $dataset by key: joins/${join.metaData.name}")
    kvStore.put(
      PutRequest(s"joins/${join.metaData.nameToFilePath}".getBytes(Constants.UTF8),
                 ThriftJsonCodec.toJsonStr(join).getBytes(Constants.UTF8),
                 dataset))
  }

  def putConsistencyMetrics(joinConf: Join, metrics: DataMetrics): Unit = {
    val gson = new GsonBuilder().setPrettyPrinting().create()
    kvStore.multiPut(
      metrics.series.map {
        case (tsMillis, map) =>
          val jMap: java.util.Map[String, Any] = map.asJava
          val json = gson.toJson(jMap)
          PutRequest(s"consistency/join/${joinConf.metaData.name}".getBytes(UTF8),
                     json.getBytes(Constants.UTF8),
                     dataset,
                     Some(tsMillis))
      }
    )
  }

  def getConsistencyMetrics(joinConf: Join, fromDate: String): Future[Try[DataMetrics]] = {
    val gson = new Gson()
    val responseFuture = kvStore
      .get(
        GetRequest(s"consistency/join/${joinConf.metaData.name}".getBytes(UTF8),
                   dataset,
                   Some(Constants.Partition.epochMillis(fromDate))));
    responseFuture.map { response =>
      val valuesTry = response.values
      valuesTry.map { values =>
        val series = values.map {
          case TimedValue(bytes, millis) =>
            val jsonString = new String(bytes, Constants.UTF8)
            val jMap = gson.fromJson(jsonString, classOf[java.util.Map[String, Object]])
            millis -> (SortedMap.empty[String, Any] ++ jMap.asScala)
        }
        DataMetrics(series)
      }
    }
  }

  lazy val getGroupByServingInfo: TTLCache[String, Try[GroupByServingInfoParsed]] =
    new TTLCache[String, Try[GroupByServingInfoParsed]]({ name =>
      val startTimeMs = System.currentTimeMillis()
      val batchDataset = s"${name.sanitize.toUpperCase()}_BATCH"
      val metaData =
        kvStore.getString(Constants.GroupByServingInfoKey, batchDataset, timeoutMillis)
      println(s"Fetched ${Constants.GroupByServingInfoKey} from : $batchDataset\n$metaData")
      if (metaData.isFailure) {
        Failure(
          new RuntimeException(s"Couldn't fetch group by serving info for $batchDataset, " +
                                 s"please make sure a batch upload was successful",
                               metaData.failed.get))
      } else {
        val groupByServingInfo = ThriftJsonCodec
          .fromJsonStr[GroupByServingInfo](metaData.get, check = true, classOf[GroupByServingInfo])
        MetadataMetrics.reportJoinConfRequestMetric(
          System.currentTimeMillis() - startTimeMs,
          Metrics.Context(groupBy = groupByServingInfo.getGroupBy.getMetaData.getName))
        Success(new GroupByServingInfoParsed(groupByServingInfo))
      }
    })

  // upload the materialized JSONs to KV store:
  // key = <conf_type>/<team>/<conf_name> in bytes e.g joins/team/team.example_join.v1 value = materialized json string in bytes
  def putConf(configPath: String): Future[Seq[Boolean]] = {
    val configFile = new File(configPath)
    assert(configFile.exists(), s"$configFile does not exist")
    println(s"Uploading Zipline configs from $configPath")
    val fileList = listFiles(configFile)

    val puts = fileList
      .filter { file =>
        val name = parseName(file.getPath)
        if (name.isEmpty) println(s"Skipping invalid file ${file.getPath}")
        name.isDefined
      }
      .flatMap { file =>
        // capture <conf_type>/<team>/<conf_name> as key e.g joins/team/team.example_join.v1
        val path = file.getPath
        val key = path.split("/").takeRight(3).mkString("/")
        val confJsonOpt = path match {
          case value if value.contains("staging_queries/") => loadJson[StagingQuery](value)
          case value if value.contains("joins/")           => loadJson[Join](value)
          case value if value.contains("group_bys/")       => loadJson[GroupBy](value)
          case _                                           => println(s"unknown config type in file $path"); None
        }
        confJsonOpt.map { conf =>
          println(s"Putting metadata for $key to KV store")
          PutRequest(keyBytes = key.getBytes(),
                     valueBytes = conf.getBytes(),
                     dataset = dataset,
                     tsMillis = Some(System.currentTimeMillis()))
        }
      }
    println(s"Putting ${puts.size} configs to KV Store, dataset=$dataset")
    kvStore.multiPut(puts)
  }

  // list file recursively
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

  // process zipline configs only. others will be ignored
  // todo: add metrics
  private def loadJson[T <: TBase[_, _]: Manifest: ClassTag](file: String): Option[String] = {
    try {
      val configConf = ThriftJsonCodec.fromJsonFile[T](file, check = true)
      Some(ThriftJsonCodec.toJsonStr(configConf))
    } catch {
      case e: Throwable =>
        println(s"Failed to parse compiled Zipline config file: $file, \nerror=${e.getMessage}")
        None
    }
  }

  def parseName(path: String): Option[String] = {
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
        println(s"Failed to parse Zipline config file at $path as JSON with error: ${ex.getMessage}")
        ex.printStackTrace()
        None
    }
  }
}
