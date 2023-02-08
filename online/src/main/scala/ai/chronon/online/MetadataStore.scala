package ai.chronon.online

import ai.chronon.api.Constants.{UTF8, ChrononMetadataKey}
import ai.chronon.api.Extensions.{JoinOps, MetadataOps, StringOps}
import ai.chronon.api._
import ai.chronon.online.KVStore.{GetRequest, PutRequest, TimedValue}
import com.google.gson.{Gson, GsonBuilder}
import org.apache.thrift.TBase

import java.io.File
import java.nio.file.{Files, Paths}
import scala.collection.JavaConverters.{mapAsJavaMapConverter, mapAsScalaMapConverter}
import scala.collection.immutable.SortedMap
import scala.collection.Seq
import scala.concurrent.{ExecutionContext, Future}
import scala.reflect.ClassTag
import scala.util.{Failure, Success, Try}

// [timestamp -> {metric name -> metric value}]
case class DataMetrics(series: Seq[(Long, SortedMap[String, Any])])

class MetadataStore(kvStore: KVStore, val dataset: String = ChrononMetadataKey, timeoutMillis: Long) {
  implicit val executionContext: ExecutionContext = kvStore.executionContext

  def getConf[T <: TBase[_, _]: Manifest](confPathOrName: String): Try[T] = {
    val clazz = implicitly[ClassTag[T]].runtimeClass.asInstanceOf[Class[T]]
    val confKey = pathToKey(confPathOrName)
    kvStore
      .getString(confKey, dataset, timeoutMillis)
      .map(conf => ThriftJsonCodec.fromJsonStr[T](conf, false, clazz))
      .recoverWith {
        case th: Throwable =>
          Failure(
            new RuntimeException(
              s"Couldn't fetch ${clazz.getName} for key $confKey. Perhaps metadata upload wasn't successful.",
              th
            ))
      }
  }

  lazy val getJoinConf: TTLCache[String, Try[JoinOps]] = new TTLCache[String, Try[JoinOps]](
    { name =>
      val startTimeMs = System.currentTimeMillis()
      val result = getConf[Join](s"joins/$name")
        .recover {
          case e: java.util.NoSuchElementException =>
            println(
              s"Failed to fetch conf for join $name at joins/$name, please check metadata upload to make sure the join metadata for $name has been uploaded")
            throw e
        }
        .map(new JoinOps(_))
      val context =
        if (result.isSuccess) Metrics.Context(Metrics.Environment.MetaDataFetching, result.get.join)
        else Metrics.Context(Metrics.Environment.MetaDataFetching, join = name)
      // Throw exception after metrics. No join metadata is bound to be a critical failure.
      if (result.isFailure) {
        context.withSuffix("join").increment(Metrics.Name.Exception)
        throw result.failed.get
      }
      context.withSuffix("join").histogram(Metrics.Name.LatencyMillis, System.currentTimeMillis() - startTimeMs)
      result
    },
    { join => Metrics.Context(environment = "join.meta.fetch", join = join) })

  def putJoinConf(join: Join): Unit = {
    println(s"uploading join conf to dataset: $dataset by key: joins/${join.metaData.nameToFilePath}")
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
                   Some(Constants.Partition.epochMillis(fromDate))))
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

  // pull and cache groupByServingInfo from the groupBy uploads
  lazy val getGroupByServingInfo: TTLCache[String, Try[GroupByServingInfoParsed]] =
    new TTLCache[String, Try[GroupByServingInfoParsed]](
      { name =>
        val startTimeMs = System.currentTimeMillis()
        val batchDataset = s"${name.sanitize.toUpperCase()}_BATCH"
        val metaData =
          kvStore.getString(Constants.GroupByServingInfoKey, batchDataset, timeoutMillis).recover {
            case e: java.util.NoSuchElementException =>
              println(
                s"Failed to fetch metadata for $batchDataset, is it possible Group By Upload for $name has not succeeded?")
              throw e
          }
        println(s"Fetched ${Constants.GroupByServingInfoKey} from : $batchDataset\n$metaData")
        if (metaData.isFailure) {
          Failure(
            new RuntimeException(s"Couldn't fetch group by serving info for $batchDataset, " +
                                   s"please make sure a batch upload was successful",
                                 metaData.failed.get))
        } else {
          val groupByServingInfo = ThriftJsonCodec
            .fromJsonStr[GroupByServingInfo](metaData.get, check = true, classOf[GroupByServingInfo])
          Metrics
            .Context(Metrics.Environment.GroupByFetching, groupByServingInfo.groupBy)
            .withSuffix("group_by")
            .histogram(Metrics.Name.LatencyMillis, System.currentTimeMillis() - startTimeMs)
          Success(new GroupByServingInfoParsed(groupByServingInfo))
        }
      },
      { gb => Metrics.Context(environment = "group_by.serving_info.fetch", groupBy = gb) })

  // derive a key from path to file
  def pathToKey(confPath: String): String = {
    // capture <conf_type>/<team>/<conf_name> as key e.g joins/team/team.example_join.v1
    confPath.split("/").takeRight(3).mkString("/")
  }

  // upload the materialized JSONs to KV store:
  // key = <conf_type>/<team>/<conf_name> in bytes e.g joins/team/team.example_join.v1 value = materialized json string in bytes
  def putConf(configPath: String): Future[Seq[Boolean]] = {
    val configFile = new File(configPath)
    assert(configFile.exists(), s"$configFile does not exist")
    println(s"Uploading Chronon configs from $configPath")
    val fileList = listFiles(configFile)

    val puts = fileList
      .filter { file =>
        val name = parseName(file.getPath)
        if (name.isEmpty) println(s"Skipping invalid file ${file.getPath}")
        name.isDefined
      }
      .flatMap { file =>
        val path = file.getPath
        val confJsonOpt = path match {
          case value if value.contains("staging_queries/") => loadJson[StagingQuery](value)
          case value if value.contains("joins/")           => loadJson[Join](value)
          case value if value.contains("group_bys/")       => loadJson[GroupBy](value)
          case _                                           => println(s"unknown config type in file $path"); None
        }
        val key = pathToKey(path)
        confJsonOpt.map { conf =>
          println(s"""Putting metadata for 
               |key: $key 
               |conf: $conf""".stripMargin)
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

  // process chronon configs only. others will be ignored
  // todo: add metrics
  private def loadJson[T <: TBase[_, _]: Manifest: ClassTag](file: String): Option[String] = {
    try {
      val configConf = ThriftJsonCodec.fromJsonFile[T](file, check = true)
      Some(ThriftJsonCodec.toJsonStr(configConf))
    } catch {
      case e: Throwable =>
        println(s"Failed to parse compiled Chronon config file: $file, \nerror=${e.getMessage}")
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
        println(s"Failed to parse Chronon config file at $path as JSON with error: ${ex.getMessage}")
        ex.printStackTrace()
        None
    }
  }
}
