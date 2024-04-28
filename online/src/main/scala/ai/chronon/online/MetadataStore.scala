/*
 *    Copyright (C) 2023 The Chronon Authors.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package ai.chronon.online

import org.slf4j.LoggerFactory
import ai.chronon.api.Constants.{ChrononMetadataKey, UTF8}
import ai.chronon.api.Extensions.{JoinOps, MetadataOps, StringOps, WindowOps, WindowUtils}
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
  @transient implicit lazy val logger = LoggerFactory.getLogger(getClass)
  private var partitionSpec = PartitionSpec(format = "yyyy-MM-dd", spanMillis = WindowUtils.Day.millis)
  private val CONF_BATCH_SIZE = 50

  // Note this should match with the format used in the warehouse
  def setPartitionMeta(format: String, spanMillis: Long): Unit = {
    partitionSpec = PartitionSpec(format = format, spanMillis = spanMillis)
  }

  // Note this should match with the format used in the warehouse
  def setPartitionMeta(format: String): Unit = {
    partitionSpec = PartitionSpec(format = format, spanMillis = partitionSpec.spanMillis)
  }

  implicit val executionContext: ExecutionContext = kvStore.executionContext

  def getConf[T <: TBase[_, _]: Manifest](confPathOrName: String): Try[T] = {
    val clazz = implicitly[ClassTag[T]].runtimeClass.asInstanceOf[Class[T]]
    val confKey = confPathOrName.confPathToKey
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
            logger.error(
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
      context.withSuffix("join").distribution(Metrics.Name.LatencyMillis, System.currentTimeMillis() - startTimeMs)
      result
    },
    { join => Metrics.Context(environment = "join.meta.fetch", join = join) })

  def putJoinConf(join: Join): Unit = {
    logger.info(s"uploading join conf to dataset: $dataset by key: joins/${join.metaData.nameToFilePath}")
    kvStore.put(
      PutRequest(s"joins/${join.metaData.nameToFilePath}".getBytes(Constants.UTF8),
                 ThriftJsonCodec.toJsonStr(join).getBytes(Constants.UTF8),
                 dataset))
  }

  def getSchemaFromKVStore(dataset: String, key: String): AvroCodec = {
    kvStore
      .getString(key, dataset, timeoutMillis)
      .recover {
        case e: java.util.NoSuchElementException =>
          logger.error(s"Failed to retrieve $key for $dataset. Is it possible that hasn't been uploaded?")
          throw e
      }
      .map(AvroCodec.of(_))
      .get
  }

  lazy val getStatsSchemaFromKVStore: TTLCache[(String, String), AvroCodec] = new TTLCache[(String, String), AvroCodec](
    { case (dataset, key) => getSchemaFromKVStore(dataset, key) },
    { _ => Metrics.Context(environment = "stats.serving_info.fetch") }
  )

  // pull and cache groupByServingInfo from the groupBy uploads
  lazy val getGroupByServingInfo: TTLCache[String, Try[GroupByServingInfoParsed]] =
    new TTLCache[String, Try[GroupByServingInfoParsed]](
      { name =>
        val startTimeMs = System.currentTimeMillis()
        val batchDataset = s"${name.sanitize.toUpperCase()}_BATCH"
        val metaData =
          kvStore.getString(Constants.GroupByServingInfoKey, batchDataset, timeoutMillis).recover {
            case e: java.util.NoSuchElementException =>
              logger.error(
                s"Failed to fetch metadata for $batchDataset, is it possible Group By Upload for $name has not succeeded?")
              throw e
          }
        logger.info(s"Fetched ${Constants.GroupByServingInfoKey} from : $batchDataset")
        if (metaData.isFailure) {
          Failure(
            new RuntimeException(s"Couldn't fetch group by serving info for $batchDataset, " +
                                   s"please make sure a batch upload was successful",
                                 metaData.failed.get))
        } else {
          val groupByServingInfo = ThriftJsonCodec
            .fromJsonStr[GroupByServingInfo](metaData.get, check = true, classOf[GroupByServingInfo])
          Metrics
            .Context(Metrics.Environment.MetaDataFetching, groupByServingInfo.groupBy)
            .withSuffix("group_by")
            .distribution(Metrics.Name.LatencyMillis, System.currentTimeMillis() - startTimeMs)
          Success(new GroupByServingInfoParsed(groupByServingInfo, partitionSpec))
        }
      },
      { gb => Metrics.Context(environment = "group_by.serving_info.fetch", groupBy = gb) })

  // derive a key from path to file
  def pathToKey(confPath: String): String = {
    // capture <conf_type>/<team>/<conf_name> as key e.g joins/team/team.example_join.v1
    confPath.split("/").takeRight(3).mkString("/")
  }

  // derive team from path to file
  def pathToTeam(confPath: String): String = {
    //  capture <conf_type>/<team> as key e.g group_bys/host_churn
    confPath.split("/").takeRight(3).take(2).mkString("/")
  }

  // list file recursively
  def listFiles(base: File, recursive: Boolean = true): Seq[File] = {
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
  def loadJson[T <: TBase[_, _]: Manifest: ClassTag](file: String): Option[String] = {
    try {
      val configConf = ThriftJsonCodec.fromJsonFile[T](file, check = true)
      Some(ThriftJsonCodec.toJsonStr(configConf))
    } catch {
      case e: Throwable =>
        logger.error(s"Failed to parse compiled Chronon config file: $file, \nerror=${e.getMessage}")
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
        logger.error(s"Failed to parse Chronon config file at $path as JSON", ex)
        ex.printStackTrace()
        None
    }
  }

  // upload the materialized JSONs to KV store:
  // key = <conf_type>/<team>/<conf_name> in bytes e.g joins/team/team.example_join.v1 value = materialized json string in bytes
  def putConfByName(configPath: String): Future[Seq[Boolean]] = {
    val configFile = new File(configPath)
    assert(configFile.exists(), s"$configFile does not exist")
    logger.info(s"Uploading Chronon configs from $configPath")
    val fileList = listFiles(configFile)

    val puts: Seq[PutRequest] = fileList
      .filter { file =>
        val name = parseName(file.getPath)
        if (name.isEmpty) logger.info(s"Skipping invalid file ${file.getPath}")
        name.isDefined
      }
      .flatMap { file =>
        val path = file.getPath
        val key = pathToKey(path)
        val confJsonOpt = path match {
          case value if value.contains("staging_queries/") => loadJson[StagingQuery](value)
          case value if value.contains("joins/")           => loadJson[Join](value)
          case value if value.contains("group_bys/")       => loadJson[GroupBy](value)
          case _                                           => logger.info(s"unknown config type in file $path"); None
        }

        confJsonOpt.map { value =>
          logger.info(s"""Putting metadata for
               |key: $key
               |conf: $value""".stripMargin)
          PutRequest(keyBytes = key.getBytes(),
                     valueBytes = value.getBytes(),
                     dataset = dataset,
                     tsMillis = Some(System.currentTimeMillis()))
        }
      }
    val putsBatches = puts.grouped(CONF_BATCH_SIZE).toSeq
    logger.info(s"Putting ${puts.size} configs to KV Store, dataset=$dataset")
    val futures = putsBatches.map(batch => kvStore.multiPut(batch))
    Future.sequence(futures).map(_.flatten)
  }

  // upload the configs list by team to KV store:
  // key = <conf_type>/<team> in bytes, e.g group_bys/host_churn
  // value = list of config names in bytes, e.g List(group_bys/host_churn/agg_p2_impressions.v1, group_bys/host_churn/agg_deactivations.v1)
  def putConfByTeam(configPath: String): Future[Seq[Boolean]] = {

    val configFile = new File(configPath)
    assert(configFile.exists(), s"$configFile does not exist")
    logger.info(s"Uploading Chronon configs from $configPath")
    val fileList = listFiles(configFile)
    val nameByTeam: Map[String, List[String]] = Map()
    val validFileList = fileList
      .filter { file =>
        val name = parseName(file.getPath)
        if (name.isEmpty) logger.info(s"Skipping invalid file ${file.getPath}")
        name.isDefined
      }

    val kvPairs: Map[String, List[String]] = validFileList.foldLeft(Map.empty[String, List[String]]) { (map, file) =>
      {
        val path = file.getPath
        val key = pathToTeam(path)
        val value = pathToKey(path)
        val updatedList = map.getOrElse(key, List()) :+ value
        map + (key -> updatedList)
      }
    }

    val puts: Seq[PutRequest] = kvPairs.map {
      case (key, list) => {
        val jsonString = "[" + list.map(name => "\"" + name + "\"").mkString(",") + "]"
        logger.info(s"""Putting metadata for
             |key: $key
             |conf: $jsonString""".stripMargin)
        PutRequest(keyBytes = key.getBytes(),
                   valueBytes = jsonString.getBytes(),
                   dataset = dataset,
                   tsMillis = Some(System.currentTimeMillis()))
      }
    }.toSeq

    val putsBatches = puts.grouped(CONF_BATCH_SIZE).toSeq
    logger.info(s"Putting ${puts.size} configs to KV Store, dataset=$dataset")
    val futures = putsBatches.map(batch => kvStore.multiPut(batch))
    Future.sequence(futures).map(_.flatten)
  }
}
