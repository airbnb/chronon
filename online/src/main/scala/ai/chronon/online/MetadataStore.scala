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
import ai.chronon.online.MetadataEndPoint.{ConfByKeyEndPointName, NameByTeamEndPointName}
import ai.chronon.online.Metrics.Name.Exception
import com.google.gson.{Gson, GsonBuilder}
import org.apache.thrift.TBase

import java.io.File
import java.nio.file.{Files, Paths}
import scala.collection.immutable.SortedMap
import scala.collection.Seq
import scala.concurrent.{ExecutionContext, Future}
import scala.reflect.ClassTag
import scala.util.{Failure, Success, Try}

// [timestamp -> {metric name -> metric value}]
case class DataMetrics(series: Seq[(Long, SortedMap[String, Any])])

class MetadataStore(kvStore: KVStore,
                    val dataset: String = ChrononMetadataKey,
                    timeoutMillis: Long,
                    executionContextOverride: ExecutionContext = null) {
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

  implicit val executionContext: ExecutionContext =
    Option(executionContextOverride).getOrElse(FlexibleExecutionContext.buildExecutionContext)

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

  def getEntityListByTeam[T <: TBase[_, _]: Manifest](team: String): Try[Seq[String]] = {
    val clazz = implicitly[ClassTag[T]].runtimeClass.asInstanceOf[Class[T]]
    val dataset = NameByTeamEndPointName
    kvStore
      .getStringArray(team, dataset, timeoutMillis)
      .recoverWith {
        case th: Throwable =>
          Failure(
            new RuntimeException(
              s"Couldn't fetch ${clazz.getName} for key $team. Perhaps metadata upload wasn't successful.",
              th
            ))
      }
  }

  lazy val getGroupByListByTeam: TTLCache[String, Try[Seq[String]]] = {
    new TTLCache[String, Try[Seq[String]]](
      { team =>
        getEntityListByTeam[GroupBy]("group_bys/" + team)
          .recover {
            case e: java.util.NoSuchElementException =>
              logger.error(
                s"Failed to fetch conf for team $team at group_bys/$team, please check metadata upload to make sure the metadata has been uploaded")
              throw e
          }
      },
      { team => Metrics.Context(environment = "group_by.list.fetch", groupBy = team) }
    )
  }

  lazy val getJoinListByTeam: TTLCache[String, Try[Seq[String]]] = {
    new TTLCache[String, Try[Seq[String]]](
      { team =>
        getEntityListByTeam[Join]("joins/" + team)
          .recover {
            case e: java.util.NoSuchElementException =>
              logger.error(
                s"Failed to fetch conf for team $team at joins/$team, please check metadata upload to make sure the metadata has been uploaded")
              throw e
          }
      },
      { team => Metrics.Context(environment = "join.list.fetch", groupBy = team) }
    )
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
        context.withSuffix("join").incrementException(result.failed.get)
        throw result.failed.get
      }
      context.withSuffix("join").distribution(Metrics.Name.LatencyMillis, System.currentTimeMillis() - startTimeMs)
      result
    },
    { join => Metrics.Context(environment = "join.meta.fetch", join = join) })

  // Validate whether the join exists in the active join list saved in kv store
  def validateJoinExist(team: String, name: String): Boolean = {
    val activeJoinList: Try[Seq[String]] = getJoinListByTeam(team)
    if (activeJoinList.isFailure) {
      logger.error(s"Failed to fetch active join list for team $team")
      false
    } else {
      val joinKey = "joins/" + name
      if (activeJoinList.get.contains(joinKey)) {
        true
      } else {
        logger.error(s"Join $name not found in active join list for team $team")
        false
      }
    }
  }

  // Validate whether the groupBy exists in the active groupBy list saved in kv store
  def validateGroupByExist(team: String, name: String): Boolean = {
    val activeGroupByList: Try[Seq[String]] = getGroupByListByTeam(team)
    if (activeGroupByList.isFailure) {
      logger.error(s"Failed to fetch active group_by list for team $team")
      false
    } else {
      // The groupBy fetch could be through the groupBy fetch directly or through the join part fetch inside a join.
      // For the first situation the name is the name user sent to the fetcher client, like zipline_test/test_online_group_by_small.v2
      // For the second situation the name is the join part name, like zipline_test.test_online_group_by_small.v2
      // The work around here is to normalize the groupBy name.
      val groupByKey = if (name.contains("/")) {
        "group_bys/" + name
      } else {
        "group_bys/" + name.replaceFirst("\\.", "/")
      }
      if (activeGroupByList.get.contains(groupByKey)) {
        true
      } else {
        logger.error(s"GroupBy $name not found in active group_by list for team $team")
        false
      }
    }
  }

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
          Metrics
            .Context(Metrics.Environment.MetaDataFetching, groupBy = name)
            .withSuffix("group_by")
            .incrementException(metaData.failed.get)
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

  def put(
      kVPairs: Map[String, Seq[String]],
      datasetName: String = ChrononMetadataKey,
      batchSize: Int = CONF_BATCH_SIZE
  ): Future[Seq[Boolean]] = {
    val puts = kVPairs.map {
      case (k, v) => {
        logger.info(s"""Putting metadata for
             |dataset: $datasetName
             |key: $k
             |conf: $v""".stripMargin)
        val kBytes = k.getBytes()
        // The value is a single string by default, for NameByTeamEndPointName, it's a list of strings
        val vBytes = if (datasetName == NameByTeamEndPointName) {
          StringArrayConverter.stringsToBytes(v)
        } else {
          v.head.getBytes()
        }
        PutRequest(keyBytes = kBytes,
                   valueBytes = vBytes,
                   dataset = datasetName,
                   tsMillis = Some(System.currentTimeMillis()))
      }
    }.toSeq
    val putsBatches = puts.grouped(batchSize).toSeq
    logger.info(s"Putting ${puts.size} configs to KV Store, dataset=$datasetName, batchSize=$batchSize")
    val futures = putsBatches.map(batch => kvStore.multiPut(batch))
    Future.sequence(futures).map(_.flatten)
  }
}
