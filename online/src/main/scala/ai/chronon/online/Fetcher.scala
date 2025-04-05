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

import ai.chronon.aggregator.row.{ColumnAggregator, StatsGenerator}
import ai.chronon.api
import ai.chronon.api.Constants.UTF8
import ai.chronon.api.Extensions.{
  ExternalPartOps,
  GroupByOps,
  JoinOps,
  JoinPartOps,
  MetadataOps,
  StringOps,
  ThrowableOps
}
import ai.chronon.api._
import ai.chronon.online.Fetcher._
import ai.chronon.online.KVStore.GetRequest
import ai.chronon.online.Metrics.Environment
import ai.chronon.online.OnlineDerivationUtil.{applyDeriveFunc, buildDerivedFields}
import com.google.gson.Gson
import com.timgroup.statsd.Event
import com.timgroup.statsd.Event.AlertType
import org.apache.avro.generic.GenericRecord
import org.json4s.BuildInfo

import java.util.function.Consumer
import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer
import scala.collection.{Seq, mutable}
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Random, Success, Try}

object Fetcher {
  case class Request(name: String,
                     keys: Map[String, AnyRef],
                     atMillis: Option[Long] = None,
                     context: Option[Metrics.Context] = None)

  case class PrefixedRequest(prefix: String, request: Request)
  case class StatsRequest(name: String, startTs: Option[Long] = None, endTs: Option[Long] = None)
  case class StatsResponse(request: StatsRequest, values: Try[Map[String, AnyRef]], millis: Long)
  case class SeriesStatsResponse(request: StatsRequest, values: Try[Map[String, AnyRef]])
  case class Response(request: Request, values: Try[Map[String, AnyRef]])
  case class ResponseWithContext(request: Request,
                                 derivedValues: Map[String, AnyRef],
                                 baseValues: Map[String, AnyRef]) {
    def combinedValues: Map[String, AnyRef] = baseValues ++ derivedValues
  }
  case class ColumnSpec(groupByName: String,
                        columnName: String,
                        prefix: Option[String],
                        keyMapping: Option[Map[String, AnyRef]])

  def logResponseStats(response: Response, context: Metrics.Context): Unit = {
    val responseMap = response.values.get
    var exceptions = 0
    var nulls = 0
    responseMap.foreach {
      case (k, v) =>
        if (v == null) nulls += 1
        else if (v.isInstanceOf[Throwable] || k.endsWith("_exception")) exceptions += 1
    }
    context.distribution(Metrics.Name.FetchNulls, nulls)
    context.distribution(Metrics.Name.FetchExceptions, exceptions)
    context.distribution(Metrics.Name.FetchCount, responseMap.size)
  }
}

private[online] case class FetcherResponseWithTs(responses: scala.collection.Seq[Response], endTs: Long)

// BaseFetcher + Logging + External service calls
class Fetcher(val kvStore: KVStore,
              metaDataSet: String,
              timeoutMillis: Long = 10000,
              logFunc: Consumer[LoggableResponse] = null,
              debug: Boolean = false,
              val externalSourceRegistry: ExternalSourceRegistry = null,
              callerName: String = null,
              flagStore: FlagStore = null,
              disableErrorThrows: Boolean = false,
              executionContextOverride: ExecutionContext = null)
    extends FetcherBase(kvStore,
                        metaDataSet,
                        timeoutMillis,
                        debug,
                        flagStore,
                        disableErrorThrows,
                        executionContextOverride) {
  private def reportCallerNameFetcherVersion(): Unit = {
    val message = s"CallerName: ${Option(callerName).getOrElse("N/A")}, FetcherVersion: ${BuildInfo.version}"
    val ctx = Metrics.Context(Environment.Fetcher)
    val event = Event
      .builder()
      .withTitle("FetcherInitialization")
      .withText(message)
      .withAlertType(AlertType.INFO)
      .build()
    ctx.recordEvent("caller_name_fetcher_version", event)
  }

  // run during initialization
  reportCallerNameFetcherVersion()

  private def buildJoinPartCodec(
      joinPart: JoinPartOps,
      servingInfo: GroupByServingInfoParsed): (Iterable[StructField], Iterable[StructField]) = {
    val keySchema = servingInfo.keyCodec.chrononSchema.asInstanceOf[StructType]
    val joinKeyFields = joinPart.leftToRight
      .map {
        case (leftKey, rightKey) =>
          StructField(leftKey, keySchema.fields.find(_.name == rightKey).get.fieldType)
      }

    val baseValueSchema: StructType = if (servingInfo.groupBy.aggregations == null) {
      servingInfo.selectedChrononSchema
    } else {
      servingInfo.outputChrononSchema
    }
    val valueFields = if (!servingInfo.groupBy.hasDerivations) {
      baseValueSchema.fields
    } else {
      buildDerivedFields(servingInfo.groupBy.derivationsScala, keySchema, baseValueSchema).toArray
    }
    val joinValueFields = valueFields.map(joinPart.constructJoinPartSchema)

    (joinKeyFields, joinValueFields)
  }

  def buildJoinCodec(joinConf: Join, refreshOnFail: Boolean): (JoinCodec, Boolean) = {
    val keyFields = new mutable.LinkedHashSet[StructField]
    val valueFields = new mutable.ListBuffer[StructField]
    var hasPartialFailure = false
    // collect keyFields and valueFields from joinParts/GroupBys
    joinConf.joinPartOps.foreach { joinPart =>
      getGroupByServingInfo(joinPart.groupBy.metaData.getName)
        .map { servingInfo =>
          val (keys, values) = buildJoinPartCodec(joinPart, servingInfo)
          keys.foreach(k => keyFields.add(k))
          values.foreach(v => valueFields.append(v))
        }
        .recoverWith {
          case exception: Throwable => {
            if (refreshOnFail) {
              getGroupByServingInfo.refresh(joinPart.groupBy.metaData.getName)
              hasPartialFailure = true
              Success(())
            } else {
              Failure(new Exception(
                s"Failure to build join codec for join ${joinConf.metaData.name} due to bad groupBy serving info for ${joinPart.groupBy.metaData.name}",
                exception))
            }
          }
        }
        .get
    }

    // gather key schema and value schema from external sources.
    Option(joinConf.join.onlineExternalParts).foreach { externals =>
      externals
        .iterator()
        .asScala
        .foreach { part =>
          val source = part.source

          def buildFields(schema: TDataType, prefix: String = ""): Seq[StructField] =
            DataType
              .fromTDataType(schema)
              .asInstanceOf[StructType]
              .fields
              .map(f => StructField(prefix + f.name, f.fieldType))

          buildFields(source.getKeySchema).foreach(f =>
            keyFields.add(f.copy(name = part.rightToLeft.getOrElse(f.name, f.name))))
          buildFields(source.getValueSchema, part.fullName + "_").foreach(f => valueFields.append(f))
        }
    }

    val joinName = joinConf.metaData.nameToFilePath
    val keySchema = StructType(s"${joinName.sanitize}_key", keyFields.toArray)
    val baseValueSchema = StructType(s"${joinName.sanitize}_value", valueFields.toArray)
    val joinCodec = JoinCodec(joinConf, keySchema, baseValueSchema)
    logControlEvent(joinCodec)
    (joinCodec, hasPartialFailure)
  }

  // key and value schemas
  lazy val getJoinCodecs = new TTLCache[String, Try[(JoinCodec, Boolean)]](
    { joinName: String =>
      val startTimeMs = System.currentTimeMillis()
      val result: Try[(JoinCodec, Boolean)] = getJoinConf(joinName)
        .map(_.join)
        .map(join => buildJoinCodec(join, refreshOnFail = true))
        .recoverWith {
          case th: Throwable =>
            Failure(
              new RuntimeException(
                s"Couldn't fetch joinName = ${joinName} or build join codec due to ${th.traceString}",
                th
              ))
        }
      val context = Metrics.Context(Metrics.Environment.MetaDataFetching, join = joinName).withSuffix("join_codec")
      if (result.isFailure) {
        context.incrementException(result.failed.get)
      } else {
        context.distribution(Metrics.Name.LatencyMillis, System.currentTimeMillis() - startTimeMs)
      }
      result
    },
    { join: String => Metrics.Context(environment = "join.codec.fetch", join = join) })

  private[online] def withTs(responses: Future[scala.collection.Seq[Response]]): Future[FetcherResponseWithTs] = {
    responses.map { response =>
      FetcherResponseWithTs(response, System.currentTimeMillis())
    }
  }

  override def fetchJoin(requests: scala.collection.Seq[Request],
                         joinConf: Option[api.Join] = None): Future[scala.collection.Seq[Response]] = {
    val ts = System.currentTimeMillis()
    val internalResponsesF = super.fetchJoin(requests, joinConf)
    val externalResponsesF = fetchExternal(requests)
    val combinedResponsesF = internalResponsesF.zip(externalResponsesF).map {
      case (internalResponses, externalResponses) =>
        internalResponses.zip(externalResponses).map {
          case (internalResponse, externalResponse) =>
            if (debug) {
              logger.info(internalResponse.values.get.keys.toSeq.mkString(","))
              logger.info(externalResponse.values.get.keys.toSeq.mkString(","))
            }
            val cleanInternalRequest = internalResponse.request.copy(context = None)
            assert(
              cleanInternalRequest == externalResponse.request,
              s"""
                 |Logic error. Responses are not aligned to requests
                 |mismatching requests:  ${cleanInternalRequest}, ${externalResponse.request}
                 |  requests:            ${requests.map(_.name)}
                 |  internalResponses:   ${internalResponses.map(_.request.name)}
                 |  externalResponses:   ${externalResponses.map(_.request.name)}""".stripMargin
            )
            val internalMap = internalResponse.values.getOrElse(
              Map("join_part_fetch_exception" -> internalResponse.values.failed.get.traceString))
            val externalMap = externalResponse.values.getOrElse(
              Map("external_part_fetch_exception" -> externalResponse.values.failed.get.traceString))
            val derivationStartTs = System.currentTimeMillis()
            val joinName = internalResponse.request.name
            val ctx = Metrics.Context(Environment.JoinFetching, join = joinName)
            val joinCodecTry = getJoinCodecs(internalResponse.request.name)
            joinCodecTry match {
              case Success((joinCodec, hasPartialFailure)) =>
                ctx.distribution("derivation_codec.latency.millis", System.currentTimeMillis() - derivationStartTs)
                // try to fix request mistype
                val keySchemaMap: Map[String, DataType] = joinCodec.keySchema.fields.map { field =>
                  field.name -> field.fieldType
                }.toMap
                val castedKeys: Map[String, AnyRef] = internalResponse.request.keys.map {
                  case (name, value) =>
                    name -> (if (keySchemaMap.contains(name)) ColumnAggregator.castTo(value, keySchemaMap(name))
                             else value)
                }
                val request = Request(name = internalResponse.request.name,
                                      keys = castedKeys,
                                      atMillis = internalResponse.request.atMillis,
                                      context = internalResponse.request.context)

                val baseMap = internalMap ++ externalMap
                val derivedMapTry: Try[Map[String, AnyRef]] = Try {
                  applyDeriveFunc(joinCodec.deriveFunc, request, baseMap)
                }
                val derivedMap: Map[String, AnyRef] = derivedMapTry match {
                  case Success(derivedMap) => derivedMap
                  case Failure(exception) =>
                    ctx.incrementException(exception)
                    val renameOnlyDerivedMapTry: Try[Map[String, AnyRef]] = Try {
                      joinCodec
                        .renameOnlyDeriveFunc(internalResponse.request.keys, baseMap)
                        .mapValues(_.asInstanceOf[AnyRef])
                        .toMap
                    }
                    val renameOnlyDerivedMap: Map[String, AnyRef] = renameOnlyDerivedMapTry match {
                      case Success(renameOnlyDerivedMap) =>
                        renameOnlyDerivedMap
                      case Failure(exception) =>
                        ctx.incrementException(exception)
                        Map("derivation_rename_exception" -> exception.traceString.asInstanceOf[AnyRef])
                    }
                    val derivedExceptionMap: Map[String, AnyRef] =
                      Map("derivation_fetch_exception" -> exception.traceString.asInstanceOf[AnyRef])
                    renameOnlyDerivedMap ++ derivedExceptionMap
                }
                // Preserve exceptions from baseMap
                val baseMapExceptions = baseMap.filter(_._1.endsWith("_exception"))
                val finalizedDerivedMap = derivedMap ++ baseMapExceptions
                val requestEndTs = System.currentTimeMillis()
                ctx.distribution("derivation.latency.millis", requestEndTs - derivationStartTs)
                ctx.distribution("overall.latency.millis", requestEndTs - ts)
                val response = ResponseWithContext(request, finalizedDerivedMap, baseMap)
                // Refresh joinCodec if it has partial failure
                if (hasPartialFailure) {
                  getJoinCodecs.refresh(joinName)
                }
                response
              case Failure(exception) =>
                // more validation logic will be covered in compile.py to avoid this case
                ctx.incrementException(exception)
                ResponseWithContext(internalResponse.request,
                                    Map("join_codec_fetch_exception" -> exception.traceString),
                                    Map.empty)
            }
        }
    }

    combinedResponsesF
      .map(_.iterator.map(logResponse(_, ts)).toSeq)
  }

  private def encode(loggingContext: Option[Metrics.Context],
                     schema: StructType,
                     codec: AvroCodec,
                     dataMap: Map[String, AnyRef],
                     cast: Boolean = false,
                     tries: Int = 3): Try[Array[Byte]] = {
    def encodeOnce(schema: StructType,
                   codec: AvroCodec,
                   dataMap: Map[String, AnyRef],
                   cast: Boolean = false): Array[Byte] = {
      val data = schema.fields.map {
        case StructField(name, typ) =>
          val elem = dataMap.getOrElse(name, null)
          // handle cases where a join contains keys of the same name but different types
          // e.g. `listing` is a long in one groupby, but a string in another groupby
          if (cast) {
            ColumnAggregator.castTo(elem, typ)
          } else {
            elem
          }
      }
      val avroRecord = AvroConversions.fromChrononRow(data, schema, codec.schema).asInstanceOf[GenericRecord]
      codec.encodeBinary(avroRecord)
    }

    def tryOnce(lastTry: Try[Array[Byte]], tries: Int, totalTries: Int): Try[Array[Byte]] = {
      if (tries == 0 || (lastTry != null && lastTry.isSuccess)) {
        loggingContext.foreach(_.gauge("tries", totalTries - tries))
        return lastTry
      }
      val binary = encodeOnce(schema, codec, dataMap, cast)
      tryOnce(Try(codec.decodeRow(binary)).map(_ => binary), tries - 1, totalTries)
    }

    tryOnce(null, tries, totalTries = tries)
  }

  private def logResponse(resp: ResponseWithContext, ts: Long): Response = {
    val loggingStartTs = System.currentTimeMillis()
    val loggingContext = resp.request.context.map(_.withSuffix("logging_request"))
    val loggingTs = resp.request.atMillis.getOrElse(ts)
    val joinCodecTry = getJoinCodecs(resp.request.name)

    val loggingTry: Try[Unit] = joinCodecTry
      .map(_._1)
      .map(codec => {
        val metaData = codec.conf.join.metaData
        val samplePercent = if (metaData.isSetSamplePercent) metaData.getSamplePercent else 0

        // Exit early if sample percent is 0
        if (samplePercent == 0) {
          return Response(resp.request, Success(resp.derivedValues))
        }

        val keyBytesTry: Try[Array[Byte]] = encode(loggingContext.map(_.withSuffix("encode_key")),
                                                   codec.keySchema,
                                                   codec.keyCodec,
                                                   resp.request.keys,
                                                   cast = true)
        if (keyBytesTry.isFailure) {
          loggingContext.foreach(_.withSuffix("encode_key").incrementException(keyBytesTry.failed.get))
          throw keyBytesTry.failed.get
        }
        val keyBytes = keyBytesTry.get
        val hash = if (samplePercent > 0) {
          Math.abs(HashUtils.md5Long(keyBytes))
        } else {
          -1
        }
        val shouldPublishLog = (hash > 0) && ((hash % (100 * 1000)) <= (samplePercent * 1000))
        if (shouldPublishLog || debug) {
          val values = if (codec.conf.join.logFullValues) {
            resp.combinedValues
          } else {
            resp.derivedValues
          }

          if (debug) {
            logger.info(s"Logging ${resp.request.keys} : ${hash % 100000}: $samplePercent")
            val gson = new Gson()
            val valuesFormatted = values.map { case (k, v) => s"$k -> ${gson.toJson(v)}" }.mkString(", ")
            logger.info(s"""Sampled join fetch
               |Key Map: ${resp.request.keys}
               |Value Map: [${valuesFormatted}]
               |""".stripMargin)
          }

          val valueBytesTry: Try[Array[Byte]] =
            encode(loggingContext.map(_.withSuffix("encode_value")), codec.valueSchema, codec.valueCodec, values)
          if (valueBytesTry.isFailure) {
            loggingContext.foreach(_.withSuffix("encode_value").incrementException(valueBytesTry.failed.get))
            throw valueBytesTry.failed.get
          }
          val valueBytes = valueBytesTry.get

          val loggableResponse = LoggableResponse(
            keyBytes,
            valueBytes,
            resp.request.name,
            loggingTs,
            codec.loggingSchemaHash
          )
          if (logFunc != null) {
            logFunc.accept(loggableResponse)
            loggingContext.foreach(context => context.increment("count"))
            loggingContext.foreach(context =>
              context.distribution("latency.millis", System.currentTimeMillis() - loggingStartTs))
            loggingContext.foreach(context =>
              context.distribution("overall.latency.millis", System.currentTimeMillis() - ts))

            if (debug) {
              logger.info(s"Logged data with schema_hash ${codec.loggingSchemaHash}")
            }
          }
        }
      })
    loggingTry.failed.map { exception =>
      // Publish logging failure metrics before codec refresh in case of another exception
      loggingContext.foreach(_.incrementException(exception)(logger))

      // to handle GroupByServingInfo staleness that results in encoding failure
      val refreshedJoinCodec = getJoinCodecs.refresh(resp.request.name).map(_._1)
      if (debug) {
        val rand = new Random
        val number = rand.nextDouble
        val defaultSamplePercent = 0.001
        val logSamplePercent = refreshedJoinCodec
          .map(codec =>
            if (codec.conf.join.metaData.isSetSamplePercent) codec.conf.join.metaData.getSamplePercent
            else defaultSamplePercent)
          .getOrElse(defaultSamplePercent)
        if (number < logSamplePercent) {
          logger.error(s"Logging failed due to: ${exception.traceString}, join codec status: ${joinCodecTry.isSuccess}")
          if (joinCodecTry.isFailure) {
            joinCodecTry.failed.foreach { exception =>
              logger.error(s"Logging failed due to failed join codec: ${exception.traceString}")
            }
          } else {
            logger.error(s"Join codec status is Success but logging failed due to: ${exception.traceString}")
          }
        }
      }
    }
    if (joinCodecTry.isSuccess && joinCodecTry.get._2) {
      getJoinCodecs.refresh(resp.request.name)
    }
    Response(resp.request, Success(resp.derivedValues))
  }

  // Pulling external features in a batched fashion across services in-parallel
  def fetchExternal(joinRequests: scala.collection.Seq[Request]): Future[scala.collection.Seq[Response]] = {
    val startTime = System.currentTimeMillis()
    val resultMap = new mutable.LinkedHashMap[Request, Try[mutable.HashMap[String, Any]]]
    var invalidCount = 0
    val validRequests = new ListBuffer[Request]

    // step-1 handle invalid requests and collect valid ones
    joinRequests.foreach { request =>
      val joinName = request.name
      val joinConfTry: Try[JoinOps] = getJoinConf(request.name)
      if (joinConfTry.isFailure) {
        resultMap.update(
          request,
          Failure(
            new IllegalArgumentException(
              s"Failed to fetch join conf for $joinName. Please ensure metadata upload succeeded",
              joinConfTry.failed.get))
        )
        invalidCount += 1
      } else if (joinConfTry.get.join.onlineExternalParts == null) {
        resultMap.update(request, Success(mutable.HashMap.empty[String, Any]))
      } else {
        resultMap.update(request, Success(mutable.HashMap.empty[String, Any]))
        validRequests.append(request)
      }
    }

    // step-2 dedup external requests across joins
    val externalToJoinRequests: Seq[ExternalToJoinRequest] = validRequests
      .flatMap { joinRequest =>
        val parts =
          getJoinConf(joinRequest.name).get.join.onlineExternalParts // cheap since it is cached, valid since step-1
        parts.iterator().asScala.map { part =>
          val externalRequest = Try(part.applyMapping(joinRequest.keys)) match {
            case Success(mappedKeys)                     => Left(Request(part.source.metadata.name, mappedKeys))
            case Failure(exception: KeyMissingException) => Right(exception)
            case Failure(otherException)                 => throw otherException
          }
          ExternalToJoinRequest(externalRequest, joinRequest, part)
        }
      }
    val validExternalRequestToJoinRequestMap = externalToJoinRequests
      .filter(_.externalRequest.isLeft)
      .groupBy(_.externalRequest.left.get)
      .mapValues(_.toSeq)
      .toMap

    val context =
      Metrics.Context(environment = Environment.JoinFetching,
                      join = validRequests.iterator.map(_.name.sanitize).toSeq.distinct.mkString(","))
    context.distribution("response.external_pre_processing.latency", System.currentTimeMillis() - startTime)
    context.count("response.external_invalid_joins.count", invalidCount)
    val responseFutures = externalSourceRegistry.fetchRequests(validExternalRequestToJoinRequestMap.keys.toSeq, context)

    // step-3 walk the response, find all the joins to update and the result map
    responseFutures.map { responses =>
      responses.foreach { response =>
        val responseTry: Try[Map[String, Any]] = response.values
        val joinsToUpdate: Seq[ExternalToJoinRequest] = validExternalRequestToJoinRequestMap(response.request)
        joinsToUpdate.foreach { externalToJoin =>
          val resultValueMap: mutable.HashMap[String, Any] = resultMap(externalToJoin.joinRequest).get
          val prefix = externalToJoin.part.fullName + "_"
          responseTry match {
            case Failure(exception) =>
              resultValueMap.update(prefix + "exception", exception)
              externalToJoin.context.incrementException(exception)
            case Success(responseMap) =>
              externalToJoin.context.count("response.value_count", responseMap.size)
              responseMap.foreach { case (name, value) => resultValueMap.update(prefix + name, value) }
          }
        }
      }

      externalToJoinRequests
        .filter(_.externalRequest.isRight)
        .foreach(externalToJoin => {
          val resultValueMap: mutable.HashMap[String, Any] = resultMap(externalToJoin.joinRequest).get
          val KeyMissingException = externalToJoin.externalRequest.right.get
          resultValueMap.update(externalToJoin.part.fullName + "_" + "exception", KeyMissingException)
          externalToJoin.context.incrementException(KeyMissingException)
        })

      // step-4 convert the resultMap into Responses
      joinRequests.map { req =>
        Metrics
          .Context(Environment.JoinFetching, join = req.name)
          .distribution("external.latency.millis", System.currentTimeMillis() - startTime)
        Response(req, resultMap(req).map(_.mapValues(_.asInstanceOf[AnyRef]).toMap))
      }
    }
  }

  private def logControlEvent(enc: JoinCodec): Unit = {
    val ts = System.currentTimeMillis()
    val controlEvent = LoggableResponse(
      enc.loggingSchemaHash.getBytes(UTF8),
      enc.loggingSchema.getBytes(UTF8),
      Constants.SchemaPublishEvent,
      ts,
      null
    )
    if (logFunc != null) {
      logFunc.accept(controlEvent)
      if (debug) {
        logger.info(s"schema data logged successfully with schema_hash ${enc.loggingSchemaHash}")
      }
    }
  }

  /** Main endpoint for fetching backfill tables stats or drifts. */
  def fetchStatsTimeseries(joinRequest: StatsRequest): Future[SeriesStatsResponse] =
    fetchDriftOrStatsTimeseries(joinRequest, fetchMetricsTimeseriesFromDataset(_, Constants.StatsBatchDataset))

  /** Main endpoint for fetching OOC metrics stats or drifts. */
  def fetchConsistencyMetricsTimeseries(joinRequest: StatsRequest): Future[SeriesStatsResponse] =
    fetchDriftOrStatsTimeseries(joinRequest, fetchMetricsTimeseriesFromDataset(_, Constants.ConsistencyMetricsDataset))

  /** Main endpoint for fetching logging stats or drifts. */
  def fetchLogStatsTimeseries(joinRequest: StatsRequest): Future[SeriesStatsResponse] =
    fetchDriftOrStatsTimeseries(joinRequest, fetchMetricsTimeseriesFromDataset(_, Constants.LogStatsBatchDataset))

  private def fetchMetricsTimeseriesFromDataset(joinRequest: StatsRequest,
                                                dataset: String): Future[Seq[StatsResponse]] = {
    val keyCodec = getStatsSchemaFromKVStore(dataset, s"${joinRequest.name}${Constants.TimedKvRDDKeySchemaKey}")
    val valueCodec = getStatsSchemaFromKVStore(dataset, s"${joinRequest.name}${Constants.TimedKvRDDValueSchemaKey}")
    val upperBound: Long = joinRequest.endTs.getOrElse(System.currentTimeMillis())
    val responseFuture: Future[Seq[StatsResponse]] = kvStore
      .get(GetRequest(keyCodec.encodeArray(Array(joinRequest.name)), dataset, afterTsMillis = joinRequest.startTs))
      .map(
        _.values.get.toArray
          .filter(_.millis <= upperBound)
          .map { tv =>
            StatsResponse(joinRequest, Try(valueCodec.decodeMap(tv.bytes)), millis = tv.millis)
          }
          .toSeq)
    responseFuture
  }

  /**
    * Given a sequence of stats responses for different time intervals, re arrange it into a map containing the time
    * series for each statistic.
    */
  private def convertStatsResponseToSeriesResponse(
      joinRequest: StatsRequest,
      rawResponses: Future[Seq[StatsResponse]]): Future[SeriesStatsResponse] = {
    rawResponses.map { responseFuture =>
      val convertedValue = responseFuture
        .flatMap { response =>
          response.values
            .getOrElse(Map.empty[String, AnyRef])
            .map {
              case (key, v) =>
                key ->
                  Map(
                    "millis" -> response.millis.asInstanceOf[AnyRef],
                    "value" -> StatsGenerator.SeriesFinalizer(key, v)
                  ).asJava
            }
        }
        .groupBy(_._1)
        .mapValues(_.map(_._2).toList.asJava)
        .toMap
      SeriesStatsResponse(joinRequest, Try(convertedValue))
    }
  }

  /**
    * Given a sequence of stats responses for different time intervals, re arrange it into a map containing the drift
    * for
    * the approx percentile metrics.
    * TODO: Extend to larger periods of time by merging the Sketches from a larger slice.
    * TODO: Allow for non sequential time intervals. i.e. this week against the same week last year.
    */
  private def convertStatsResponseToDriftResponse(
      joinRequest: StatsRequest,
      rawResponses: Future[Seq[StatsResponse]]): Future[SeriesStatsResponse] =
    rawResponses.map { response =>
      val driftMap = response
        .sortBy(_.millis)
        .sliding(2)
        .collect {
          case Seq(prev, curr) =>
            val commonKeys = prev.values.get.keySet.intersect(curr.values.get.keySet.filter(_.endsWith("percentile")))
            commonKeys
              .map { key =>
                val previousValue = prev.values.get(key)
                val currentValue = curr.values.get(key)
                key -> Map(
                  "millis" -> curr.millis.asInstanceOf[AnyRef],
                  "value" -> StatsGenerator.PSIKllSketch(previousValue, currentValue)
                ).asJava
              }
              .filter(_._2.get("value") != None)
              .toMap
        }
        .toSeq
        .flatMap(_.toSeq)
        .groupBy(_._1)
        .mapValues(_.map(_._2).toList.asJava)
        .toMap
      SeriesStatsResponse(joinRequest, Try(driftMap))
    }

  /**
    * Main helper for fetching statistics over time available.
    * It takes a function that will get the stats for the specific dataset (OOC, LOG, Backfill stats) and then operates
    * on it to either return a time series of the features or drift between the approx percentile features.
    */
  private def fetchDriftOrStatsTimeseries(
      joinRequest: StatsRequest,
      fetchFunc: StatsRequest => Future[Seq[StatsResponse]]): Future[SeriesStatsResponse] = {
    if (joinRequest.name.endsWith("/drift")) {
      // In the case of drift we only find the percentile keys and do a shifted distance.
      val rawResponses = fetchFunc(
        StatsRequest(joinRequest.name.dropRight("/drift".length), joinRequest.startTs, joinRequest.endTs))
      return convertStatsResponseToDriftResponse(joinRequest, rawResponses)
    }
    convertStatsResponseToSeriesResponse(joinRequest, fetchFunc(joinRequest))
  }

  private case class ExternalToJoinRequest(externalRequest: Either[Request, KeyMissingException],
                                           joinRequest: Request,
                                           part: ExternalPart) {
    lazy val context: Metrics.Context =
      Metrics.Context(Environment.JoinFetching, join = joinRequest.name, groupBy = part.fullName)
  }
}
