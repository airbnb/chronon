package ai.chronon.online

import ai.chronon.aggregator.row.{ColumnAggregator, StatsGenerator}
import ai.chronon.api
import ai.chronon.api.Constants.UTF8
import ai.chronon.api.Extensions.{ExternalPartOps, JoinOps, MetadataOps, StringOps, ThrowableOps}
import ai.chronon.api._
import ai.chronon.online.Fetcher._
import ai.chronon.online.KVStore.GetRequest
import ai.chronon.online.Metrics.Environment
import com.google.gson.Gson
import org.apache.avro.generic.GenericRecord

import java.util.function.Consumer
import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer
import scala.collection.{Seq, mutable}
import scala.concurrent.Future
import scala.util.{Failure, Success, Try}

object Fetcher {
  case class Request(name: String,
                     keys: Map[String, AnyRef],
                     atMillis: Option[Long] = None,
                     context: Option[Metrics.Context] = None)

  case class PrefixedRequest(prefix: String, request: Request)
  case class StatsRequest(name: String, startTs: Option[Long] = None, endTs: Option[Long] = None)
  case class StatsResponse(request: StatsRequest, values: Try[Map[String, AnyRef]], millis: Long)
  case class MergedStatsResponse(request: StatsRequest, values: Try[Map[String, AnyRef]])
  case class SeriesStatsResponse(request: StatsRequest, values: Try[Map[String, AnyRef]])
  case class Response(request: Request, values: Try[Map[String, AnyRef]])
  case class ResponseWithContext(request: Request,
                                 derivedValues: Map[String, AnyRef],
                                 baseValues: Map[String, AnyRef]) {
    def combinedValues: Map[String, AnyRef] = baseValues ++ derivedValues
  }
  case class ColumnSpec(groupByName: String, columnName: String, prefix: Option[String], keyMapping: Option[Map[String, AnyRef]])
}

private[online] case class FetcherResponseWithTs(responses: scala.collection.Seq[Response], endTs: Long)

// BaseFetcher + Logging + External service calls
class Fetcher(val kvStore: KVStore,
              metaDataSet: String,
              timeoutMillis: Long = 10000,
              logFunc: Consumer[LoggableResponse] = null,
              debug: Boolean = false,
              val externalSourceRegistry: ExternalSourceRegistry = null)
    extends FetcherBase(kvStore, metaDataSet, timeoutMillis, debug) {

  def buildJoinCodec(joinConf: api.Join): JoinCodec = {
    val keyFields = new mutable.LinkedHashSet[StructField]
    val valueFields = new mutable.ListBuffer[StructField]
    // collect keyFields and valueFields from joinParts/GroupBys
    joinConf.joinPartOps.foreach { joinPart =>
      val servingInfoTry = getGroupByServingInfo(joinPart.groupBy.metaData.getName)
      servingInfoTry
        .map { servingInfo =>
          val keySchema = servingInfo.keyCodec.chrononSchema.asInstanceOf[StructType]
          joinPart.leftToRight
            .mapValues(right => keySchema.fields.find(_.name == right).get.fieldType)
            .foreach {
              case (name, dType) =>
                val keyField = StructField(name, dType)
                keyFields.add(keyField)
            }
          val baseValueSchema = if (joinPart.groupBy.aggregations == null) {
            servingInfo.selectedChrononSchema
          } else {
            servingInfo.outputChrononSchema
          }
          baseValueSchema.fields.foreach { sf =>
            valueFields.append(joinPart.constructJoinPartSchema(sf))
          }
        }
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
    val keyCodec = AvroCodec.of(AvroConversions.fromChrononSchema(keySchema).toString)
    val baseValueSchema = StructType(s"${joinName.sanitize}_value", valueFields.toArray)
    val baseValueCodec = AvroCodec.of(AvroConversions.fromChrononSchema(baseValueSchema).toString)
    val joinCodec = JoinCodec(joinConf, keySchema, baseValueSchema, keyCodec, baseValueCodec)
    logControlEvent(joinCodec)
    joinCodec
  }

  // key and value schemas
  lazy val getJoinCodecs = new TTLCache[String, Try[JoinCodec]](
    { joinName: String =>
      getJoinConf(joinName).map(_.join).map(buildJoinCodec)
    },
    { join: String => Metrics.Context(environment = "join.codec.fetch", join = join) })

  private[online] def withTs(responses: Future[scala.collection.Seq[Response]]): Future[FetcherResponseWithTs] = {
    responses.map { response =>
      FetcherResponseWithTs(response, System.currentTimeMillis())
    }
  }

  override def fetchJoin(requests: scala.collection.Seq[Request]): Future[scala.collection.Seq[Response]] = {
    val ts = System.currentTimeMillis()
    val internalResponsesF = super.fetchJoin(requests)
    val externalResponsesF = fetchExternal(requests)
    val combinedResponsesF = internalResponsesF.zip(externalResponsesF).map {
      case (internalResponses, externalResponses) =>
        internalResponses.zip(externalResponses).map {
          case (internalResponse, externalResponse) =>
            if (debug) {
              println(internalResponse.values.get.keys.toSeq)
              println(externalResponse.values.get.keys.toSeq)
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
            val joinCodec = getJoinCodecs(internalResponse.request.name).get
            ctx.histogram("derivation_codec.latency.millis", System.currentTimeMillis() - derivationStartTs)
            val baseMap = internalMap ++ externalMap
            val derivedMap: Map[String, AnyRef] = Try(
              joinCodec
                .deriveFunc(internalResponse.request.keys, baseMap)
                .mapValues(_.asInstanceOf[AnyRef])
                .toMap) match {
              case Success(derivedMap) => derivedMap
              case Failure(exception) => {
                ctx.incrementException(exception)
                throw exception
              }
            }
            val requestEndTs = System.currentTimeMillis()
            ctx.histogram("derivation.latency.millis", requestEndTs - derivationStartTs)
            ctx.histogram("overall.latency.millis", requestEndTs - ts)
            ResponseWithContext(internalResponse.request, derivedMap, baseMap)
        }
    }

    combinedResponsesF
      .map(_.iterator.map(logResponse(_, ts)).toSeq)
  }

  private def encode(schema: StructType,
                     codec: AvroCodec,
                     dataMap: Map[String, AnyRef],
                     cast: Boolean = false,
                     tries: Int = 3): Array[Byte] = {
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
      val avroRecord = AvroConversions.fromChrononRow(data, schema).asInstanceOf[GenericRecord]
      codec.encodeBinary(avroRecord)
    }

    def tryOnce(lastTry: Try[Array[Byte]], tries: Int): Try[Array[Byte]] = {
      if (tries == 0 || (lastTry != null && lastTry.isSuccess)) return lastTry
      val binary = encodeOnce(schema, codec, dataMap, cast)
      tryOnce(Try(codec.decodeRow(binary)).map(_ => binary), tries - 1)
    }

    tryOnce(null, tries).get
  }

  private def logResponse(resp: ResponseWithContext, ts: Long): Response = {
    val loggingStartTs = System.currentTimeMillis()
    val joinContext = resp.request.context
    val loggingTs = resp.request.atMillis.getOrElse(ts)
    val joinCodecTry = getJoinCodecs(resp.request.name)

    val loggingTry: Try[Unit] = joinCodecTry.map(codec => {
      val metaData = codec.conf.join.metaData
      val samplePercent = if (metaData.isSetSamplePercent) metaData.getSamplePercent else 0
      val keyBytes = encode(codec.keySchema, codec.keyCodec, resp.request.keys, cast = true)

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
          println(s"Logging ${resp.request.keys} : ${hash % 100000}: $samplePercent")
          val gson = new Gson()
          val valuesFormatted = values.map { case (k, v) => s"$k -> ${gson.toJson(v)}" }.mkString(", ")
          println(s"""Sampled join fetch
               |Key Map: ${resp.request.keys}
               |Value Map: [${valuesFormatted}]
               |""".stripMargin)
        }

        val valueBytes = encode(codec.valueSchema, codec.valueCodec, values)

        val loggableResponse = LoggableResponse(
          keyBytes,
          valueBytes,
          resp.request.name,
          loggingTs,
          codec.loggingSchemaHash
        )
        if (logFunc != null) {
          logFunc.accept(loggableResponse)
          joinContext.foreach(context => context.increment("logging_request.count"))
          joinContext.foreach(context =>
            context.histogram("logging_request.latency.millis", System.currentTimeMillis() - loggingStartTs))
          joinContext.foreach(context =>
            context.histogram("logging_request.overall.latency.millis", System.currentTimeMillis() - ts))

          if (debug) {
            println(s"Logged data with schema_hash ${codec.loggingSchemaHash}")
          }
        }
      }
    })
    loggingTry.failed.map { exception =>
      // to handle GroupByServingInfo staleness that results in encoding failure
      getJoinCodecs.refresh(resp.request.name)
      joinContext.foreach(_.incrementException(exception))
      println(s"logging failed due to ${exception.traceString}")
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
    context.histogram("response.external_pre_processing.latency", System.currentTimeMillis() - startTime)
    context.histogram("response.external_invalid_joins.count", invalidCount)
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
          .histogram("external.latency.millis", System.currentTimeMillis() - startTime)
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
        println(s"schema data logged successfully with schema_hash ${enc.loggingSchemaHash}")
      }
    }
  }

  /**
    * Fetch all stats IRs available between startTs and endTs.
    *
    * Stats are stored in a single dataname for all joins. For each join TimedValues are obtained and filtered as needed.
    */
  def fetchStats(joinRequest: StatsRequest): Future[Seq[StatsResponse]] = {
    val joinCodecs = getJoinCodecs(joinRequest.name).get
    val upperBound: Long = joinRequest.endTs.getOrElse(System.currentTimeMillis())
    kvStore
      .get(
        GetRequest(joinCodecs.statsKeyCodec.encodeArray(Array(joinRequest.name)),
                   Constants.StatsBatchDataset,
                   afterTsMillis = joinRequest.startTs)
      )
      .map(
        _.values.get.toArray
          .filter(_.millis <= upperBound)
          .map { tv =>
            StatsResponse(joinRequest, Try(joinCodecs.statsIrCodec.decodeMap(tv.bytes)), millis = tv.millis)
          }
          .toSeq)
  }

  /**
    * Main endpoint for fetching statistics over time available.
    */
  def fetchStatsTimeseries(joinRequest: StatsRequest): Future[SeriesStatsResponse] = {
    if (joinRequest.name.endsWith("__drift")) {
      // In the case of drift we only find the percentile keys and do a shifted distance.
      val rawResponses = fetchStats(
        StatsRequest(joinRequest.name.dropRight("__drift".length), joinRequest.startTs, joinRequest.endTs))
      return rawResponses.map { response =>
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
                  s"${key}_drift" -> Map(
                    "millis" -> curr.millis.asInstanceOf[AnyRef],
                    "value" -> StatsGenerator.lInfKllSketch(previousValue, currentValue)
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
    }
    val rawResponses = fetchStats(joinRequest)
    rawResponses.map { responseFuture =>
      val convertedValue = responseFuture
        .flatMap { response =>
          response.values.get.map {
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
    * For a time interval determine the aggregated stats between an startTs and endTs. Particularly useful for
    * determining data distributions between [startTs, endTs]
    */
  def fetchMergedStatsBetween(joinRequest: StatsRequest): Future[MergedStatsResponse] = {
    val joinCodecs = getJoinCodecs(joinRequest.name).get
    // Metrics are derived from the valueSchema of the join.
    val metrics = StatsGenerator.buildMetrics(joinCodecs.valueSchema.map(sf => (sf.name, sf.fieldType)))
    // Aggregator needs to aggregate partial IRs of stats. This should include transformations over the metrics.
    val aggregator = StatsGenerator.buildAggregator(metrics, joinCodecs.statsInputSchema)
    val rawResponses = fetchStats(joinRequest)
    rawResponses.map {
      var mergedIr: Array[Any] = Array.fill(aggregator.length)(null)
      responseFuture =>
        responseFuture.foreach { response =>
          val batchRecord = aggregator.denormalize(joinCodecs.statsIrSchema.map { field =>
            response.values.get(field.name).asInstanceOf[Any]
          }.toArray)
          mergedIr = aggregator.merge(mergedIr, batchRecord)
        }
        // Other things that would require custom processing: HeavyHitters
        val responseMap = (aggregator.outputSchema.map(_._1) zip aggregator.finalize(mergedIr)).map {
          case (statName, finalStat) =>
            if (statName.endsWith("percentile")) {
              statName ->
                StatsGenerator.finalizedPercentilesMerged.indices
                  .map(idx =>
                    Map("xvalue" -> StatsGenerator.finalizedPercentilesMerged(idx).asInstanceOf[AnyRef],
                        "value" -> finalStat.asInstanceOf[Array[Float]](idx).asInstanceOf[AnyRef]).asJava)
                  .toList
                  .asJava
            } else {
              statName -> finalStat.asInstanceOf[AnyRef]
            }
        }.toMap
        MergedStatsResponse(joinRequest, Try(responseMap))
    }
  }

  private case class ExternalToJoinRequest(externalRequest: Either[Request, KeyMissingException],
                                           joinRequest: Request,
                                           part: ExternalPart) {
    lazy val context: Metrics.Context =
      Metrics.Context(Environment.JoinFetching, join = joinRequest.name, groupBy = part.fullName)
  }
}
