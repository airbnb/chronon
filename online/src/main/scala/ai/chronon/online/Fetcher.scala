package ai.chronon.online

import ai.chronon.aggregator.row.{ColumnAggregator, StatsGenerator}
import ai.chronon.api.Constants.UTF8
import ai.chronon.api.Extensions.{ExternalPartOps, JoinOps, MetadataOps, StringOps, ThrowableOps}
import ai.chronon.api._
import ai.chronon.online.Extensions.StructTypeOps
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
import scala.util.ScalaJavaConversions.{ListOps, MapOps}
import scala.util.matching.Regex
import scala.util.{Failure, ScalaVersionSpecificCollectionsConverter, Success, Try}

object Fetcher {
  case class Request(name: String,
                     keys: Map[String, AnyRef],
                     atMillis: Option[Long] = None,
                     context: Option[Metrics.Context] = None)

  case class Response(request: Request, values: Try[Map[String, AnyRef]])
  case class ResponseWithContext(request: Request, derivedValues: Map[String, AnyRef], baseValues: Map[String, AnyRef]) {
    def combinedValues: Map[String, AnyRef] = baseValues ++ derivedValues
  }
}



// BaseFetcher + Logging + External service calls
class Fetcher(val kvStore: KVStore,
              metaDataSet: String,
              timeoutMillis: Long = 10000,
              logFunc: Consumer[LoggableResponse] = null,
              debug: Boolean = false,
              val externalSourceRegistry: ExternalSourceRegistry = null)
    extends BaseFetcher(kvStore, metaDataSet, timeoutMillis, debug) {

  // key and value schemas
  lazy val getJoinCodecs = new TTLCache[String, Try[JoinCodec]]({ joinName: String =>
    val joinConfTry = getJoinConf(joinName)
    val keyFields = new mutable.LinkedHashSet[StructField]
    val valueFields = new mutable.ListBuffer[StructField]
    joinConfTry.map { joinConf =>
      // collect schema from
      joinConf.joinPartOps.foreach {
        joinPart =>
          val servingInfoTry = getGroupByServingInfo(joinPart.groupBy.metaData.getName)
          servingInfoTry
            .map {
              servingInfo =>
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
      Option(joinConf.join.onlineExternalParts).foreach {
        externals =>
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

      val keySchema = StructType(s"${joinName}_key", keyFields.toArray)
      val keyCodec = AvroCodec.of(AvroConversions.fromChrononSchema(keySchema).toString)
      val baseValueSchema = StructType(s"${joinName}_value", valueFields.toArray)
      val baseValueCodec = AvroCodec.of(AvroConversions.fromChrononSchema(baseValueSchema).toString)
      val joinCodec = JoinCodec(joinConf, keySchema, baseValueSchema, keyCodec, baseValueCodec)
      logControlEvent(joinCodec)
      joinCodec
    }
  },
    {join: String => Metrics.Context(environment = "join.codec.fetch", join = join)}
  )

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
            val joinName = internalResponse.request.name
            Metrics.Context(Environment.JoinFetching, join = joinName).histogram("overall.latency.millis", System.currentTimeMillis() - ts)
            val joinCodec = getJoinCodecs(internalResponse.request.name).get
            val baseMap = internalMap ++ externalMap
            val derivedMap: Map[String, AnyRef] = joinCodec.deriveFunc(internalResponse.request.keys, baseMap).mapValues(_.asInstanceOf[AnyRef]).toMap
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
      val joinConfTry = getJoinConf(joinName)
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
        Metrics.Context(Environment.JoinFetching, join = req.name).histogram("external.latency.millis", System.currentTimeMillis() - startTime)
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
  def fetchStats(joinName: String, startTs: Option[Long], endTs: Option[Long]): Future[Seq[Response]] = {
    val joinCodecs = getJoinCodecs(joinName) match {
      case Success(joinCodec) => joinCodec
      case Failure(exception) => throw exception
    }
    val upperBound: Long = endTs match {
      case Some(value) => value
      case _ => System.currentTimeMillis()
    }
    kvStore.get(GetRequest(joinCodecs.statsKeyCodec.encode(Map("joinPath" -> joinName)), Constants.StatsBatchDataset, afterTsMillis = startTs)).map {
      kvResponse =>
        kvResponse.values match {
          case Success(responses) =>
            responses.toArray.filter(_.millis <= upperBound).map {
              tv =>
                Response(Request(joinName, null, endTs), Try(joinCodecs.statsIrCodec.decodeMap(tv.bytes)))
            }.toSeq
          case Failure(exception) => throw exception
        }
    }
  }

  /**
    * For a time interval determine the aggregated stats between an startTs and endTs. Particularly useful for
    * determining data distributions between [startTs, endTs]
    */
  def fetchMergedStatsBetween(joinName: String, startTs: Option[Long], endTs: Option[Long]): Future[Response] = {
    val joinCodecs = getJoinCodecs(joinName) match {
      case Success(joinCodec) => joinCodec
      case Failure(exception) => throw exception
    }
    val valueSchema = joinCodecs.valueSchema
    val statsInputSchema = joinCodecs.statsInputSchema
    val statsIrSchema = joinCodecs.statsIrSchema
    // Metrics are derived from the valueSchema of the join.
    val metrics = StatsGenerator.buildMetrics(valueSchema.map(sf => (sf.name, sf.fieldType)))
    // Aggregator needs to aggregate partial IRs of stats. This should include transformations over the metrics.
    val aggregator = StatsGenerator.buildAggregator(metrics, statsInputSchema)
    val rawResponses = fetchStats(joinName, startTs, endTs)
    rawResponses.map {
      var mergedIr: Array[Any] = Array.fill(aggregator.length)(null)
      responseFuture => responseFuture.foreach {
        response =>
          response.values match {
            case Success(valueMap) =>
              val batchRecord = aggregator.denormalize(statsIrSchema.map { field => valueMap(field.name).asInstanceOf[Any]}.toArray)
              mergedIr = aggregator.merge(mergedIr, batchRecord)
            case Failure(exception) => throw exception
          }
      }
      val responseMap = (aggregator.outputSchema.map(_._1) zip aggregator.finalize(mergedIr).map(_.asInstanceOf[AnyRef])).toMap
      Response(Request(joinName, null), Try(responseMap))
    }
  }

  private case class ExternalToJoinRequest(externalRequest: Either[Request, KeyMissingException],
                                           joinRequest: Request,
                                           part: ExternalPart) {
    lazy val context: Metrics.Context =
      Metrics.Context(Environment.JoinFetching, join = joinRequest.name, groupBy = part.fullName)
  }
}
