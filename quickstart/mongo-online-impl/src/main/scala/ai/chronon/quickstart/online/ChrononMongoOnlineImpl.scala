package ai.chronon.quickstart.online

import ai.chronon.online.{
  Api,
  ExternalSourceRegistry,
  GroupByServingInfoParsed,
  KVStore,
  LoggableResponse,
  StreamDecoder
}

import org.mongodb.scala._
import org.mongodb.scala.model.Filters._
import org.mongodb.scala.model.Sorts._


import jnr.ffi.annotations.Transient
import org.slf4j.{Logger, LoggerFactory}

object ChrononMongoOnlineImpl {

  private val MONGO_HOST = "mongodb"
  private val MONGO_PORT = 27015.toString

  private val DEFAULT_USER = "admin"
  private val DEFAULT_PASS = "admin"

  def buildDefault(): ChrononMongoOnlineImpl = {
    val userConf = Map(
      "host" -> MONGO_HOST,
      "port" -> MONGO_PORT,
      "user" -> DEFAULT_USER,
      "password" -> DEFAULT_PASS,
    )
    new ChrononMongoOnlineImpl(userConf)
  }
}

class ChrononMongoOnlineImpl(userConf: Map[String, String]) extends Api(userConf) {

  @transient lazy val registry: ExternalSourceRegistry = new ExternalSourceRegistry()

  @Transient val logger: Logger = LoggerFactory.getLogger("ChrononMongoOnlineImpl")

  def buildMongoApiClientConfig() = ???

  override def streamDecoder(groupByServingInfoParsed: GroupByServingInfoParsed): StreamDecoder = ???

  override def genKvStore: KVStore = ???

    /*
    val musselHostOpt = userConf.get(MUSSEL_HOST)
    assert(musselHostOpt.isDefined, s"$MUSSEL_HOST is not defined")
    val musselPortOpt = userConf.get(MUSSEL_PORT)
    assert(musselPortOpt.isDefined, s"$MUSSEL_PORT is not defined")
    assert(musselPortOpt.get.forall(Character.isDigit), s"$MUSSEL_PORT is not a number")
    val debug = userConf.getOrElse("debug", "false").toBoolean
    val maxThreadPoolSize = userConf.getOrElse("max-thread-pool-size", "10").toInt

    val musselConfig = new MusselClientConfiguration()
    val apiClientConfig = new ApiClientConfig()
    apiClientConfig.setClientName(MusselClientName)
    apiClientConfig.setSynapseName(musselHostOpt.get)
    apiClientConfig.setSynapsePort(musselPortOpt.get.toInt)
    apiClientConfig.setRequestTimeoutMs(MusselRequestTimeoutMs)
    apiClientConfig.setTotalAttempt(MusselTotalAttempt)
    apiClientConfig.setMaxResponseBytes(MAX_RESPONSE_BYTES)
    apiClientConfig.setMaxThreadPoolSize(maxThreadPoolSize)
    musselConfig.setApiClientConfig(apiClientConfig)
    MusselZiplineKvStore(MusselClientName, musselConfig, debug)
    */

  override def logResponse(resp: LoggableResponse): Unit = ???
  /*{
    // construct zipline prediction event
    val ziplinePredictionEvent = new ZiplinePredictionEvent()
    ziplinePredictionEvent.setName(resp.joinName)
    ziplinePredictionEvent.setKeyBase64(Base64.getEncoder.encodeToString(resp.keyBytes))
    ziplinePredictionEvent.setValueBase64(Base64.getEncoder.encodeToString(resp.valueBytes))
    ziplinePredictionEvent.setTsMillis(resp.tsMillis)
    ziplinePredictionEvent.setSchemaHash(resp.schemaHash)
    // Publish the event
    try {
      omnesProducer.publishAsync(ziplinePredictionEvent)
    } catch {
      case e: Exception =>
        logger.error(
          s"Error publishing zipline prediction event to jitney: $e,  cause = ${e.getCause}, message = ${e.getMessage} stack trace = ${e.getStackTrace
            .mkString("Array(", ", ", ")")}"
        )
    }
  }*/

  override def externalRegistry: ExternalSourceRegistry = registry
}
