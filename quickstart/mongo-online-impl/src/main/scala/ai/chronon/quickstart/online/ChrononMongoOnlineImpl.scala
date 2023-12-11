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

  @transient lazy val mongoClient = MongoClient(s"mongodb://${userConf("user")}:${userConf("password")}@${userConf("host")}:${userConf("port")}")
  override def streamDecoder(groupByServingInfoParsed: GroupByServingInfoParsed): StreamDecoder = ???

  override def genKvStore: KVStore = {
    val databaseName = "chronon"
    val dataset = "chronon_kvstore"
    new MongoKvStore(mongoClient, databaseName)
  }

  @transient lazy val loggingClient = mongoClient.getDatabase("chronon").getCollection("chronon_logging")
  override def logResponse(resp: LoggableResponse): Unit =
    loggingClient.insertOne(Document(
      "joinName" -> resp.joinName,
      "keyBytes" -> resp.keyBytes,
      "schemaHash" -> resp.schemaHash,
      "valueBytes" -> resp.valueBytes,
      "atMillis" -> resp.tsMillis,
      "ts" -> System.currentTimeMillis(),
    )).toFuture()

  override def externalRegistry: ExternalSourceRegistry = registry
}
