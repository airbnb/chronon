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
import org.slf4j.{Logger, LoggerFactory}

class ChrononMongoOnlineImpl(userConf: Map[String, String]) extends Api(userConf) {

  @transient lazy val registry: ExternalSourceRegistry = new ExternalSourceRegistry()

  @transient val logger: Logger = LoggerFactory.getLogger("ChrononMongoOnlineImpl")

  @transient lazy val mongoClient = MongoClient(s"mongodb://${userConf("user")}:${userConf("password")}@${userConf("host")}:${userConf("port")}")
  override def streamDecoder(groupByServingInfoParsed: GroupByServingInfoParsed): StreamDecoder =
    new QuickstartMutationDecoder(groupByServingInfoParsed)


  override def genKvStore: KVStore = new MongoKvStore(mongoClient, Constants.mongoDatabase)


  @transient lazy val loggingClient = mongoClient.getDatabase(Constants.mongoDatabase).getCollection(Constants.mongoLoggingCollection)
  override def logResponse(resp: LoggableResponse): Unit =
    loggingClient.insertOne(Document(
      "joinName" -> resp.joinName,
      "keyBytes" -> resp.keyBytes,
      "schemaHash" -> Option(resp.schemaHash).getOrElse("SCHEMA_PUBLISHED"),
      "valueBytes" -> resp.valueBytes,
      "atMillis" -> resp.tsMillis,
      "ts" -> System.currentTimeMillis(),
    )).toFuture()

  override def externalRegistry: ExternalSourceRegistry = registry
}
