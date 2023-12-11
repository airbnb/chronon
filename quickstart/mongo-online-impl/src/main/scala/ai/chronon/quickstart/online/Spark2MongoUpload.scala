package ai.chronon.quickstart.online
import org.rogach.scallop._

object Spark2MongoUpload {
  class Conf(arguments: Seq[String]) extends ScallopConf(arguments) {
    val offlineTable = trailArg[String]("offlineTable", "Offline table name")
    val destination = trailArg[String]("destination", "Destination name")
    verify()
  }

  def main(args: Array[String]): Unit = {
    val conf = new Conf(args)
    val offlineTable = conf.offlineTable()
    val destination = conf.destination()
    val chrononMongoOnlineImpl = ChrononMongoOnlineImpl.buildDefault()
    val mongoKvStore = chrononMongoOnlineImpl.genKvStore
    mongoKvStore.bulkPut(offlineTable, destination, "")
  }
}