package ai.zipline.spark
import ai.zipline.api.ThriftJsonCodec
import org.rogach.scallop._

class BatchArgs(args: Seq[String]) extends ScallopConf(args) {
  val confPath: ScallopOption[String] = opt[String](required = true)
  val endDate: ScallopOption[String] = opt[String](required = true)
  val stepDays: ScallopOption[Int] = opt[Int](required = false) // doesn't apply to uploads
  verify()
  def parseConf[T]: T =
    ThriftJsonCodec.fromJsonFile[T](confPath(), check = true, clazz = classOf[T])
}
