package ai.zipline.spark

import org.apache.spark._
import org.apache.spark.streaming._
import ai.zipline.api.Extensions._
import ai.zipline.api.{
  Accuracy,
  Constants,
  DataModel,
  GroupByServingInfo,
  OnlineImpl,
  ThriftJsonCodec,
  GroupBy => GroupByConf
}
import org.apache.spark.sql.Row

class GroupByStreaming {
  def main(args: Array[String]): Unit = {
    val parsedArgs = new StreamingArgs(args)
    parsedArgs.verify()
    println(s"Parsed Args: $parsedArgs")
    val groupByConf = parsedArgs.parseConf[GroupByConf]
    val onlineImpl: OnlineImpl = ClassLoader.load[OnlineImpl](parsedArgs.jar(), parsedArgs.onlineClass())

    val session = SparkSessionBuilder.build("Zipline Streaming")
    assert(groupByConf.streamingSource.isDefined,
           "No streaming source defined in GroupBy. Please set a topic/mutationTopic.")
    val topic = groupByConf.streamingSource.get.topic
    val props = parsedArgs.properties
    val df = session.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", props("kafka.bootstrap.servers"))
      // init deserializer
      .option("subscribe", topic)
      .load()
    val deserialized = df.rdd // key/value
      .map { row: Row =>
        val value = row(1).asInstanceOf[Array[Byte]]
      // Array[AnyRef] - spark Array[AnyRef]
      // -> apply sql
      // -> convert row to avro binary
      // -> put in kvStore
      }
  }

}

object ClassLoader {
  def load[T](jar: String, cls: String): T = ???
}
