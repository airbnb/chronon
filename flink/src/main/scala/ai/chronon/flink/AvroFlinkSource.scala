package ai.chronon.flink

import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.spark.sql.Row
import org.apache.spark.sql.avro.AvroBytesToSparkRow


class AvroFlinkSource(source: DataStream[Array[Byte]],
                      avroSchemaJson: String,
                      avroOptions: Map[String, String]) extends FlinkSource[Row] {

  val (_, encoder) = AvroBytesToSparkRow.mapperAndEncoder(avroSchemaJson, avroOptions)


  override def getDataStream(topic: String, groupName: String)(
    env: StreamExecutionEnvironment,
    parallelism: Int): DataStream[Row] = {
    val mapper = new AvroBytesToSparkRowFunction(avroSchemaJson, avroOptions)
    source.map(mapper)
  }
}

class AvroBytesToSparkRowFunction(avroSchemaJson: String, avroOptions: Map[String, String])  extends RichMapFunction[Array[Byte], Row] {

  @transient private var mapper: Array[Byte] => Row = _
  override def open(configuration: Configuration): Unit = {
    val (_mapper, _) = AvroBytesToSparkRow.mapperAndEncoder(avroSchemaJson, avroOptions)
    mapper = _mapper
  }

  override def map(value: Array[Byte]): Row = {
    mapper(value)
  }
}