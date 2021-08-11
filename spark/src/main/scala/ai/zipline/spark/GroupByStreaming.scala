package ai.zipline.spark


import scala.collection.JavaConverters._

import ai.zipline.api.Extensions._
import ai.zipline.api
import ai.zipline.api.QueryUtils.buildStreamingQuery
import ai.zipline.api.{Constants, QueryUtils}
import org.apache.spark.sql.{Dataset, Encoders, Row, SparkSession}
import ai.zipline.fetcher.{AvroUtils, Fetcher}
import org.apache.avro.Schema
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.DataFrame

class GroupByStreaming(
    inputStream: DataFrame,
    session: SparkSession,
    groupByConf: api.GroupBy,
    onlineImpl: api.OnlineImpl,
    additionalFilterClauses: Seq[String] = Seq.empty,
    debug: Boolean = false) extends Serializable {

  def run(): Unit = {
    val kvStore = onlineImpl.genKvStore
    val fetcher = new Fetcher(kvStore)
    val groupByServingInfo = fetcher.getGroupByServingInfo(groupByConf.getMetaData.getName)
    val inputZiplineSchema = onlineImpl.batchInputAvroSchemaToStreaming(
      groupByServingInfo.inputZiplineSchema)
    val decoder = onlineImpl.genStreamDecoder(inputZiplineSchema)
    assert(groupByConf.streamingSource.isDefined,
           "No streaming source defined in GroupBy. Please set a topic/mutationTopic.")
    val streamingSource = groupByConf.streamingSource.get
    val streamingQuery = buildStreamingQuery(groupByConf, additionalFilterClauses)

    import session.implicits._
    implicit val structTypeEncoder = Encoders.kryo[api.Mutation]

    val deserialized: Dataset[api.Mutation] = inputStream
      .as[Array[Byte]]
      .map {
        record =>
          decoder.decode(record)
      }
      .filter( mutation => mutation.before != mutation.after)

    println(
      s"""
        | group by serving info: $groupByServingInfo
        | Streaming source: $streamingSource
        | Additional filter clauses: ${additionalFilterClauses.mkString(", ")}
        | streaming Query: $streamingQuery
        | streaming dataset: ${groupByConf.streamingDataset}
        | input zipline schema: $inputZiplineSchema
        |""".stripMargin)

    val des = deserialized
      .map {
        mutation =>
          Row.fromSeq(mutation.after)
      }(RowEncoder(Conversions.fromZiplineSchema(inputZiplineSchema)))
    des.createOrReplaceTempView(Constants.StreamingInputTable)
    val selectedDf = session.sql(streamingQuery)

    val fields = groupByServingInfo.selectedZiplineSchema.fields.map(_.name)
    val keys = groupByConf.keyColumns.asScala.toArray
    val keyIndices = keys.map(selectedDf.schema.fieldIndex)
    val valueIndices = fields.map(selectedDf.schema.fieldIndex)
    val tsIndex = selectedDf.schema.fieldIndex(Constants.TimeColumn)
    val streamingDataset = groupByConf.streamingDataset

    selectedDf
      .map {
        row =>
          val keys = keyIndices.map(row.get)
          val keyBytes = groupByServingInfo.keyCodec.encodeArray(keys)
          val values = valueIndices.map(row.get)
          val valueBytes = groupByServingInfo.selectedCodec.encodeArray(values)
          val ts = row.get(tsIndex).asInstanceOf[Long]
          api.KVStore.PutRequest(keyBytes, valueBytes, streamingDataset, Option(ts))
      }
      .writeStream
      .outputMode("append")
      .foreach(new StreamingDataWriter(onlineImpl, groupByServingInfo, debug))
      .start()
  }
}
