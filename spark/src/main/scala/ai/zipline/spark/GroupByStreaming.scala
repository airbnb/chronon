package ai.zipline.spark


import scala.collection.JavaConverters._

import ai.zipline.api
import ai.zipline.api.{Constants, Mutation, QueryUtils}
import org.apache.spark.sql.{DataFrame, Dataset, Encoder, Encoders, Row, SparkSession}
import ai.zipline.fetcher.Fetcher
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import ai.zipline.api.Extensions.{GroupByOps, SourceOps}
import ai.zipline.lib.Metrics


class GroupByStreaming(
    inputStream: DataFrame,
    session: SparkSession,
    groupByConf: api.GroupBy,
    onlineImpl: api.OnlineImpl,
    additionalFilterClauses: Seq[String] = Seq.empty,
    debug: Boolean = false,
    mockWrites: Boolean = false) extends Serializable {

  private def buildStreamingQuery(): String = {
    val streamingSource = groupByConf.streamingSource.get
    val query = streamingSource.query
    val selects = Option(query.selects).map(_.asScala.toMap).orNull
    val timeColumn = Option(query.timeColumn).getOrElse(Constants.TimeColumn)
    val fillIfAbsent = if (selects == null) null else Map(Constants.TimeColumn -> timeColumn)
    val keys = groupByConf.getKeyColumns.asScala

    val baseWheres = Option(query.wheres).map(_.asScala).getOrElse(Seq.empty[String])
    val keyWhereOption = Option(selects).map(
      selectsMap => keys.map(key => s"(${selectsMap(key)} is NOT NULL)").mkString(" OR "))
    val timeWheres = Seq(s"$timeColumn is NOT NULL")
   
    QueryUtils.build(
      selects,
      Constants.StreamingInputTable,
      baseWheres ++ additionalFilterClauses ++ timeWheres ++ keyWhereOption,
      fillIfAbsent = fillIfAbsent
    )
  }

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
    val streamingQuery = buildStreamingQuery()

    val context = Metrics.Context(groupBy = groupByConf.getMetaData.getName)

    import session.implicits._
    implicit val structTypeEncoder: Encoder[Mutation] = Encoders.kryo[api.Mutation]

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
    assert(selectedDf.schema.fieldNames.contains(Constants.TimeColumn),
      s"time column ${Constants.TimeColumn} must be included in the selects")
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
      .foreach(new StreamingDataWriter(onlineImpl, groupByServingInfo, context, debug, mockWrites))
      .start()
  }
}
