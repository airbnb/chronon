package ai.zipline.spark.streaming

import ai.zipline.api
import ai.zipline.api.Extensions.{GroupByOps, SourceOps}
import ai.zipline.api.{Constants, GroupByServingInfo, QueryUtils}
import ai.zipline.online.{
  Api,
  AvroCodec,
  AvroUtils,
  Fetcher,
  GroupByServingInfoParsed,
  KVStore,
  Mutation,
  Metrics => FetcherMetrics
}
import ai.zipline.spark.{Conversions, KvRdd}
import com.google.gson.Gson
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.streaming.DataStreamWriter

import java.time.format.DateTimeFormatter
import java.time.{Instant, ZoneId, ZoneOffset}
import java.util.Base64
import scala.collection.JavaConverters._

class GroupBy(inputStream: DataFrame,
              session: SparkSession,
              groupByConf: api.GroupBy,
              onlineImpl: Api,
              debug: Boolean = false)
    extends Serializable {

  private def buildStreamingQuery(): String = {
    val streamingSource = groupByConf.streamingSource.get
    val query = streamingSource.query
    val selects = Option(query.selects).map(_.asScala.toMap).orNull
    val timeColumn = Option(query.timeColumn).getOrElse(Constants.TimeColumn)
    val fillIfAbsent = groupByConf.dataModel match {
      case api.DataModel.Entities =>
        Map(Constants.TimeColumn -> timeColumn, Constants.ReversalColumn -> null, Constants.MutationTimeColumn -> null)
      case api.DataModel.Events => Map(Constants.TimeColumn -> timeColumn)
    }
    val keys = groupByConf.getKeyColumns.asScala

    val baseWheres = Option(query.wheres).map(_.asScala).getOrElse(Seq.empty[String])
    val selectMap = Option(selects).getOrElse(Map.empty[String, String])
    val keyWhereOption = Seq(
      keys
        .map { key =>
          s"${selectMap.getOrElse(key, key)} IS NOT NULL"
        }
        .mkString(" OR "))
    val timeWheres = groupByConf.dataModel match {
      case api.DataModel.Entities => Seq(s"${Constants.MutationTimeColumn} is NOT NULL")
      case api.DataModel.Events   => Seq(s"$timeColumn is NOT NULL")
    }
    QueryUtils.build(
      selects,
      Constants.StreamingInputTable,
      baseWheres ++ timeWheres ++ keyWhereOption,
      fillIfAbsent = if (selects == null) null else fillIfAbsent
    )
  }

  def run(local: Boolean = false): Unit = {
    buildDataStream(local).start()
  }

  def buildDataStream(local: Boolean = false): DataStreamWriter[KVStore.PutRequest] = {
    val kvStore = onlineImpl.genKvStore
    val fetcher = new Fetcher(kvStore)
    val groupByServingInfo = if (local) {
      // don't talk to kv store - instead make a dummy ServingInfo
      val gb = new GroupByServingInfo()
      gb.setGroupBy(groupByConf)
      new GroupByServingInfoParsed(gb)
    } else {
      fetcher.getGroupByServingInfo(groupByConf.getMetaData.getName).get
    }

    val streamDecoder = onlineImpl.streamDecoder(groupByServingInfo)
    assert(groupByConf.streamingSource.isDefined,
           "No streaming source defined in GroupBy. Please set a topic/mutationTopic.")
    val streamingSource = groupByConf.streamingSource.get
    val streamingQuery = buildStreamingQuery()

    val context = FetcherMetrics.Context(groupBy = groupByConf.getMetaData.getName)

    import session.implicits._
    implicit val structTypeEncoder: Encoder[Mutation] = Encoders.kryo[Mutation]
    val deserialized: Dataset[Mutation] = inputStream
      .as[Array[Byte]]
      .map { streamDecoder.decode }
      .filter(mutation => mutation.before != mutation.after)

    val streamSchema = Conversions.fromZiplineSchema(streamDecoder.schema)
    println(s"""
        | group by serving info: $groupByServingInfo
        | Streaming source: ${streamingSource}
        | streaming Query: $streamingQuery
        | streaming dataset: ${groupByConf.streamingDataset}
        | stream schema: ${streamSchema}
        |""".stripMargin)

    val des = deserialized
      .flatMap { mutation =>
        Seq(mutation.after, mutation.before)
          .filter(_ != null)
          .map(KvRdd.toSparkRow(_, streamDecoder.schema).asInstanceOf[Row])
      }(RowEncoder(streamSchema))
    des.createOrReplaceTempView(Constants.StreamingInputTable)
    val selectedDf = session.sql(streamingQuery)
    assert(selectedDf.schema.fieldNames.contains(Constants.TimeColumn),
           s"time column ${Constants.TimeColumn} must be included in the selects")
    if (groupByConf.dataModel == api.DataModel.Entities) {
      assert(selectedDf.schema.fieldNames.contains(Constants.MutationTimeColumn), "Required Mutation ts")
    }
    val keys = groupByConf.keyColumns.asScala.toArray
    val keyIndices = keys.map(selectedDf.schema.fieldIndex)
    val (additionalColumns, eventTimeColumn) = groupByConf.dataModel match {
      case api.DataModel.Entities => groupByServingInfo.MutationAvroColumns -> Constants.MutationTimeColumn
      case api.DataModel.Events   => Seq.empty[String] -> Constants.TimeColumn
    }
    val valueColumns = groupByConf.aggregationInputs ++ additionalColumns
    val valueIndices = valueColumns.map(selectedDf.schema.fieldIndex)
    val tsIndex = selectedDf.schema.fieldIndex(eventTimeColumn)
    val streamingDataset = groupByConf.streamingDataset

    def schema(indices: Seq[Int], name: String): AvroCodec = {
      val fields = indices
        .map(Conversions.toZiplineSchema(selectedDf.schema))
        .map { case (f, d) => api.StructField(f, d) }
        .toArray
      AvroCodec.of(AvroUtils.fromZiplineSchema(api.StructType(name, fields)).toString())
    }
    val keyCodec = schema(keyIndices, "key")
    val valueCodec = schema(valueIndices, "selected")
    println("Ready to stream")
    selectedDf
      .map { row =>
        val keys = keyIndices.map(row.get)
        val values = valueIndices.map(row.get)

        val ts = row.get(tsIndex).asInstanceOf[Long]
        val keyBytes = keyCodec.encodeArray(keys)
        val valueBytes = valueCodec.encodeArray(values)
        if (debug) {
          val gson = new Gson()
          val formatter = DateTimeFormatter.ISO_LOCAL_DATE_TIME.withZone(ZoneId.from(ZoneOffset.UTC))
          val pstFormatter = DateTimeFormatter.ISO_LOCAL_DATE_TIME.withZone(ZoneId.of("America/Los_Angeles"))
          println(s"""
               |keys: ${gson.toJson(keys)}
               |values: ${gson.toJson(values)}
               |keyBytes: ${Base64.getEncoder.encodeToString(keyBytes)}
               |valueBytes: ${Base64.getEncoder.encodeToString(valueBytes)}
               |ts: $ts  |  UTC: ${formatter.format(Instant.ofEpochMilli(ts))} | PST: ${pstFormatter.format(
            Instant.ofEpochMilli(ts))}
               |""".stripMargin)
        }
        KVStore.PutRequest(keyBytes, valueBytes, streamingDataset, Option(ts))
      }
      .writeStream
      .outputMode("append")
      .foreach(new DataWriter(onlineImpl, context, debug))
  }
}
