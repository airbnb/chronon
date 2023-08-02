package ai.chronon.spark.streaming

import ai.chronon.api
import ai.chronon.api.ThriftJsonCodec
import ai.chronon.online.StreamDecoder
import org.apache.spark.sql.{DataFrame, SparkSession}

class JoinSource(joinSource: api.JoinSource) {
//  def getDataStream(streamBuilder: DataStreamBuilder, streamDecoder: StreamDecoder, conf: Map[String, String])(implicit
//      session: SparkSession): DataStream = {
//    val left = joinSource.getJoin.getLeft
//    // for left entities, we won't be able to look back at the prior value online to join
//    assert(
//      !left.isSetEvents && left.getEvents.isSetTopic,
//      s"Need a JoinSource whose left side is events with the topic set\n${ThriftJsonCodec.toJsonStr(joinSource)}"
//    )
//    // get base stream
//    val stream = if (left.isSetEvents) {
//      val events = left.getEvents
//      val stream = streamBuilder.from(events.getTopic, conf)
//      stream.apply(events.query)
//    } else if (left.isSetJoinSource) { // recursively walk underlying joins
//      val joinSource = left.getJoinSource
//      val stream = new JoinSource(joinSource).getDataStream(streamBuilder, streamDecoder, conf)
//      stream.apply(joinSource.query)
//    }
//
//    // get
//    null
//  }
//  val sourceIsJoin = streamingSource.isSetJoin
  // func to take rows of left and create rows of output by batch fetching
  // we build all necessary information prior to function creation for performance
//  @transient lazy val enrichFunc: Seq[Row] => Seq[Row] = {
//    assert(sourceIsJoin, s"Cannot call enrich function on a non-join source: $streamingSource")
//    val tsIndex = inputStream.schema.fieldIndex(Constants.TimeColumn)
//    val join = streamingSource.getJoin
//    val joinKeys = streamingSource.getJoin.leftKeyCols
//    val joinName = join.metaData.cleanName
//    val valueSchema = onlineImpl.buildFetcher().getJoinCodecs(joinName).get.valueSchema
//    val valueFields = valueSchema.fields.map(_.name)
//    println(s"Enriching join: $joinName with keys: $joinKeys and value fields $valueFields")
//    val func = { rows: Seq[Row] =>
//      val requests = rows.map { row =>
//        val keyMap = row.getValuesMap(joinKeys)
//        Request(joinName, keyMap, Some(row.getLong(tsIndex)))
//      }
//      val responses = Await.result(fetcher.fetchJoin(requests), Duration(fetchTimeoutMs, MILLISECONDS))
//      rows.zip(responses).map { case (row, response) =>
//        val enriched = row.toSeq ++ (response.values match {
//          case Success(responseMap) => valueFields.map(col => responseMap.getOrElse(col, null))
//          case Failure(exception) =>
//            exception.printStackTrace(System.err)
//            valueFields.map(_ => null)
//        })
//        Row.fromSeq(enriched)
//      }
//    }
//    func
//  }

  def getBatchDataframe: DataFrame = ???
  def getBatchEventSouce: api.EventSource = ???
}
