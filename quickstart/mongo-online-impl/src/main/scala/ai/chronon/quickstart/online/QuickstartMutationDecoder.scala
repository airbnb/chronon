package ai.chronon.quickstart.online

import ai.chronon.api
import ai.chronon.api.Extensions.{GroupByOps, SourceOps}
import ai.chronon.api.{StructField, StructType}
import ai.chronon.online.{GroupByServingInfoParsed, Mutation, StreamDecoder}

/**
 *  We build a convention that for events (immutable) topic starts with 'event.' For mutable topics, it starts with 'mutation.'
 *  Similarly we accept that for events and mutations we follow the schema as data loader and the topic data is csv.
 *
 *  For mutations we require three additional columns, to be implemented later.
 */
class QuickstartMutationDecoder(groupByServingInfoParsed: GroupByServingInfoParsed) extends StreamDecoder {
  private val eventPrefix = "event."
  val groupByConf: api.GroupBy = groupByServingInfoParsed.groupBy
  private val source = {
    val opt = groupByConf.streamingSource
    assert(opt.isDefined, "A valid streaming source (with topic) can't be found")
    opt.get
  }
  val query: String = groupByConf.buildStreamingQuery
  private val tokens = {
    val set = new java.util.HashSet[String]
    query.split("[^A-Za-z0-9_]").foreach(set.add)
    set
  }

  val eventDecoder: EventDecoder = {
    val fields = source.topicTokens("fields")
    if (source.topic.startsWith(eventPrefix)) {
      new StreamingEventDecoder(schema, tokens, fields)
    } else {
      new CDCDecoder(schema, tokens, fields)
    }
  }

  override def decode(bytes: Array[Byte]): Mutation = eventDecoder.decode(bytes).orNull

  override def schema: StructType = eventDecoder.schema

}

trait EventDecoder extends Serializable {
  def schema: StructType
  def decode(bytes: Array[Byte]): Option[Mutation]
}

class CDCDecoder(schema: StructType, tokens: util.HashSet[String], fields: String) extends EventDecoder {
  override def schema: StructType = schema
  override def decode(bytes: Array[Byte]): Option[Mutation] = ???
}
