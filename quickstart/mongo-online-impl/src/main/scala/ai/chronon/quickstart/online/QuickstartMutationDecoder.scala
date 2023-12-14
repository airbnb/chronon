package ai.chronon.quickstart.online

import ai.chronon.api
import ai.chronon.api.Extensions.{GroupByOps, SourceOps}
import ai.chronon.api.{StructField, StructType}
import ai.chronon.online.{GroupByServingInfoParsed, Mutation, StreamDecoder}
import java.util.HashSet

/**
 *  We build a convention that for events (immutable) topic starts with 'event.' For mutable topics, it starts with 'mutation.'
 *  Similarly we accept that for events and mutations we follow the schema as data loader and the topic data is csv.
 *
 *  For mutations we require three additional columns, to be implemented later.
 */
class QuickstartMutationDecoder(groupByServingInfoParsed: GroupByServingInfoParsed) extends StreamDecoder {
  private val eventPrefix = "events."
  val groupByConf: api.GroupBy = groupByServingInfoParsed.groupBy
  private val source = {
    val opt = groupByConf.streamingSource
    assert(opt.isDefined, "A valid streaming source (with topic) can't be found")
    opt.get
  }

  val eventDecoder: EventDecoder = {
    val fields = source.topicTokens("fields").split(",")
    if (source.topic.startsWith(eventPrefix)) {
      new StreamingEventDecoder(fields)
    } else {
      new CDCDecoder(fields)
    }
  }

  override def decode(bytes: Array[Byte]): Mutation = eventDecoder.decode(bytes).orNull

  override def schema: StructType = eventDecoder.schema

}

trait EventDecoder extends Serializable {
  def schema: StructType
  def decode(bytes: Array[Byte]): Option[Mutation]
}

class StreamingEventDecoder(fields: Array[String]) extends EventDecoder {
  override def schema: StructType = StructType("event",
    fields.map { columnName =>
      val dataType = columnName match {
        case name if name.endsWith("ts") => api.LongType
        case name if name.endsWith("_price") || name.endsWith("_amt") => api.LongType
        case _ => api.StringType
      }
      StructField(columnName, dataType)
    }
  )

  /**
   * Receive a csv string and convert it to a mutation.
   */
  override def decode(bytes: Array[Byte]): Option[Mutation] = {
    val csvRow = new String(bytes)
    val values: Array[Any] = csvRow.split(",").zip(schema).map {
      case (value, field) =>
        // Convert the string value to the appropriate data type based on the schema
        if (value == null || value.isEmpty || value == "") null
        else field.fieldType match {
          case api.LongType => value.toLong
          case _ => value
        }
    }
    Some(Mutation(schema, null, values))
  }
}

class CDCDecoder(fields: Array[String]) extends EventDecoder {

  val mutationColumns = Array("__mutationTs", "__mutationType")
  override def schema: StructType = StructType("mutation",
    (fields ++ mutationColumns).map { columnName =>
      val dataType = columnName match {
        case name if name.endsWith("ts") => api.LongType
        case name if name.endsWith("_price") || name.endsWith("_amt") => api.LongType
        case _ => api.StringType
      }
      StructField(columnName, dataType)
    }
  )
  override def decode(bytes: Array[Byte]): Option[Mutation] = ???
}
