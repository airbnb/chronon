package ai.chronon.flink

import ai.chronon.api.Extensions.GroupByOps
import ai.chronon.api.{Constants, DataModel, GroupBy, GroupByServingInfo, Query, StructType => ChrononStructType}
import ai.chronon.online.Extensions.StructTypeOps
import ai.chronon.online.{AvroConversions, GroupByServingInfoParsed}
import ai.chronon.online.KVStore.PutRequest
import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.configuration.Configuration
import org.apache.flink.metrics.Counter
import org.apache.flink.util.Collector
import org.apache.spark.sql.types.StructType

import scala.Console.println
import scala.jdk.CollectionConverters._

/**
 * A Flink function that is responsible for converting the Spark expr eval output and converting that to a form
 * that can be written out to the KV store (PutRequest object)
 * @param groupBy The GroupBy we are working with
 * @param outputSchema The output schema for the data
 * @tparam T The input data type
 */
case class AvroCodecFn[T](groupBy: GroupBy, inputSchema: StructType, outputSchema: StructType)
  extends RichFlatMapFunction[Map[String, Any], PutRequest] {

  @transient protected var avroConversionErrorCounter: Counter = _

  protected val query: Query = groupBy.streamingSource.get.getEvents.query
  protected val streamingDataset: String = groupBy.streamingDataset

  // TODO: update to use constant names that are company specific
  protected val timeColumnAlias: String = Constants.TimeColumn
  protected val timeColumn: String = Option(query.timeColumn).getOrElse(timeColumnAlias)

  // we currently choose to construct the GroupByServingInfoParsed object here as it allows us
  // to deploy the Flink apps prior to having the GroupByUpload jobs land
  val groupByServingInfoParsed: GroupByServingInfoParsed = getGroupByServingInfoParsed

  protected val (keyToBytes, valueToBytes): (Any => Array[Byte], Any => Array[Byte]) =
    getKVSerializers(groupByServingInfoParsed)
  protected val (keyColumns, valueColumns): (Array[String], Array[String]) = getKVColumns
  protected val extraneousRecord: Any => Array[Any] = {
    case x: Map[_, _] if x.keys.forall(_.isInstanceOf[String]) =>
      x.flatMap { case (key, value) => Array(key, value) }.toArray
  }

  private lazy val getGroupByServingInfoParsed: GroupByServingInfoParsed = {
    // Create groupByServingInfo obj and set attribute
    val groupByServingInfo = new GroupByServingInfo()
    groupByServingInfo.setGroupBy(groupBy)

    // Set input avro schema for groupByServingInfo
    groupByServingInfo.setInputAvroSchema(
      inputSchema.toAvroSchema("Input").toString(true)
    )

    // Set key avro schema for groupByServingInfo
    groupByServingInfo.setKeyAvroSchema(
      StructType(
        groupBy.keyColumns.asScala.map { keyCol =>
          val keyColStructType = outputSchema.fields.find(field => field.name == keyCol)
          keyColStructType match {
            case Some(col) => col
            case None =>
              throw new IllegalArgumentException(s"Missing key col from output schema: ${keyCol}")
          }
        }
      ).toAvroSchema("Key")
        .toString(true)
    )

    // Set value avro schema for groupByServingInfo
    val aggInputColNames = groupBy.aggregations.asScala.map(_.inputColumn).toList
    groupByServingInfo.setSelectedAvroSchema(
      StructType(outputSchema.fields.filter(field => aggInputColNames.contains(field.name)))
        .toAvroSchema("Value")
        .toString(true)
    )
    new GroupByServingInfoParsed(
      groupByServingInfo,
      Constants.Partition
    )
  }

  private lazy val getKVSerializers = (
                                        groupByServingInfoParsed: GroupByServingInfoParsed
                                      ) => {
    val keyZSchema: ChrononStructType = groupByServingInfoParsed.keyChrononSchema
    val valueZSchema: ChrononStructType = groupBy.dataModel match {
      case DataModel.Events => groupByServingInfoParsed.valueChrononSchema
      case _ =>
        throw new IllegalArgumentException(
          s"Only the events based data model is supported at the moment - $groupBy"
        )
    }

    (
      AvroConversions.encodeBytes(keyZSchema, extraneousRecord),
      AvroConversions.encodeBytes(valueZSchema, extraneousRecord)
    )
  }

  private lazy val getKVColumns: (Array[String], Array[String]) = {
    val keyColumns = groupBy.keyColumns.asScala.toArray
    val (additionalColumns, _) = groupBy.dataModel match {
      case DataModel.Events =>
        Seq.empty[String] -> timeColumn
      case _ =>
        throw new IllegalArgumentException(
          s"Only the events based data model is supported at the moment - $groupBy"
        )
    }
    val valueColumns = groupBy.aggregationInputs ++ additionalColumns
    (keyColumns, valueColumns)
  }

  override def open(configuration: Configuration): Unit = {
    super.open(configuration)
    val metricsGroup = getRuntimeContext.getMetricGroup
      .addGroup("chronon")
      .addGroup("feature_group", groupBy.getMetaData.getName)
    avroConversionErrorCounter = metricsGroup.counter("avro_conversion_errors")
  }

  override def close(): Unit = super.close()

  override def flatMap(value: Map[String, Any], out: Collector[PutRequest]): Unit =
    try {
      out.collect(avroConvertMapToPutRequest(value))
    } catch {
      case e: Exception =>
        // To improve availability, we don't rethrow the exception. We just drop the event
        // and track the errors in a metric. If there are too many errors we'll get alerted/paged.
        println(s"Error converting to Avro bytes - $e")
        avroConversionErrorCounter.inc()
    }

  def avroConvertMapToPutRequest(in: Map[String, Any]): PutRequest = {
    val tsMills = in(timeColumnAlias).asInstanceOf[Long]
    val keyBytes = keyToBytes(keyColumns.map(in.get(_).get))
    val valueBytes = valueToBytes(valueColumns.map(in.get(_).get))
    PutRequest(keyBytes, valueBytes, streamingDataset, Some(tsMills))
  }

}
