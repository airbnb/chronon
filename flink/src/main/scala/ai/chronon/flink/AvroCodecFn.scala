package ai.chronon.flink

import org.slf4j.LoggerFactory
import ai.chronon.api.Extensions.GroupByOps
import ai.chronon.api.{Constants, DataModel, Query, StructType => ChrononStructType}
import ai.chronon.online.{AvroConversions, GroupByServingInfoParsed}
import ai.chronon.online.KVStore.PutRequest
import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.configuration.Configuration
import org.apache.flink.metrics.Counter
import org.apache.flink.util.Collector

import scala.jdk.CollectionConverters._

/**
  * A Flink function that is responsible for converting the Spark expr eval output and converting that to a form
  * that can be written out to the KV store (PutRequest object)
  * @param groupByServingInfoParsed The GroupBy we are working with
  * @tparam T The input data type
  */
case class AvroCodecFn[T](groupByServingInfoParsed: GroupByServingInfoParsed)
    extends RichFlatMapFunction[Map[String, Any], PutRequest] {
  @transient lazy val logger = LoggerFactory.getLogger(getClass)

  @transient protected var avroConversionErrorCounter: Counter = _

  protected val query: Query = groupByServingInfoParsed.groupBy.streamingSource.get.getEvents.query
  protected val streamingDataset: String = groupByServingInfoParsed.groupBy.streamingDataset

  // TODO: update to use constant names that are company specific
  protected val timeColumnAlias: String = Constants.TimeColumn
  protected val timeColumn: String = Option(query.timeColumn).getOrElse(timeColumnAlias)

  protected val (keyToBytes, valueToBytes): (Any => Array[Byte], Any => Array[Byte]) =
    getKVSerializers(groupByServingInfoParsed)
  protected val (keyColumns, valueColumns): (Array[String], Array[String]) = getKVColumns
  protected val extraneousRecord: Any => Array[Any] = {
    case x: Map[_, _] if x.keys.forall(_.isInstanceOf[String]) =>
      x.flatMap { case (key, value) => Array(key, value) }.toArray
  }

  private lazy val getKVSerializers = (
    groupByServingInfoParsed: GroupByServingInfoParsed
  ) => {
    val keyZSchema: ChrononStructType = groupByServingInfoParsed.keyChrononSchema
    val valueZSchema: ChrononStructType = groupByServingInfoParsed.groupBy.dataModel match {
      case DataModel.Events => groupByServingInfoParsed.valueChrononSchema
      case _ =>
        throw new IllegalArgumentException(
          s"Only the events based data model is supported at the moment - ${groupByServingInfoParsed.groupBy}"
        )
    }

    (
      AvroConversions.encodeBytes(keyZSchema, extraneousRecord),
      AvroConversions.encodeBytes(valueZSchema, extraneousRecord)
    )
  }

  private lazy val getKVColumns: (Array[String], Array[String]) = {
    val keyColumns = groupByServingInfoParsed.groupBy.keyColumns.asScala.toArray
    val (additionalColumns, _) = groupByServingInfoParsed.groupBy.dataModel match {
      case DataModel.Events =>
        Seq.empty[String] -> timeColumn
      case _ =>
        throw new IllegalArgumentException(
          s"Only the events based data model is supported at the moment - ${groupByServingInfoParsed.groupBy}"
        )
    }
    val valueColumns = groupByServingInfoParsed.groupBy.aggregationInputs ++ additionalColumns
    (keyColumns, valueColumns)
  }

  override def open(configuration: Configuration): Unit = {
    super.open(configuration)
    val metricsGroup = getRuntimeContext.getMetricGroup
      .addGroup("chronon")
      .addGroup("feature_group", groupByServingInfoParsed.groupBy.getMetaData.getName)
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
        logger.error(s"Error converting to Avro bytes - $e")
        avroConversionErrorCounter.inc()
    }

  def avroConvertMapToPutRequest(in: Map[String, Any]): PutRequest = {
    val tsMills = in(timeColumnAlias).asInstanceOf[Long]
    val keyBytes = keyToBytes(keyColumns.map(in.get(_).get))
    val valueBytes = valueToBytes(valueColumns.map(in.get(_).get))
    PutRequest(keyBytes, valueBytes, streamingDataset, Some(tsMills))
  }

}
