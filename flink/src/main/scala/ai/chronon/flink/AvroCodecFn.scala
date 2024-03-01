package ai.chronon.flink

import org.slf4j.LoggerFactory
import ai.chronon.api.Extensions.GroupByOps
import ai.chronon.api.{Constants, DataModel, Query, StructType => ChrononStructType}
import ai.chronon.flink.window.TimestampedTile
import ai.chronon.online.{AvroConversions, GroupByServingInfoParsed}
import ai.chronon.online.KVStore.PutRequest
import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.configuration.Configuration
import org.apache.flink.metrics.Counter
import org.apache.flink.util.Collector

import scala.jdk.CollectionConverters._

/**
  * Base class for the Avro conversion Flink operator.
  *
  * Subclasses should override the RichFlatMapFunction methods (flatMap) and groupByServingInfoParsed.
  *
  * @tparam IN The input data type which contains the data to be avro-converted to bytes.
  * @tparam OUT The output data type (generally a PutRequest).
  */
sealed abstract class BaseAvroCodecFn[IN, OUT] extends RichFlatMapFunction[IN, OUT] {
  def groupByServingInfoParsed: GroupByServingInfoParsed

  @transient lazy val logger = LoggerFactory.getLogger(getClass)
  @transient protected var avroConversionErrorCounter: Counter = _
  @transient protected var eventProcessingErrorCounter: Counter =
    _ // Shared metric for errors across the entire Flink app.

  protected lazy val query: Query = groupByServingInfoParsed.groupBy.streamingSource.get.getEvents.query
  protected lazy val streamingDataset: String = groupByServingInfoParsed.groupBy.streamingDataset

  // TODO: update to use constant names that are company specific
  protected lazy val timeColumnAlias: String = Constants.TimeColumn
  protected lazy val timeColumn: String = Option(query.timeColumn).getOrElse(timeColumnAlias)

  protected lazy val (keyToBytes, valueToBytes): (Any => Array[Byte], Any => Array[Byte]) =
    getKVSerializers(groupByServingInfoParsed)
  protected lazy val (keyColumns, valueColumns): (Array[String], Array[String]) = getKVColumns
  protected lazy val extraneousRecord: Any => Array[Any] = {
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
}

/**
  * A Flink function that is responsible for converting the Spark expr eval output and converting that to a form
  * that can be written out to the KV store (PutRequest object)
  * @param groupByServingInfoParsed The GroupBy we are working with
  * @tparam T The input data type
  */
case class AvroCodecFn[T](groupByServingInfoParsed: GroupByServingInfoParsed)
    extends BaseAvroCodecFn[Map[String, Any], PutRequest] {

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
        // and track the errors in a metric. Alerts should be set up on this metric.
        logger.error(s"Error converting to Avro bytes - $e")
        eventProcessingErrorCounter.inc()
        avroConversionErrorCounter.inc()
    }

  def avroConvertMapToPutRequest(in: Map[String, Any]): PutRequest = {
    val tsMills = in(timeColumnAlias).asInstanceOf[Long]
    val keyBytes = keyToBytes(keyColumns.map(in(_)))
    val valueBytes = valueToBytes(valueColumns.map(in(_)))
    PutRequest(keyBytes, valueBytes, streamingDataset, Some(tsMills))
  }
}

/**
  * A Flink function that is responsible for converting an array of pre-aggregates (aka a tile) to a form
  * that can be written out to the KV store (PutRequest object).
  *
  * @param groupByServingInfoParsed The GroupBy we are working with
  * @tparam T The input data type
  */
case class TiledAvroCodecFn[T](groupByServingInfoParsed: GroupByServingInfoParsed)
    extends BaseAvroCodecFn[TimestampedTile, PutRequest] {
  override def open(configuration: Configuration): Unit = {
    super.open(configuration)
    val metricsGroup = getRuntimeContext.getMetricGroup
      .addGroup("chronon")
      .addGroup("feature_group", groupByServingInfoParsed.groupBy.getMetaData.getName)
    avroConversionErrorCounter = metricsGroup.counter("avro_conversion_errors")
    eventProcessingErrorCounter = metricsGroup.counter("event_processing_error")
  }
  override def close(): Unit = super.close()

  override def flatMap(value: TimestampedTile, out: Collector[PutRequest]): Unit =
    try {
      out.collect(avroConvertTileToPutRequest(value))
    } catch {
      case e: Exception =>
        // To improve availability, we don't rethrow the exception. We just drop the event
        // and track the errors in a metric. Alerts should be set up on this metric.
        logger.error(s"Error converting to Avro bytes - ", e)
        eventProcessingErrorCounter.inc()
        avroConversionErrorCounter.inc()
    }

  def avroConvertTileToPutRequest(in: TimestampedTile): PutRequest = {
    val tsMills = in.latestTsMillis

    // 'keys' is a map of (key name in schema -> key value), e.g. Map("card_number" -> "4242-4242-4242-4242")
    // We convert to AnyRef because Chronon expects an AnyRef (for scala <> java interoperability reasons).
    val keys: Map[String, AnyRef] = keyColumns.zip(in.keys.map(_.asInstanceOf[AnyRef])).toMap
    val keyBytes = keyToBytes(in.keys.toArray)
    val valueBytes = in.tileBytes

    logger.debug(
      s"""
        |Avro converting tile to PutRequest - tile=${in}
        |groupBy=${groupByServingInfoParsed.groupBy.getMetaData.getName} tsMills=$tsMills keys=$keys
        |keyBytes=${java.util.Base64.getEncoder.encodeToString(keyBytes)}
        |valueBytes=${java.util.Base64.getEncoder.encodeToString(valueBytes)}
        |streamingDataset=$streamingDataset""".stripMargin
    )

    PutRequest(keyBytes, valueBytes, streamingDataset, Some(tsMills))
  }
}
