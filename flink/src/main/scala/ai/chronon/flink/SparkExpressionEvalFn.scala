package ai.chronon.flink

import ai.chronon.api.Extensions.{GroupByOps, MetadataOps}
import ai.chronon.api.{Constants, GroupBy, Query, StructType, StructType => ChrononStructType}
import ai.chronon.online.{CatalystUtil, SparkConversions}
import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.configuration.Configuration
import org.apache.flink.util.Collector
import org.apache.spark.sql.Encoder
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.StructType

import scala.jdk.CollectionConverters.{asScalaBufferConverter, mapAsScalaMapConverter}

/**
 * Interface to provide Spark Encoders for a given type T
 * (based on T being a Thrift / Proto/ ..)
 */
trait EncoderProvider[T] extends Serializable {
  def getEncoder: Encoder[T]
}

/**
 * A Flink function that uses Chronon's CatalystUtil to evaluate the Spark SQL expression in a GroupBy.
 * @param encoder Spark Encoder for the input data type
 * @param groupBy The GroupBy to evaluate.
 * @tparam T The type of the input data.
 */
class SparkExpressionEvalFn[T](encoder: Encoder[T], groupBy: GroupBy) extends RichFlatMapFunction[T, Map[String, Any]] {

  private val query: Query = groupBy.streamingSource.get.getEvents.query

  private val timeColumnAlias: String = Constants.TimeColumn
  private val timeColumn: String = Option(query.timeColumn).getOrElse(timeColumnAlias)
  private val transforms: Seq[(String, String)] =
    (query.selects.asScala ++ Map(timeColumnAlias -> timeColumn)).toSeq
  private val filters: Seq[String] = query.getWheres.asScala

  // Chronon's CatalystUtil expects a Chronon `StructType` so we convert the
  // Encoder[T]'s schema to one.
  private val chrononSchema: ChrononStructType =
  ChrononStructType.from(
    s"${groupBy.metaData.cleanName}",
    SparkConversions.toChrononSchema(encoder.schema)
  )

  def getOutputSchema: StructType = {
    // before we do anything, run our setup statements.
    // in order to create the output schema, we'll evaluate expressions
    // including UDFs so we'll need them to be registered already.
    logger.info("--- REGISTERING ALL UDFS ---")
    ShepherdUdfRegistry.registerAllUdfs(CatalystUtil.session)
    logger.info("--- FINISHED REGISTERING ALL UDFS ---")
    CatalystUtil.getOutputSparkSchema(transforms, chrononSchema)
  }

  override def open(configuration: Configuration): Unit = {
    super.open(configuration)
    // TODO create CatalystUtil object
    // TODO create rowSerializer object
  }

  // TODO flesh out the implementation
  def flatMap(inputEvent: T, out: Collector[Map[String, Any]]): Unit = {
    // todo fill me in
  }

  override def close(): Unit = {
    super.close()
    // TODO close CatalystUtil session
  }
}
