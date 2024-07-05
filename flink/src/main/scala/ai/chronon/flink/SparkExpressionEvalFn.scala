package ai.chronon.flink

import org.slf4j.LoggerFactory
import ai.chronon.api.Extensions.{GroupByOps, MetadataOps}
import ai.chronon.api.{Constants, GroupBy, Query, StructType => ChrononStructType}
import ai.chronon.online.{CatalystUtil, SparkConversions}
import com.codahale.metrics.ExponentiallyDecayingReservoir
import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.configuration.Configuration
import org.apache.flink.metrics.{Counter, Histogram}
import org.apache.flink.util.Collector
import org.apache.spark.sql.Encoder
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.flink.dropwizard.metrics.DropwizardHistogramWrapper
import org.apache.spark.sql.types.StructType

import scala.jdk.CollectionConverters.{asScalaBufferConverter, mapAsScalaMapConverter}

/**
  * A Flink function that uses Chronon's CatalystUtil to evaluate the Spark SQL expression in a GroupBy.
  * This function is instantiated for a given type T (specific case class object, Thrift / Proto object).
  * Based on the selects and where clauses in the GroupBy, this function projects and filters the input data and
  * emits a Map which contains the relevant fields & values that are needed to compute the aggregated values for the
  * GroupBy.
  * @param encoder Spark Encoder for the input data type
  * @param groupBy The GroupBy to evaluate.
  * @tparam T The type of the input data.
  */
class SparkExpressionEvalFn[T](encoder: Encoder[T], groupBy: GroupBy) extends RichFlatMapFunction[T, Map[String, Any]] {
  @transient lazy val logger = LoggerFactory.getLogger(getClass)

  private val query: Query = groupBy.streamingSource.get.getEvents.query

  private val timeColumnAlias: String = Constants.TimeColumn
  private val timeColumn: String = Option(query.timeColumn).getOrElse(timeColumnAlias)
  private val transforms: Seq[(String, String)] =
    (query.selects.asScala ++ Map(timeColumnAlias -> timeColumn)).toSeq
  private val filters: Seq[String] = query.getWheres.asScala

  @transient private var catalystUtil: CatalystUtil = _
  @transient private var rowSerializer: ExpressionEncoder.Serializer[T] = _

  @transient private var exprEvalTimeHistogram: Histogram = _
  @transient private var rowSerTimeHistogram: Histogram = _
  @transient private var exprEvalSuccessCounter: Counter = _
  @transient private var exprEvalErrorCounter: Counter = _

  // Chronon's CatalystUtil expects a Chronon `StructType` so we convert the
  // Encoder[T]'s schema to one.
  private val chrononSchema: ChrononStructType =
    ChrononStructType.from(
      s"${groupBy.metaData.cleanName}",
      SparkConversions.toChrononSchema(encoder.schema)
    )

  private[flink] def getOutputSchema: StructType = {
    // before we do anything, run our setup statements.
    // in order to create the output schema, we'll evaluate expressions
    // TODO handle UDFs
    new CatalystUtil(chrononSchema, transforms, filters).getOutputSparkSchema
  }

  override def open(configuration: Configuration): Unit = {
    super.open(configuration)
    catalystUtil = new CatalystUtil(chrononSchema, transforms, filters)
    val eventExprEncoder = encoder.asInstanceOf[ExpressionEncoder[T]]
    rowSerializer = eventExprEncoder.createSerializer()

    val metricsGroup = getRuntimeContext.getMetricGroup
      .addGroup("chronon")
      .addGroup("feature_group", groupBy.getMetaData.getName)

    exprEvalTimeHistogram = metricsGroup.histogram(
      "spark_expr_eval_time",
      new DropwizardHistogramWrapper(
        new com.codahale.metrics.Histogram(new ExponentiallyDecayingReservoir())
      )
    )
    rowSerTimeHistogram = metricsGroup.histogram(
      "spark_row_ser_time",
      new DropwizardHistogramWrapper(
        new com.codahale.metrics.Histogram(new ExponentiallyDecayingReservoir())
      )
    )
    exprEvalSuccessCounter = metricsGroup.counter("spark_expr_eval_success")
    exprEvalErrorCounter = metricsGroup.counter("spark_expr_eval_errors")
  }

  def flatMap(inputEvent: T, out: Collector[Map[String, Any]]): Unit = {
    try {
      val start = System.currentTimeMillis()
      val row: InternalRow = rowSerializer(inputEvent)
      val serFinish = System.currentTimeMillis()
      rowSerTimeHistogram.update(serFinish - start)

      val maybeRow = catalystUtil.performSql(row)
      exprEvalTimeHistogram.update(System.currentTimeMillis() - serFinish)
      maybeRow.foreach(out.collect)
      exprEvalSuccessCounter.inc()
    } catch {
      case e: Exception =>
        // To improve availability, we don't rethrow the exception. We just drop the event
        // and track the errors in a metric. Alerts should be set up on this metric.
        logger.error(s"Error evaluating Spark expression - $e")
        exprEvalErrorCounter.inc()
    }
  }

  override def close(): Unit = {
    super.close()
    CatalystUtil.session.close()
  }
}
