package ai.chronon.spark.streaming

import ai.chronon.api
import ai.chronon.api.Extensions.{GroupByOps, MetadataOps}
import ai.chronon.api.{Constants, StructField, StructType}
import ai.chronon.online.Fetcher.Request
import ai.chronon.online.Metrics.Context
import ai.chronon.online._
import ai.chronon.spark.Extensions.InternalRowOps
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.{ForeachWriter, Row, SparkSession}

import scala.util.ScalaJavaConversions.{ListOps, MapOps}


case class JoinWriterComponents(kvStore: KVStore,
                                catalystUtil: CatalystUtil,
                                fetcher: Fetcher)

object JoinWriterComponents {
  def from(joinSource: api.JoinSource, joinSchema: StructType, debug: Boolean)(implicit apiImpl: Api): JoinWriterComponents = {
    val catalystUtil = new CatalystUtil(
      expressions = Option(joinSource.getQuery.selects).map(_.toScala.toSeq).getOrElse(Seq.empty),
      inputSchema = joinSchema,
      filters = Option(joinSource.getQuery.getWheres).map(_.toScala.toSeq).getOrElse(Seq.empty)
    )
    val fetcher = apiImpl.buildFetcher(debug)
    val kvStore = apiImpl.genKvStore
    JoinWriterComponents(kvStore, catalystUtil, fetcher)
  }
}

object JoinWriter {
  def from(groupBy: api.GroupBy, debug: Boolean)(implicit session: SparkSession, apiImpl: Api): JoinWriter = {
    val source = groupBy.streamingSource
    assert(source.get.isSetJoinSource, s"No JoinSource found in the groupBy: ${groupBy.metaData.name}")
    assert(source.isDefined, s"No streaming source present in the groupBy: ${groupBy.metaData.name}")
    val joinSource = source.get.getJoinSource
    val joinCodec = apiImpl.buildFetcher(debug).buildJoinCodec(joinSource.getJoin)
    val joinFields: Array[StructField] = joinCodec.keySchema.fields ++ joinCodec.valueSchema.fields
    val joinSchema = StructType("join_output", joinFields)
    val context = Context(
      environment = Metrics.Environment.GroupByStreaming,
      join=joinSource.getJoin.metaData.cleanName,
      groupBy = groupBy.metaData.cleanName,
      production = groupBy.metaData.isProduction,
      accuracy = groupBy.inferredAccuracy,
      team = groupBy.metaData.owningTeam
    )
    val putRequestBuilder = PutRequestBuilder.from(groupBy, session)
    new JoinWriter(joinSource, joinSchema, context, putRequestBuilder, debug)
  }
}
class JoinWriter(joinSource: api.JoinSource,
                 joinSchema: StructType,
                 context: Metrics.Context,
                 putRequestBuilder: PutRequestBuilder,
                 debug: Boolean)(implicit apiImpl: Api) extends ForeachWriter[Row] {
  private var components: JoinWriterComponents = _
  val joinRequestName: String = joinSource.join.metaData.nameToFilePath
  val leftColumns: Seq[String] = joinSchema.fields.map(_.name)
  val joinOverrides: Map[String, api.Join] = Map(joinRequestName -> joinSource.join)
  val timeIndex: Int = joinSchema.fields.indexWhere(_.name == Constants.TimeColumn)
  @transient private lazy val localStats = new ThreadLocal[StreamingStats]() {
    override def initialValue(): StreamingStats = new StreamingStats(120)
  }

  override def open(partitionId: Long, epochId: Long): Boolean = {
    components = JoinWriterComponents.from(joinSource, joinSchema, debug)
    true
  }

  override def process(row: Row): Unit = {
    localStats.get().increment(putRequestBuilder.from(row, debug = true))

    val keyMap = row.getValuesMap[AnyRef](leftColumns)
    // name matches putJoinConf/getJoinConf logic in MetadataStore.scala
    val responsesFuture =
      components.fetcher.fetchJoin(requests = Seq(Request(joinRequestName, keyMap, Option(row.getLong(timeIndex)))),
        joinOverrides = joinOverrides)
    implicit val ec = components.fetcher.executionContext
    // we don't exit the future land - because awaiting will stall the calling thread in spark streaming
    // we instead let the future run its course asynchronously - we apply all the sql using catalyst instead.
    responsesFuture.foreach { responses =>
      responses.foreach { response =>
        val ts = response.request.atMillis.get
        response.values.failed.foreach { ex => ex.printStackTrace(System.out); context.incrementException(ex) }
        response.values.map { valuesMap =>
          val fullMap = valuesMap ++ keyMap ++ Map(Constants.TimeColumn -> ts)
          val internalRow = components.catalystUtil.inputEncoder(fullMap).asInstanceOf[InternalRow]
          val resultRowOpt = components.catalystUtil.transformFunc(internalRow)
          val outputSchema = components.catalystUtil.outputSparkSchema
          resultRowOpt
            .foreach { internalRow =>
              val row = internalRow.toRow(outputSchema)
              val putRequest = putRequestBuilder.from(row, debug)
              components.kvStore.put(putRequest)
            }
        }
      }
    }
  }

  override def close(errorOrNull: Throwable): Unit = {}
}
