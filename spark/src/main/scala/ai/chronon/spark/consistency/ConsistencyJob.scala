package ai.chronon.spark.consistency

import ai.chronon
import ai.chronon.api.Extensions._
import ai.chronon.api._
import ai.chronon.online._
import ai.chronon.spark.Extensions._
import ai.chronon.spark.{PartitionRange, TableUtils}
import org.apache.spark.sql.SparkSession

import java.util
import scala.collection.JavaConverters._

class ConsistencyJob(session: SparkSession, joinConf: Join, endDate: String, impl: Api) extends Serializable {

  val kvStore: KVStore = impl.genKvStore
  val metadataStore = new MetadataStore(kvStore, timeoutMillis = 10000)
  val fetcher: Fetcher = impl.fetcher
  val joinCodec = fetcher.getJoinCodecs(joinConf.metaData.nameToFilePath).get
  val tblProperties = Option(joinConf.metaData.tableProperties)
    .map(_.asScala.toMap)
    .getOrElse(Map.empty[String, String])
  val tableUtils: TableUtils = TableUtils(session)

  // replace join's left side with the logged table
  private def buildComparisonJoin(): Join = {
    val copiedJoin = joinConf.deepCopy()
    val loggedSource: Source = new Source()
    val loggedEvents: EventSource = new EventSource()
    val query = new Query()
    val mapping = joinCodec.keyFields.map(_.name).map(k => k -> k) ++
      joinCodec.valueFields.map(_.name).map(v => s"$v${ConsistencyMetrics.loggedSuffix}" -> v) ++
      JoinCodec.timeFields.map(_.name).map(t => t -> t)
    val selects = new util.HashMap[String, String]()
    mapping.foreach { case (key, value) => selects.put(key, value) }
    query.setSelects(selects)
    loggedEvents.setQuery(query)
    loggedEvents.setTable(joinConf.metaData.loggedTable)
    loggedSource.setEvents(loggedEvents)
    copiedJoin.setLeft(loggedSource)
    val newName = joinConf.metaData.comparisonConfName
    copiedJoin.metaData.setName(newName)
    copiedJoin
  }

  private def buildComparisonTable(): Unit = {
    val unfilled = tableUtils.unfilledRange(joinConf.metaData.comparisonTable,
                                            PartitionRange(null, endDate),
                                            Some(joinConf.metaData.loggedTable))
    if (unfilled.isEmpty) return
    val join = new chronon.spark.Join(buildComparisonJoin(), unfilled.get.end, TableUtils(session))
    join.computeJoin(Some(30))
  }

  def buildConsistencyMetrics(): DataMetrics = {
    buildComparisonTable()
    val unfilled = tableUtils.unfilledRange(joinConf.metaData.consistencyTable,
                                            PartitionRange(null, endDate),
                                            Some(joinConf.metaData.comparisonTable))
    if (unfilled.isEmpty) return null
    val comparisonDf = tableUtils.sql(unfilled.get.genScanQuery(null, joinConf.metaData.comparisonTable))
    val renamedDf =
      joinCodec.valueFields.foldLeft(comparisonDf)((df, field) =>
        df.withColumnRenamed(field.name, s"${field.name}${ConsistencyMetrics.backfilledSuffix}"))
    val (df, metrics) = ConsistencyMetrics.compute(joinCodec.valueFields, renamedDf)
    df.withTimeBasedColumn("ds").save(joinConf.metaData.consistencyTable, tableProperties = tblProperties)
    metadataStore.putConsistencyMetrics(joinConf, metrics)
    metrics
  }
}
