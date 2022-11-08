package ai.chronon.spark.stats

import ai.chronon
import ai.chronon.api.Extensions._
import ai.chronon.spark.Extensions._
import ai.chronon.api._
import ai.chronon.online._
import ai.chronon.spark.{PartitionRange, TableUtils}
import org.apache.spark.sql.SparkSession

import java.util
import scala.collection.JavaConverters._

class ConsistencyJob(session: SparkSession, joinConf: Join, endDate: String) extends Serializable {

  val tblProperties = Option(joinConf.metaData.tableProperties)
    .map(_.asScala.toMap)
    .getOrElse(Map.empty[String, String])
  val tableUtils: TableUtils = TableUtils(session)

  // Replace join's left side with the logged table events to determine offline values of the aggregations.
  private def buildComparisonJoin(): Join = {
    println("Building Join With left as logged")
    val copiedJoin = joinConf.deepCopy()
    val loggedSource: Source = new Source()
    val loggedEvents: EventSource = new EventSource()
    val query = new Query()
    val mapping = joinConf.leftKeyCols.map(k => k -> k)
    val selects = new util.HashMap[String, String]()
    mapping.foreach { case (key, value) => selects.put(key, value) }
    query.setSelects(selects)
    query.setTimeColumn(Constants.TimeColumn)
    query.setStartPartition(joinConf.left.query.startPartition)
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
    println("Starting compute Join for comparison table")
    join.computeJoin(Some(30))
  }

  def buildConsistencyMetrics(): DataMetrics = {
    buildComparisonTable()
    println("Determining Range between consistency table and comparison table")
    val unfilled = tableUtils.unfilledRange(joinConf.metaData.consistencyTable,
                                            PartitionRange(null, endDate),
                                            Some(joinConf.metaData.comparisonTable))
    if (unfilled.isEmpty) return null
    val comparisonDf = tableUtils.sql(unfilled.get.genScanQuery(null, joinConf.metaData.comparisonTable))
    val loggedDf = tableUtils.sql(unfilled.get.genScanQuery(null, joinConf.metaData.loggedTable)).drop(Constants.SchemaHash)
    println("Starting compare job for stats")
    val (df, metrics) = CompareJob.compare(comparisonDf, loggedDf, keys = JoinCodec.timeFields.map(_.name))
    println("Saving output.")
    df.withTimeBasedColumn("ds").save(joinConf.metaData.consistencyTable, tableProperties = tblProperties)
    metrics
  }
}
