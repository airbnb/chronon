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
    val unfilledRanges = tableUtils.unfilledRanges(joinConf.metaData.comparisonTable,
                                            PartitionRange(null, endDate),
                                            Some(Seq(joinConf.metaData.loggedTable))).getOrElse(Seq.empty)
    if (unfilledRanges.isEmpty) return
    val join = new chronon.spark.Join(buildComparisonJoin(), unfilledRanges.last.end, TableUtils(session))
    println("Starting compute Join for comparison table")
    join.computeJoin(Some(30))
  }

  def buildConsistencyMetrics(): DataMetrics = {
    buildComparisonTable()
    println("Determining Range between consistency table and comparison table")
    val unfilledRanges = tableUtils.unfilledRanges(joinConf.metaData.consistencyTable,
                                            PartitionRange(null, endDate),
                                            Some(Seq(joinConf.metaData.comparisonTable))).getOrElse(Seq.empty)
    if (unfilledRanges.isEmpty) return null
    val allMetrics = unfilledRanges.map { unfilled =>
      val comparisonDf = tableUtils.sql(unfilled.genScanQuery(null, joinConf.metaData.comparisonTable))
      val loggedDf = tableUtils.sql(unfilled.genScanQuery(null, joinConf.metaData.loggedTable)).drop(Constants.SchemaHash)
      // external columns are logged during online env, therefore they could not be used for computing OOC
      val externalCols: Seq[String] = joinConf.getExternalFeatureCols

      println(s"drop external columns ${externalCols.mkString(",")}")
      val loggedDfNoExternalCols = loggedDf.drop(externalCols: _*)

      println("Starting compare job for stats")
      //TODO: Using timestamp as comparison key is a proxy for row_id as the latter is precise on ts and join key.
      // Using solely timestamp can lead to issues for fetches that involve multiple keys.
      val (df, metrics) = CompareJob.compare(comparisonDf, loggedDfNoExternalCols, keys = JoinCodec.timeFields.map(_.name))
      println("Saving output.")
      df.withTimeBasedColumn("ds").save(joinConf.metaData.consistencyTable, tableProperties = tblProperties)
      metrics
    }
    DataMetrics(allMetrics.flatMap(_.series))
  }
}
