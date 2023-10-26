package ai.chronon.spark.stats

import ai.chronon
import ai.chronon.api.Extensions._
import ai.chronon.api._
import ai.chronon.online._
import ai.chronon.spark.Extensions._
import ai.chronon.spark.{BaseTableUtils, PartitionRange, TableUtils}
import org.apache.spark.sql.SparkSession

import java.util
import scala.collection.JavaConverters._
import scala.util.ScalaVersionSpecificCollectionsConverter

class ConsistencyJob(session: SparkSession, joinConf: Join, endDate: String, tableUtilsOpt: Option[BaseTableUtils] = None) extends Serializable {

  val tblProperties: Map[String, String] = Option(joinConf.metaData.tableProperties)
    .map(_.asScala.toMap)
    .getOrElse(Map.empty[String, String])
  implicit val tableUtils: BaseTableUtils = tableUtilsOpt match {
    case Some(tblUtils) => tblUtils
    case None => TableUtils(session)
  }

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
    // apply sampling logic to reduce OOC offline compute overhead
    val wheres = ScalaVersionSpecificCollectionsConverter.convertScalaSeqToJava(
      if (joinConf.metaData.consistencySamplePercent < 100) {
        Seq(s"RAND() <= ${joinConf.metaData.consistencySamplePercent / 100}")
      } else {
        Seq()
      }
    )
    query.setWheres(wheres)
    loggedEvents.setQuery(query)
    loggedEvents.setTable(joinConf.metaData.loggedTable)
    loggedSource.setEvents(loggedEvents)
    copiedJoin.setLeft(loggedSource)
    val newName = joinConf.metaData.comparisonConfName
    copiedJoin.metaData.setName(newName)
    // mark OOC tables as chronon_ooc_table
    if (!copiedJoin.metaData.isSetTableProperties) {
      copiedJoin.metaData.setTableProperties(new util.HashMap[String, String]())
    }
    copiedJoin.metaData.tableProperties.put(Constants.ChrononOOCTable, true.toString)
    copiedJoin
  }

  private def buildComparisonTable(): Unit = {
    val unfilledRanges = tableUtils
      .unfilledRanges(joinConf.metaData.comparisonTable,
                      PartitionRange(null, endDate),
                      Some(Seq(joinConf.metaData.loggedTable)))
      .getOrElse(Seq.empty)
    if (unfilledRanges.isEmpty) return
    val join = new chronon.spark.Join(buildComparisonJoin(), unfilledRanges.last.end, tableUtils)
    println("Starting compute Join for comparison table")
    val compareDf = join.computeJoin(Some(30))
    println("======= side-by-side comparison schema =======")
    println(compareDf.schema.pretty)
  }

  def buildConsistencyMetrics(): DataMetrics = {
    // migrate legacy configs without consistencySamplePercent param
    if (!joinConf.metaData.isSetConsistencySamplePercent) {
      println("consistencySamplePercent is unset and will default to 100")
      joinConf.metaData.consistencySamplePercent = 100
    }

    if (joinConf.metaData.consistencySamplePercent == 0) {
      println(s"Exit ConsistencyJob because consistencySamplePercent = 0 for join conf ${joinConf.metaData.name}")
      return DataMetrics(Seq())
    }

    buildComparisonTable()
    println("Determining Range between consistency table and comparison table")
    val unfilledRanges = tableUtils
      .unfilledRanges(joinConf.metaData.consistencyTable,
                      PartitionRange(null, endDate),
                      Some(Seq(joinConf.metaData.comparisonTable)))
      .getOrElse(Seq.empty)
    if (unfilledRanges.isEmpty) return null
    val allMetrics = unfilledRanges.map { unfilled =>
      val comparisonDf = tableUtils.sql(unfilled.genScanQuery(null, joinConf.metaData.comparisonTable))
      val loggedDf =
        tableUtils.sql(unfilled.genScanQuery(null, joinConf.metaData.loggedTable)).drop(Constants.SchemaHash)
      // there could be external columns that are logged during online env, therefore they could not be used for computing OOC
      val loggedDfNoExternalCols = loggedDf.select(comparisonDf.columns.map(org.apache.spark.sql.functions.col): _*)
      println("Starting compare job for stats")
      val joinKeys = if (joinConf.isSetRowIds) {
        ScalaVersionSpecificCollectionsConverter.convertJavaListToScala(joinConf.rowIds)
      } else {
        JoinCodec.timeFields.map(_.name).toList ++ joinConf.leftKeyCols
      }
      println(s"Using ${joinKeys.mkString("[", ",", "]")} as join keys between log and backfill.")
      val (compareDf, metricsDf, metrics) = CompareBaseJob.compare(comparisonDf,
                                                                   loggedDfNoExternalCols,
                                                                   keys = joinKeys,
                                                                   tableUtils)
      println("Saving output.")
      val outputDf = metricsDf.withTimeBasedColumn(Constants.PartitionColumn)
      println(s"output schema ${outputDf.schema.fields.map(sb => (sb.name, sb.dataType)).toMap.mkString("\n - ")}")
      tableUtils.insertPartitions(outputDf,
                                  joinConf.metaData.consistencyTable,
                                  tableProperties = tblProperties,
                                  autoExpand = true)
      metrics
    }
    DataMetrics(allMetrics.flatMap(_.series))
  }
}
