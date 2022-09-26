package ai.chronon.spark.stats

import ai.chronon
import ai.chronon.api.Extensions._
import ai.chronon.api._
import ai.chronon.online._
import ai.chronon.spark.{PartitionRange, TableUtils}
import org.apache.spark.sql.SparkSession

import java.util
import scala.collection.JavaConverters._

class CompareJob(session: SparkSession, joinConf: Join, endDate: String, impl: Api) extends Serializable {

  val kvStore: KVStore = impl.genKvStore
  val metadataStore = new MetadataStore(kvStore, timeoutMillis = 10000)
  val fetcher: Fetcher = impl.fetcher
  val joinCodec = fetcher.getJoinCodecs(joinConf.metaData.nameToFilePath).get
  val rawTable: String = impl.logTable
  val tblProperties = Option(joinConf.metaData.tableProperties)
    .map(_.asScala.toMap)
    .getOrElse(Map.empty[String, String])
  val tableUtils: TableUtils = TableUtils(session)

  private def unfilledRange(inputTable: String, outputTable: String): Option[PartitionRange] = {
    val joinName = joinConf.metaData.nameToFilePath
    val inputPartitions = session.sqlContext
      .sql(
        s"""
           |select distinct ${Constants.PartitionColumn}
           |from $inputTable
           |where name = '$joinName' """.stripMargin)
      .collect()
      .map(row => row.getString(0))
      .toSet

    val inputStart = inputPartitions.reduceOption(Ordering[String].min)
    assert(inputStart.isDefined,
        s"""
           |The join name $joinName does not have available logged data yet.
           |Please double check your logging status""".stripMargin)

    val fillablePartitions = PartitionRange(inputStart.get, endDate).partitions.toSet
    val outputMissing = fillablePartitions -- tableUtils.partitions(outputTable)
    val inputMissing = fillablePartitions -- inputPartitions
    val missingPartitions = outputMissing -- inputMissing

    println(
      s"""
         |   Unfilled range computation:
         |   Output table: $outputTable
         |   Missing output partitions: $outputMissing
         |   Missing input partitions: $inputMissing
         |   Unfilled Partitions: $missingPartitions
         |""".stripMargin)
    if (missingPartitions.isEmpty) {
      println(
        s"$outputTable seems to be caught up - to either " +
          s"$inputTable(latest ${tableUtils.lastAvailablePartition(inputTable)}) or $endDate.")
      return None
    }
    Some(PartitionRange(missingPartitions.min, missingPartitions.max))
  }

  private def buildLogTable(): Unit = {
    val unfilled = unfilledRange(rawTable, joinConf.metaData.loggedTable)
    if (unfilled.isEmpty) return
    val joinName = joinConf.metaData.nameToFilePath
    val rawTableScan = unfilled.get.genScanQuery(null, rawTable)
    val rawDf = tableUtils.sql(rawTableScan).where(s"name = '$joinName'")
    println(s"scanned data for $joinName")
    val outputSize = joinCodec.outputFields.length
    tableUtils.insertPartitions(CompareJob.flattenKeyValueBytes(rawDf, joinCodec, outputSize),
                                joinConf.metaData.loggedTable, tableProperties = tblProperties)
  }

  // replace join's left side with the logged table
  private def buildComparisonJoin(): Join = {
    val copiedJoin = joinConf.deepCopy()
    val loggedSource: Source = new Source()
    val loggedEvents: EventSource = new EventSource()
    val query = new Query()
    val mapping = joinCodec.keyFields.map(_.name).map(k => k -> k) ++
      joinCodec.valueFields.map(_.name).map(v => s"$v${CompareMetrics.loggedSuffix}" -> v) ++
      joinCodec.timeFields.map(_.name).map(t => t -> t)
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
    val unfilled = tableUtils.unfilledRange(joinConf.metaData.comparisonTable, PartitionRange(null, endDate), Some(joinConf.metaData.loggedTable))
    if (unfilled.isEmpty) return
    val join = new chronon.spark.Join(buildComparisonJoin(), unfilled.get.end, TableUtils(session))
    join.computeJoin(Some(30))
  }

  def buildConsistencyMetrics(): DataMetrics = {
    buildLogTable()
    buildComparisonTable()
    val unfilled = tableUtils.unfilledRange(joinConf.metaData.consistencyTable, PartitionRange(null, endDate), Some(joinConf.metaData.comparisonTable))
    if (unfilled.isEmpty) return null
    val comparisonDf = tableUtils.sql(unfilled.get.genScanQuery(null, joinConf.metaData.comparisonTable))
    val renamedDf =
      joinCodec.valueFields.foldLeft(comparisonDf)((df, field) =>
        df.withColumnRenamed(field.name, s"${field.name}${CompareMetrics.backfilledSuffix}"))
    val (df, metrics) = CompareMetrics.compute(joinCodec.valueFields, renamedDf)
    df.withTimeBasedColumn("ds").save(joinConf.metaData.consistencyTable, tableProperties = tblProperties)
    metadataStore.putConsistencyMetrics(joinConf, metrics)
    metrics
  }
}


