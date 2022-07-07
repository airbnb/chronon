package ai.chronon.spark.consistency

import ai.chronon
import ai.chronon.api.Extensions._
import ai.chronon.api._
import ai.chronon.online._
import ai.chronon.spark.Extensions._
import ai.chronon.spark.{Conversions, PartitionRange, TableUtils}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

import java.util
import java.util.Base64
import scala.collection.JavaConverters._

class ConsistencyJob(session: SparkSession, joinConf: Join, endDate: String, impl: Api) extends Serializable {

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
    tableUtils.insertPartitions(ConsistencyJob.flattenKeyValueBytes(rawDf, joinCodec, outputSize),
                                joinConf.metaData.loggedTable, tableProperties = tblProperties)
  }

  // replace join's left side with the logged table
  private def buildComparisonJoin(): Join = {
    val copiedJoin = joinConf.deepCopy()
    val loggedSource: Source = new Source()
    val loggedEvents: EventSource = new EventSource()
    val query = new Query()
    val mapping = joinCodec.keyFields.map(_.name).map(k => k -> k) ++
      joinCodec.valueFields.map(_.name).map(v => s"$v${ConsistencyMetrics.loggedSuffix}" -> v) ++
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
        df.withColumnRenamed(field.name, s"${field.name}${ConsistencyMetrics.backfilledSuffix}"))
    val (df, metrics) = ConsistencyMetrics.compute(joinCodec.valueFields, renamedDf)
    df.withTimeBasedColumn("ds").save(joinConf.metaData.consistencyTable, tableProperties = tblProperties)
    metadataStore.putConsistencyMetrics(joinConf, metrics)
    metrics
  }
}

object ConsistencyJob {
  def flattenKeyValueBytes(rawDf: Dataset[Row], joinCodec: JoinCodec, outputSize: Int): DataFrame = {
    val outputSchema: StructType = StructType("", joinCodec.outputFields)
    val outputSparkSchema = Conversions.fromChrononSchema(outputSchema)
    val outputRdd: RDD[Row] = rawDf
      .select("key_base64", "value_base64", "ts_millis", "ds")
      .rdd
      .map { row =>
        val keyBytes = Base64.getDecoder.decode(row.getString(0))
        val keyRow = joinCodec.keyCodec.decodeRow(keyBytes)
        val valueBytes = Base64.getDecoder.decode(row.getString(1))

        val result = new Array[Any](outputSize)
        System.arraycopy(keyRow, 0, result, 0, keyRow.length)

        val valueRow = joinCodec.valueCodec.decodeRow(valueBytes)
        if (valueRow != null) {
          System.arraycopy(valueRow, 0, result, keyRow.length, valueRow.length)
        }
        result.update(outputSize - 2, row.get(2))
        result.update(outputSize - 1, row.get(3))
        Conversions.toSparkRow(result, outputSchema).asInstanceOf[GenericRow]
      }
    rawDf.sparkSession.createDataFrame(outputRdd, outputSparkSchema)
  }
}
