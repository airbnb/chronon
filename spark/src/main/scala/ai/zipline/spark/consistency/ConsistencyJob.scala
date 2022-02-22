package ai.zipline.spark.consistency

import ai.zipline.api.Extensions._
import ai.zipline.api._
import ai.zipline.online._
import ai.zipline.spark.Extensions._
import ai.zipline.spark.{Conversions, PartitionRange, TableUtils}
import ai.zipline.{api, spark}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

import java.util

class ConsistencyJob(session: SparkSession, joinConf: api.Join, endDate: String, impl: Api) extends Serializable {

  val kvStore: KVStore = impl.genKvStore
  val metadataStore = new MetadataStore(kvStore, timeoutMillis = 10000)
  val fetcher: Fetcher = impl.fetcher
  val joinCodec = fetcher.getJoinCodecs(joinConf.metaData.name).get
  val rawTable: String = impl.logTable
  val tableUtils: TableUtils = TableUtils(session)

  private def unfilledRange(inputTable: String, outputTable: String): Option[PartitionRange] = {
    val unfilledRange = tableUtils.unfilledRange(outputTable, PartitionRange(null, endDate), Some(inputTable))
    if (unfilledRange.isEmpty) {
      println(
        s"$outputTable seems to be caught up - to either " +
          s"$inputTable(latest ${tableUtils.lastAvailablePartition(inputTable)}) or $endDate.")
    }
    unfilledRange
  }

  private def buildLogTable(): Unit = {
    val unfilled = unfilledRange(rawTable, joinConf.metaData.loggedTable)
    if (unfilled.isEmpty) return
    val joinName = joinConf.metaData.name
    val rawTableScan = unfilled.get.genScanQuery(null, rawTable)
    val rawDf = tableUtils.sql(rawTableScan).where(s"join_name = '$joinName'")
    println(s"scanned data for $joinName")
    rawDf.show()
    val outputSize = joinCodec.outputFields.length
    tableUtils.insertPartitions(ConsistencyJob.flattenKeyValueBytes(rawDf, joinCodec, outputSize),
                                joinConf.metaData.loggedTable)
  }

  // replace join's left side with the logged table
  private def buildComparisonJoin(): api.Join = {
    val copiedJoin = joinConf.deepCopy()
    val loggedSource: api.Source = new api.Source()
    val loggedEvents: api.EventSource = new EventSource()
    val query = new api.Query()
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
    val unfilled = unfilledRange(joinConf.metaData.loggedTable, joinConf.metaData.comparisonTable)
    if (unfilled.isEmpty) return
    val join = new spark.Join(buildComparisonJoin(), unfilled.get.end, TableUtils(session))
    join.computeJoin(Some(30))
  }

  def buildConsistencyMetrics(): DataMetrics = {
    buildLogTable()
    buildComparisonTable()
    val unfilled = unfilledRange(joinConf.metaData.comparisonTable, joinConf.metaData.consistencyTable)
    if (unfilled.isEmpty) return null
    val comparisonDf = tableUtils.sql(unfilled.get.genScanQuery(null, joinConf.metaData.comparisonTable))
    val renamedDf =
      joinCodec.valueFields.foldLeft(comparisonDf)((df, field) =>
        df.withColumnRenamed(field.name, s"${field.name}${ConsistencyMetrics.backfilledSuffix}"))
    val (df, metrics) = ConsistencyMetrics.compute(joinCodec.valueFields, renamedDf)
    df.show()
    df.withTimeBasedColumn("ds").save(joinConf.metaData.consistencyTable)
    metadataStore.putConsistencyMetrics(joinConf, metrics)
    metrics
  }
}

object ConsistencyJob {
  def flattenKeyValueBytes(rawDf: Dataset[Row], joinCodec: JoinCodec, outputSize: Int): DataFrame = {
    val outputSchema: StructType = StructType("", joinCodec.outputFields)
    val outputSparkSchema = Conversions.fromZiplineSchema(outputSchema)
    val outputRdd: RDD[Row] = rawDf
      .select("key_bytes", "value_bytes", "ts_millis", "ds")
      .rdd
      .map { row =>
        val keyBytes = row.get(0).asInstanceOf[Array[Byte]]
        val keyRow = joinCodec.keyCodec.decodeRow(keyBytes)
        val valueBytes = row.get(1).asInstanceOf[Array[Byte]]

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
