package ai.chronon.spark.consistency

import ai.chronon.api.Extensions.MetadataOps
import ai.chronon.api.{Join, StructType}
import ai.chronon.online.{Api, JoinCodec}
import ai.chronon.spark.{Conversions, PartitionRange, TableUtils}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.catalyst.expressions.GenericRow
import ai.chronon.api._
import ai.chronon.online._
import ai.chronon.spark.Extensions._
import scala.collection.JavaConverters._

import java.util.Base64

class FlattenerJob(session: SparkSession, joinConf: Join, endDate: String, impl: Api) extends Serializable {
  val rawTable: String = impl.logTable
  val tableUtils: TableUtils = TableUtils(session)
  val tblProperties = Option(joinConf.metaData.tableProperties)
    .map(_.asScala.toMap)
    .getOrElse(Map.empty[String, String])

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

  def flattenKeyValueBytes(rawDf: Dataset[Row], joinCodec: JoinCodec, outputSize: Int): DataFrame = {
    val outputSchema: StructType = StructType("", joinCodec.outputFields)
    val outputSparkSchema = Conversions.fromChrononSchema(outputSchema)
    val outputRdd: RDD[Row] = rawDf
      .select("key_base64", "value_base64", "ts_millis", "ds", "schema_hash")
      .rdd
      .map { row =>
        val keyBytes = Base64.getDecoder.decode(row.getString(0))
        val schemaHash = Base64.getDecoder.decode(row.getString(5))
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

  private def buildLogTable(): Unit = {
    val unfilled = unfilledRange(rawTable, joinConf.metaData.loggedTable)
    if (unfilled.isEmpty) return
    val joinName = joinConf.metaData.nameToFilePath
    val rawTableScan = unfilled.get.genScanQuery(null, rawTable)
    val rawDf = tableUtils.sql(rawTableScan).where(s"name = '$joinName'")
    println(s"scanned data for $joinName")
    val outputSize = joinCodec.outputFields.length
    tableUtils.insertPartitions(flattenKeyValueBytes(rawDf, joinCodec, outputSize),
      joinConf.metaData.loggedTable, tableProperties = tblProperties)
  }
}