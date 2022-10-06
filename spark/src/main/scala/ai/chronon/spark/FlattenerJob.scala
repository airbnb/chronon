package ai.chronon.spark


import ai.chronon.api
import ai.chronon.api.Extensions._
import ai.chronon.api._
import ai.chronon.online._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

import java.util.Base64
import scala.util.ScalaVersionSpecificCollectionsConverter

class FlattenerJob(session: SparkSession, joinConf: api.Join, endDate: String, impl: Api) extends Serializable {
  val rawTable: String = impl.logTable
  val tableUtils: TableUtils = TableUtils(session)
  val tblProperties: Map[String, String] = Option(joinConf.metaData.tableProperties)
    .map(ScalaVersionSpecificCollectionsConverter.convertJavaMapToScala)
    .getOrElse(Map.empty[String, String])

  private def unfilledRange(inputTable: String, outputTable: String): Option[PartitionRange] = {
    val joinName = joinConf.metaData.nameToFilePath
    val inputPartitions = session.sqlContext
      .sql(s"SHOW PARTITIONS $inputTable")
      .collect()
      .flatMap { row => tableUtils.parsePartition(row.getString(0)) }
      .filter(_._2.equals(joinName))
      .map(_._1)
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

  def flattenKeyValueBytes(rawDf: Dataset[Row]): DataFrame = {
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
    tableUtils.insertPartitions(flattenKeyValueBytes(rawDf),
      joinConf.metaData.loggedTable, tableProperties = tblProperties)
  }
}