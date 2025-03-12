/*
 *    Copyright (C) 2023 The Chronon Authors.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package ai.chronon.spark

import org.slf4j.LoggerFactory
import ai.chronon.api
import ai.chronon.api.Extensions._
import ai.chronon.api._
import ai.chronon.online._
import ai.chronon.spark.Extensions.StructTypeOps
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import java.util.Base64

import scala.+:
import scala.collection.mutable
import scala.collection.Seq
import scala.util.ScalaJavaConversions.{IterableOps, MapOps}
import scala.util.{Failure, Success, Try}

import ai.chronon.online.OnlineDerivationUtil.timeFields

/**
  * Purpose of LogFlattenerJob is to unpack serialized Avro data from online requests and flatten each field
  * (both keys and values) into individual columns and save to an offline "flattened" log table.
  *
  * Steps:
  * 1. determine unfilled range and pull raw logs from partitioned log table
  * 2. fetch joinCodecs for all unique schema_hash present in the logs
  * 3. build a merged schema from all schema versions, which will be used as output schema
  * 4. unpack each row and adhere to the output schema
  * 5. save the schema info in the flattened log table properties (cumulatively)
  */
class LogFlattenerJob(session: SparkSession,
                      joinConf: api.Join,
                      endDate: String,
                      logTable: String,
                      schemaTable: String,
                      stepDays: Option[Int] = None)
    extends Serializable {
  @transient lazy val logger = LoggerFactory.getLogger(getClass)
  implicit val tableUtils: TableUtils = TableUtils(session)
  val joinTblProps: Map[String, String] = Option(joinConf.metaData.tableProperties)
    .map(_.toScala)
    .getOrElse(Map.empty[String, String])
  val metrics: Metrics.Context = Metrics.Context(Metrics.Environment.JoinLogFlatten, joinConf)

  private def getUnfilledRanges(inputTable: String, outputTable: String): Seq[PartitionRange] = {
    val partitionName: String = joinConf.metaData.nameToFilePath.replace("/", "%2F")
    val unfilledRangeTry = Try(
      tableUtils.unfilledRanges(
        outputTable,
        PartitionRange(null, endDate),
        Some(Seq(inputTable)),
        Map(inputTable -> Map("name" -> partitionName))
      )
    )

    val ranges = unfilledRangeTry match {
      case Failure(_: AssertionError) => {
        logger.info(s"""
             |The join name ${joinConf.metaData.nameToFilePath} does not have available logged data yet.
             |Please double check your logging status""".stripMargin)
        Seq()
      }
      case Success(None) => {
        logger.info(
          s"$outputTable seems to be caught up - to either " +
            s"$inputTable(latest ${tableUtils.lastAvailablePartition(inputTable)}) or $endDate.")
        Seq()
      }
      case Success(Some(partitionRange)) => {
        partitionRange
      }
      case Failure(otherException) => throw otherException
    }

    if (stepDays.isEmpty) {
      ranges
    } else {
      ranges.flatMap(_.steps(stepDays.get))
    }
  }

  // remove duplicate fields and raise if two fields have the same name but different data types.
  // output order of the fields is based on the first appearance of each field in the iterable
  private def dedupeFields(fields: Iterable[StructField]): Iterable[StructField] = {
    val fieldsBuilder = mutable.LinkedHashMap[String, DataType]()
    fields.foreach { f =>
      {
        if (fieldsBuilder.contains(f.name)) {
          if (fieldsBuilder(f.name) != f.fieldType) {
            throw new Exception(
              s"Found field with same name ${f.name} but different dataTypes: ${fieldsBuilder(f.name)} vs ${f.fieldType}")
          }
        } else {
          fieldsBuilder.put(f.name, f.fieldType)
        }
      }
    }
    fieldsBuilder.map(f => StructField(f._1, f._2))
  }

  private def flattenKeyValueBytes(rawDf: Dataset[Row], schemaMap: Map[String, LoggingSchema]): DataFrame = {

    val allDataFields = dedupeFields(schemaMap.values.flatMap(_.keyFields) ++ schemaMap.values.flatMap(_.valueFields))
    // contextual features are logged twice in keys and values, where values are prefixed with ext_contextual
    // here we exclude the duplicated fields as the two are always identical
    val dataFields = allDataFields.filterNot(_.name.startsWith(Constants.ContextualPrefix))
    val metadataFields = StructField(Constants.SchemaHash, StringType) +: timeFields
    val outputSchema = StructType("", metadataFields ++ dataFields)
    val (keyBase64Idx, valueBase64Idx, tsIdx, dsIdx, schemaHashIdx) = (0, 1, 2, 3, 4)
    val outputRdd: RDD[Row] = rawDf
      .select("key_base64", "value_base64", "ts_millis", "ds", Constants.SchemaHash)
      .rdd
      .flatMap { row =>
        if (row.isNullAt(schemaHashIdx)) {
          // ignore older logs that do not have schema_hash info
          None
        } else {
          val joinCodec = schemaMap(row.getString(schemaHashIdx))
          val keyBytes = Base64.getDecoder.decode(row.getString(keyBase64Idx))
          val valueBytes = Base64.getDecoder.decode(row.getString(valueBase64Idx))
          val keyRow = Try(joinCodec.keyCodec.decodeRow(keyBytes))
          val valueRow = Try(joinCodec.valueCodec.decodeRow(valueBytes))

          if (keyRow.isFailure || valueRow.isFailure) {
            metrics.increment(Metrics.Name.Exception)
            None
          } else {
            val dataColumns = dataFields.map { field =>
              val keyIdxOpt = joinCodec.keyIndices.get(field)
              val valIdxOpt = joinCodec.valueIndices.get(field)
              if (keyIdxOpt.isDefined) {
                keyRow.toOption.map(_.apply(keyIdxOpt.get)).orNull
              } else if (valIdxOpt.isDefined) {
                valueRow.toOption.map(_.apply(valIdxOpt.get)).orNull
              } else {
                null
              }
            }.toArray

            val metadataColumns = Array(row.get(schemaHashIdx), row.get(tsIdx), row.get(dsIdx))
            val outputRow = metadataColumns ++ dataColumns
            val unpackedRow = SparkConversions.toSparkRow(outputRow, outputSchema).asInstanceOf[GenericRow]
            Some(unpackedRow)
          }
        }
      }

    val outputSparkSchema = SparkConversions.fromChrononSchema(outputSchema)
    session.createDataFrame(outputRdd, outputSparkSchema)
  }

  private def fetchSchemas(hashes: Seq[String]): Map[String, String] = {
    val schemaTableDs = tableUtils.lastAvailablePartition(schemaTable)
    if (schemaTableDs.isEmpty) {
      throw new Exception(s"$schemaTable has no partitions available!")
    }

    session
      .table(schemaTable)
      .where(col(tableUtils.partitionColumn) === schemaTableDs.get)
      .where(col(Constants.SchemaHash).isin(hashes.toSeq: _*))
      .select(
        col(Constants.SchemaHash),
        col("schema_value_last").as("schema_value")
      )
      .collect()
      .map(row => (row.getString(0), row.getString(1)))
      .toMap
  }

  def buildTableProperties(schemaMap: Map[String, String]): Map[String, String] = {
    def escape(str: String): String = str.replace("""\""", """\\""")
    (LogFlattenerJob.readSchemaTableProperties(tableUtils, joinConf.metaData.loggedTable) ++ schemaMap)
      .map {
        case (key, value) => (escape(s"${Constants.SchemaHash}_$key"), escape(value))
      }
  }

  private def columnCount(): Int = {
    Try(tableUtils.getSchemaFromTable(joinConf.metaData.loggedTable).fields.length).toOption.getOrElse(0)
  }

  def buildLogTable(): Unit = {
    if (!joinConf.metaData.isSetSamplePercent) {
      logger.info(s"samplePercent is unset for ${joinConf.metaData.name}. Exit.")
      return
    }
    val unfilledRanges = getUnfilledRanges(logTable, joinConf.metaData.loggedTable)
    if (unfilledRanges.isEmpty) return
    val joinName = joinConf.metaData.nameToFilePath

    val start = System.currentTimeMillis()
    val columnBeforeCount = columnCount()
    val rowCounts = unfilledRanges.map { unfilled =>
      val rawDf = unfilled.scanQueryDf(null, logTable).where(col("name") === joinName)
      val schemaHashes = rawDf.select(col(Constants.SchemaHash)).distinct().collect().map(_.getString(0)).toSeq
      val schemaStringsMap = fetchSchemas(schemaHashes)

      // we do not have exact joinConf at time of logging, and since it is not used during flattening, we pass in null
      val schemaMap = schemaStringsMap.mapValues(LoggingSchema.parseLoggingSchema).map(identity).toMap
      val flattenedDf = flattenKeyValueBytes(rawDf, schemaMap)

      val schemaTblProps = buildTableProperties(schemaStringsMap)
      logger.info("======= Log table schema =======")
      logger.info(flattenedDf.schema.pretty)
      tableUtils.insertPartitions(flattenedDf,
                                  joinConf.metaData.loggedTable,
                                  tableProperties =
                                    joinTblProps ++ schemaTblProps ++ Map(Constants.ChrononLogTable -> true.toString),
                                  autoExpand = true)

      val inputRowCount = rawDf.count()
      // read from output table to avoid recomputation
      val outputRowCount = unfilled.scanQueryDf(query = null, joinConf.metaData.loggedTable).count()

      (inputRowCount, outputRowCount)
    }
    val totalInputRowCount = rowCounts.map(_._1).sum
    val totalOutputRowCount = rowCounts.map(_._2).sum
    val columnAfterCount = columnCount()

    val failureCount = totalInputRowCount - totalOutputRowCount
    metrics.gauge(Metrics.Name.RowCount, totalOutputRowCount)
    metrics.gauge(Metrics.Name.FailureCount, failureCount)
    logger.info(s"Processed logs: ${totalOutputRowCount} rows success, ${failureCount} rows failed.")
    metrics.gauge(Metrics.Name.ColumnBeforeCount, columnBeforeCount)
    metrics.gauge(Metrics.Name.ColumnAfterCount, columnAfterCount)
    val elapsedMins = (System.currentTimeMillis() - start) / 60000
    metrics.gauge(Metrics.Name.LatencyMinutes, elapsedMins)
  }
}

object LogFlattenerJob {

  def readSchemaTableProperties(tableUtils: TableUtils, logTable: String): Map[String, String] = {
    val curTblProps = tableUtils.getTableProperties(logTable).getOrElse(Map.empty)
    curTblProps
      .filterKeys(_.startsWith(Constants.SchemaHash))
      .map {
        case (key, value) => (key.substring(Constants.SchemaHash.length + 1), value)
      }
      .toMap
  }
}
