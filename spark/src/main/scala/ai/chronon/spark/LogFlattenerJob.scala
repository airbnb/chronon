package ai.chronon.spark

import ai.chronon.api
import ai.chronon.api.Extensions._
import ai.chronon.api._
import ai.chronon.online._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

import java.util.Base64
import scala.collection.mutable
import scala.util.{ScalaVersionSpecificCollectionsConverter, Try}

/*
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
class LogFlattenerJob(session: SparkSession, joinConf: api.Join, endDate: String, logTable: String, schemaTable: String)
    extends Serializable {
  val tableUtils: TableUtils = TableUtils(session)
  val joinTblProps: Map[String, String] = Option(joinConf.metaData.tableProperties)
    .map(ScalaVersionSpecificCollectionsConverter.convertJavaMapToScala)
    .getOrElse(Map.empty[String, String])
  val partitionName: String = joinConf.metaData.nameToFilePath.replace("/", "%2F")
  val metrics: Metrics.Context = Metrics.Context(Metrics.Environment.JoinLogFlatten, joinConf)

  private def unfilledRange(inputTable: String, outputTable: String): Option[PartitionRange] = {
    val inputPartitions = session.sqlContext
      .sql(s"SHOW PARTITIONS $inputTable")
      .collect()
      .map { row =>
        val parsedPartitions = tableUtils.parsePartition(row.getString(0))
        (parsedPartitions("ds"), parsedPartitions("name"))
      }
      .filter(_._2 == partitionName)
      .map(_._1)
      .toSet

    val inputStart = inputPartitions.reduceOption(Ordering[String].min)
    if (inputStart.isEmpty) {
      println(s"""
         |The join name ${joinConf.metaData.nameToFilePath} does not have available logged data yet.
         |Please double check your logging status""".stripMargin)
      return None
    }

    val fillablePartitions = PartitionRange(inputStart.get, endDate).partitions.toSet
    val outputMissing = fillablePartitions -- tableUtils.partitions(outputTable)
    val inputMissing = fillablePartitions -- inputPartitions
    val missingPartitions = outputMissing -- inputMissing

    println(s"""
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

  private def buildMergedSchema(keys: Iterable[StructField], values: Iterable[StructField]): Array[StructField] = {
    val fieldsBuilder = mutable.LinkedHashMap[String, DataType]()
    def insert(fields: Iterable[StructField]): Unit = {
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
    }
    insert(keys)
    insert(values)
    fieldsBuilder.toArray.map(f => StructField(f._1, f._2))
  }

  private def flattenKeyValueBytes(rawDf: Dataset[Row], codecMap: Map[String, JoinCodec]): DataFrame = {

    val dataFields = buildMergedSchema(codecMap.values.flatMap(_.keyFields), codecMap.values.flatMap(_.valueFields))
    val metadataFields = StructField(Constants.SchemaHash, StringType) +: JoinCodec.timeFields
    val outputSchema = StructType("", metadataFields ++ dataFields)
    val outputRdd: RDD[Row] = rawDf
      .select("key_base64", "value_base64", "ts_millis", "ds", Constants.SchemaHash)
      .rdd
      .flatMap { row =>
        if (row.isNullAt(4)) {
          // ignore logs that do not have schema_hash info
          None
        } else {
          val joinCodec = codecMap(row.getString(4))
          val keyBytes = Base64.getDecoder.decode(row.getString(0))
          val valueBytes = Base64.getDecoder.decode(row.getString(1))
          val keyRow = Try(joinCodec.keyCodec.decodeRow(keyBytes))
          val valueRow = Try(joinCodec.valueCodec.decodeRow(valueBytes))

          if (keyRow.isFailure || valueRow.isFailure) {
            metrics.increment(Metrics.Name.Exception)
            None
          } else {
            val dataColumns = dataFields.par.map { field =>
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

            val metadataColumns = Array(row.get(4), row.get(2), row.get(3))
            val outputRow = metadataColumns ++ dataColumns
            val unpackedRow = Conversions.toSparkRow(outputRow, outputSchema).asInstanceOf[GenericRow]
            Some(unpackedRow)
          }
        }
      }

    val outputSparkSchema = Conversions.fromChrononSchema(outputSchema)
    session.createDataFrame(outputRdd, outputSparkSchema)
  }

  private def fetchSchemas(hashes: Seq[String]): Map[String, String] = {
    val schemaTableDs = tableUtils.lastAvailablePartition(schemaTable)
    if (schemaTableDs.isEmpty) {
      throw new Exception(s"$schemaTable has no partitions available!")
    }

    session
      .table(schemaTable)
      .where(col(Constants.PartitionColumn) === schemaTableDs.get)
      .where(col(Constants.SchemaHash).isin(hashes: _*))
      .select(
        col(Constants.SchemaHash),
        col("schema_value_last").as("schema_value")
      )
      .collect()
      .map(row => (row.getString(0), row.getString(1)))
      .toMap
  }

  def buildTblProperties(schemaMap: Map[String, String]): Map[String, String] = {
    def escape(str: String): String = str.replace("""\""", """\\""")
    (LogFlattenerJob.readSchemaTblProps(tableUtils, joinConf) ++ schemaMap)
      .map {
        case (key, value) => (escape(s"${Constants.SchemaHash}_$key"), escape(value))
      }
  }

  private def columnCount(): Int = {
    Try(tableUtils.getSchemaFromTable(joinConf.metaData.loggedTable).fields.length).toOption.getOrElse(0)
  }

  def buildLogTable(): Unit = {
    if (!joinConf.metaData.isSetSamplePercent) {
      println(s"samplePercent is unset for ${joinConf.metaData.name}. Exit.")
      return
    }
    val unfilled = unfilledRange(logTable, joinConf.metaData.loggedTable)
    if (unfilled.isEmpty) return
    val start = System.currentTimeMillis()
    val joinName = joinConf.metaData.nameToFilePath
    val rawTableScan = unfilled.get.genScanQuery(null, logTable)
    val rawDf = tableUtils.sql(rawTableScan).where(col("name") === joinName)
    println(s"scanned data for $joinName")

    val schemaHashes = rawDf.select(col(Constants.SchemaHash)).distinct().collect().map(_.getString(0)).toSeq
    val schemaMap = fetchSchemas(schemaHashes)

    // we do not have exact joinConf at time of logging, and since it is not used during flattening, we pass in null
    val codecMap = schemaMap.mapValues(JoinCodec.fromLoggingSchema(_, joinConf = null)).map(identity)
    val flattenedDf = flattenKeyValueBytes(rawDf, codecMap)

    val schemaTblProps = buildTblProperties(schemaMap)

    val columnBeforeCount = columnCount()
    tableUtils.insertPartitions(flattenedDf,
                                joinConf.metaData.loggedTable,
                                tableProperties = joinTblProps ++ schemaTblProps,
                                autoExpand = true)
    val columnAfterCount = columnCount()
    val outputRowCount = flattenedDf.count()
    val inputRowCount = rawDf.count()
    metrics.gauge(Metrics.Name.RowCount, outputRowCount)
    metrics.gauge(Metrics.Name.FailureCount, inputRowCount - outputRowCount)
    metrics.gauge(Metrics.Name.ColumnBeforeCount, columnBeforeCount)
    metrics.gauge(Metrics.Name.ColumnAfterCount, columnAfterCount)
    val elapsedMins = (System.currentTimeMillis() - start) / 60000
    metrics.gauge(Metrics.Name.LatencyMinutes, elapsedMins)
  }
}

object LogFlattenerJob {

  def readSchemaTblProps(tableUtils: TableUtils, joinConf: api.Join): Map[String, String] = {
    val curTblProps = tableUtils.getTableProperties(joinConf.metaData.loggedTable).getOrElse(Map.empty)
    curTblProps
      .filterKeys(_.startsWith(Constants.SchemaHash))
      .map {
        case (key, value) => (key.substring(Constants.SchemaHash.length + 1), value)
      }
  }
}
