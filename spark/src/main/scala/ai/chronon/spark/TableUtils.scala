package ai.chronon.spark

import ai.chronon.api.Constants
import org.apache.spark.sql.catalyst.plans.logical.{Filter, Project}
import org.apache.spark.sql.functions.{rand, round}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import java.time.format.DateTimeFormatter
import java.time.{Instant, ZoneId}
import scala.util.{Success, Try}
case class TableUtils(sparkSession: SparkSession) {

  private val ARCHIVE_TIMESTAMP_FORMAT = "yyyy_MM_dd_HH_mm_ss"
  private lazy val archiveTimestampFormatter = DateTimeFormatter.ofPattern(ARCHIVE_TIMESTAMP_FORMAT)
    .withZone(ZoneId.systemDefault())

  sparkSession.sparkContext.setLogLevel("ERROR")
  // converts String-s like "a=b/c=d" to Map("a" -> "b", "c" -> "d")
  def parsePartition(pstring: String): Map[String, String] = {
    pstring
      .split("/")
      .map { part =>
        val p = part.split("=", 2)
        p(0) -> p(1)
      }
      .toMap
  }

  def partitions(tableName: String): Seq[String] = {
    if (!sparkSession.catalog.tableExists(tableName)) return Seq.empty[String]
    if (isIcebergTable(tableName)) return getIcebergPartitions(tableName)
    sparkSession.sqlContext
      .sql(s"SHOW PARTITIONS $tableName")
      .collect()
      .flatMap { row => parsePartition(row.getString(0)).get(Constants.PartitionColumn) }
  }

  private def isIcebergTable(tableName: String): Boolean = Try {
    sparkSession.read.format("iceberg").load(tableName)
  } match {
    case Success(_) =>
      println(s"IcebergCheck: Detected iceberg formatted table $tableName.")
      true
    case _ =>
      println(s"IcebergCheck: Checked table $tableName is not iceberg format.")
      false
  }

  private def getIcebergPartitions(tableName: String): Seq[String] = {
    val partitionsDf = sparkSession.read.format("iceberg").load(s"$tableName.partitions")
    val index = partitionsDf.schema.fieldIndex("partition")
    if(partitionsDf.schema(index).dataType.asInstanceOf[StructType].fieldNames.contains("hr")) {
      // Hour filter is currently buggy in iceberg. https://github.com/apache/iceberg/issues/4718
      // so we collect and then filter.
      partitionsDf
        .select("partition.ds", "partition.hr")
        .collect()
        .filter(_.get(1) == null)
        .map(_.getString(0))
        .toSeq
    } else {
      partitionsDf
        .select("partition.ds")
        .collect()
        .map(_.getString(0))
        .toSeq
    }
  }

  // Given a table and a query extract the schema of the columns involved as input.
  def getColumnsFromQuery(query: String): Seq[String] = {
    val parser = sparkSession.sessionState.sqlParser
    val logicalPlan = parser.parsePlan(query)
    logicalPlan
      .collect {
        case p: Project =>
          p.projectList.flatMap(p => parser.parseExpression(p.sql).references.map(attr => attr.name))
        case f: Filter => f.condition.references.map(attr => attr.name)
      }
      .flatten
      .distinct
  }

  def getSchemaFromTable(tableName: String): StructType = {
    sparkSession.sql(s"SELECT * FROM $tableName LIMIT 1").schema
  }

  def lastAvailablePartition(tableName: String): Option[String] =
    partitions(tableName).reduceOption(Ordering[String].max)

  def firstAvailablePartition(tableName: String): Option[String] =
    partitions(tableName)
      .reduceOption(Ordering[String].min)

  def insertPartitions(df: DataFrame,
                       tableName: String,
                       tableProperties: Map[String, String] = null,
                       partitionColumns: Seq[String] = Seq(Constants.PartitionColumn),
                       saveMode: SaveMode = SaveMode.Overwrite,
                       fileFormat: String = "PARQUET"): Unit = {
    // partitions to the last
    val dfRearranged: DataFrame = if (!df.columns.endsWith(partitionColumns)) {
      val colOrder = df.columns.diff(partitionColumns) ++ partitionColumns
      df.select(colOrder.map(df.col): _*)
    } else {
      df
    }

    if (!sparkSession.catalog.tableExists(tableName)) {
      val creationSql = createTableSql(tableName, dfRearranged.schema, partitionColumns, tableProperties, fileFormat)
      try {
        sql(creationSql)
      } catch {
        case e: Exception =>
          println(s"Failed to create table $tableName with error: ${e.getMessage}")
          throw e
      }
    } else {
      if (tableProperties != null && tableProperties.nonEmpty) {
        sql(alterTablePropertiesSql(tableName, tableProperties))
      }
    }

    repartitionAndWrite(dfRearranged, tableName, saveMode)
  }

  def sql(query: String): DataFrame = {
    val numPartitions = sparkSession.sparkContext.getConf.getInt("spark.default.parallelism", 1000)
    println(s"\n----[Running query (coalesced to $numPartitions)]----\n$query\n----[End of Query]----\n")
    val df = sparkSession.sql(query)
    df.coalesce(numPartitions)
  }

  def insertUnPartitioned(df: DataFrame,
                          tableName: String,
                          tableProperties: Map[String, String] = null,
                          saveMode: SaveMode = SaveMode.Overwrite,
                          fileFormat: String = "PARQUET"): Unit = {

    if (!sparkSession.catalog.tableExists(tableName)) {
      sql(createTableSql(tableName, df.schema, Seq.empty[String], tableProperties, fileFormat))
    } else {
      if (tableProperties != null && tableProperties.nonEmpty) {
        sql(alterTablePropertiesSql(tableName, tableProperties))
      }
    }

    repartitionAndWrite(df, tableName, saveMode)
  }

  private def repartitionAndWrite(df: DataFrame, tableName: String, saveMode: SaveMode): Unit = {
    val rowCount = df.count()
    println(s"$rowCount rows requested to be written into table $tableName")
    if (rowCount > 0) {
      // at-least a million rows per partition - or there will be too many files.
      val rddPartitionCount = math.min(800, math.ceil(rowCount / 1000000.0).toInt)
      println(s"repartitioning data for table $tableName into $rddPartitionCount rdd partitions")

      val saltCol = "random_partition_salt"
      val saltedDf = df.withColumn(saltCol, round(rand() * 1000000))
      val repartitionCols =
        if (df.schema.fieldNames.contains(Constants.PartitionColumn)) {
          Seq(Constants.PartitionColumn, saltCol)
        } else { Seq(saltCol) }
      saltedDf
        .repartition(rddPartitionCount, repartitionCols.map(saltedDf.col): _*)
        .drop(saltCol)
        .write
        .mode(saveMode)
        .insertInto(tableName)
      println(s"Finished writing to $tableName")
    }
  }

  private def createTableSql(tableName: String,
                             schema: StructType,
                             partitionColumns: Seq[String],
                             tableProperties: Map[String, String],
                             fileFormat: String): String = {
    val fieldDefinitions = schema
      .filterNot(field => partitionColumns.contains(field.name))
      .map(field => s"${field.name} ${field.dataType.catalogString}")
    val createFragment =
      s"""CREATE TABLE $tableName (
         |    ${fieldDefinitions.mkString(",\n    ")}
         |)""".stripMargin
    val partitionFragment = if (partitionColumns != null && partitionColumns.nonEmpty) {
      val partitionDefinitions = schema
        .filter(field => partitionColumns.contains(field.name))
        .map(field => s"${field.name} ${field.dataType.catalogString}")
      s"""PARTITIONED BY (
         |    ${partitionDefinitions.mkString(",\n    ")}
         |)""".stripMargin
    } else {
      ""
    }
    val propertiesFragment = if (tableProperties != null && tableProperties.nonEmpty) {
      s"""TBLPROPERTIES (
         |    ${tableProperties.transform((k, v) => s"'$k'='$v'").values.mkString(",\n   ")}
         |)""".stripMargin
    } else {
      ""
    }
    Seq(createFragment, partitionFragment, s"STORED AS $fileFormat", propertiesFragment).mkString("\n")
  }

  private def alterTablePropertiesSql(tableName: String, properties: Map[String, String]): String = {
    // Only SQL api exists for setting TBLPROPERTIES
    val propertiesString = properties
      .map {
        case (key, value) =>
          s"'$key' = '$value'"
      }
      .mkString(", ")
    s"ALTER TABLE $tableName SET TBLPROPERTIES ($propertiesString)"
  }

  def unfilledRange(outputTable: String,
                    partitionRange: PartitionRange,
                    inputTable: Option[String] = None): Option[PartitionRange] = {
    val validPartitionRange = if (partitionRange.start == null) { // determine partition range automatically
      val inputStart = inputTable.flatMap(firstAvailablePartition)
      assert(
        inputStart.isDefined,
        s"""Either partition range needs to have a valid start or 
           |an input table with valid data needs to be present
           |inputTable: ${inputTable}, partitionRange: ${partitionRange}
           |""".stripMargin
      )
      partitionRange.copy(start = inputStart.get)
    } else {
      partitionRange
    }
    val fillablePartitions = validPartitionRange.partitions.toSet
    val outputMissing = fillablePartitions -- partitions(outputTable)
    val inputMissing = inputTable.toSeq.flatMap(fillablePartitions -- partitions(_))
    val missingPartitions = outputMissing -- inputMissing
    println(s"""
                 |Unfilled range computation:                             
                 |   Output table: $outputTable
                 |   Missing output partitions: $outputMissing
                 |   Missing input partitions: $inputMissing
                 |   Unfilled Partitions: $missingPartitions
                 |""".stripMargin)
    if (missingPartitions.isEmpty) return None
    Some(PartitionRange(missingPartitions.min, missingPartitions.max))
  }

  def getTableProperties(tableName: String): Option[Map[String, String]] = {
    try {
      val tableId = sparkSession.sessionState.sqlParser.parseTableIdentifier(tableName)
      Some(sparkSession.sessionState.catalog.getTempViewOrPermanentTableMetadata(tableId).properties)
    } catch {
      case _: Exception => None
    }
  }

  def archiveTableIfExists(tableName: String, timestamp: Instant): Unit = {
    if (sparkSession.catalog.tableExists(tableName)) {
      val humanReadableTimestamp = archiveTimestampFormatter.format(timestamp)
      val finalArchiveTableName = s"${tableName}_${humanReadableTimestamp}"
      val command = s"ALTER TABLE $tableName RENAME TO $finalArchiveTableName"
      println(s"Archiving table with command: $command")
      sql(command)
    }
  }

  def dropPartitionsAfterHole(inputTable: String,
                              outputTable: String,
                              partitionRange: PartitionRange): Option[String] = {

    def partitionsInRange(table: String): Set[String] = {
      val allParts = partitions(table)
      val startPrunedParts = Option(partitionRange.start).map(start => allParts.filter(_ >= start)).getOrElse(allParts)
      Option(partitionRange.end).map(end => startPrunedParts.filter(_ <= end)).getOrElse(startPrunedParts).toSet
    }

    val inputPartitions = partitionsInRange(inputTable)
    val outputPartitions = partitionsInRange(outputTable)
    val earliestHoleOpt = (inputPartitions -- outputPartitions).reduceLeftOption(Ordering[String].min)
    earliestHoleOpt.foreach { hole =>
      val toDrop = outputPartitions.filter(_ > hole)
      println(s"""
                   |Earliest hole at $hole in output table $outputTable, relative to $inputTable
                   |Input Parts   : ${inputPartitions.toArray.sorted.mkString("Array(", ", ", ")")}
                   |Output Parts  : ${outputPartitions.toArray.sorted.mkString("Array(", ", ", ")")}
                   |Dropping Parts: ${toDrop.toArray.sorted.mkString("Array(", ", ", ")")} 
          """.stripMargin)
      dropPartitions(outputTable, toDrop.toArray.sorted)
    }
    earliestHoleOpt
  }

  def dropPartitions(tableName: String,
                     partitions: Seq[String],
                     partitionColumn: String = Constants.PartitionColumn): Unit = {
    if (partitions.nonEmpty && sparkSession.catalog.tableExists(tableName)) {
      val partitionSpecs =
        partitions.map(partition => s"PARTITION ($partitionColumn='$partition')").mkString(", ".stripMargin)
      val dropSql = s"ALTER TABLE $tableName DROP IF EXISTS $partitionSpecs"
      sql(dropSql)
    } else {
      println(s"$tableName doesn't exist, please double check before drop partitions")
    }
  }

  def dropPartitionRange(tableName: String, startDate: String, endDate: String): Unit = {
    if (sparkSession.catalog.tableExists(tableName)) {
      val toDrop = Stream.iterate(startDate)(Constants.Partition.after).takeWhile(_ <= endDate)
      val partitionSpecs =
        toDrop.map(ds => s"PARTITION (${Constants.PartitionColumn}='$ds')").mkString(", ".stripMargin)
      val dropSql = s"ALTER TABLE $tableName DROP IF EXISTS $partitionSpecs"
      sql(dropSql)
    } else {
      println(s"$tableName doesn't exist, please double check before drop partitions")
    }
  }
}
