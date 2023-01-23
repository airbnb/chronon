package ai.chronon.spark

import ai.chronon.api.Constants
import org.apache.spark.sql.catalyst.plans.logical.{Filter, Project}
import org.apache.spark.sql.functions.{col, count, lit, rand, round}
import org.apache.spark.sql.types.{DataType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import java.time.format.DateTimeFormatter
import java.time.{Instant, ZoneId}

import scala.collection.mutable
import scala.util.{Success, Try}

import org.apache.spark.sql.catalyst.TableIdentifier

case class TableUtils(sparkSession: SparkSession) {

  private val ARCHIVE_TIMESTAMP_FORMAT = "yyyyMMddHHmmss"
  private lazy val archiveTimestampFormatter = DateTimeFormatter
    .ofPattern(ARCHIVE_TIMESTAMP_FORMAT)
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

  def isPartitioned(tableName: String): Boolean = {
    // TODO: use proper way to detect if a table is partitioned or not
    val schema = getSchemaFromTable(tableName)
    schema.fieldNames.contains(Constants.PartitionColumn)
  }

  def partitions(tableName: String, subPartitionsFilter: Map[String, String] = Map.empty): Seq[String] = {
    if (!sparkSession.catalog.tableExists(tableName)) return Seq.empty[String]
    if (isIcebergTable(tableName)) {
      if (subPartitionsFilter.nonEmpty) {
        throw new NotImplementedError("subPartitionsFilter is not supported on Iceberg tables yet.")
      }
      return getIcebergPartitions(tableName)
    }
    sparkSession.sqlContext
      .sql(s"SHOW PARTITIONS $tableName")
      .collect()
      .flatMap { row =>
        {
          val partitionMap = parsePartition(row.getString(0))
          if (
            subPartitionsFilter.forall {
              case (k, v) => partitionMap.get(k).contains(v)
            }
          ) {
            partitionMap.get(Constants.PartitionColumn)
          } else {
            None
          }
        }
      }
  }

  private def isIcebergTable(tableName: String): Boolean =
    Try {
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
    if (partitionsDf.schema(index).dataType.asInstanceOf[StructType].fieldNames.contains("hr")) {
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

  def lastAvailablePartition(tableName: String, subPartitionFilters: Map[String, String] = Map.empty): Option[String] =
    partitions(tableName, subPartitionFilters).reduceOption(Ordering[String].max)

  def firstAvailablePartition(tableName: String, subPartitionFilters: Map[String, String] = Map.empty): Option[String] =
    partitions(tableName, subPartitionFilters).reduceOption(Ordering[String].min)

  def insertPartitions(df: DataFrame,
                       tableName: String,
                       tableProperties: Map[String, String] = null,
                       partitionColumns: Seq[String] = Seq(Constants.PartitionColumn),
                       saveMode: SaveMode = SaveMode.Overwrite,
                       fileFormat: String = "PARQUET",
                       autoExpand: Boolean = false): Unit = {
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

      if (autoExpand) {
        expandTable(tableName, dfRearranged.schema)
      }
    }

    val finalizedDf = if (autoExpand) {
      // reselect the columns so that an deprecated columns will be selected as NULL before write
      val updatedSchema = getSchemaFromTable(tableName)
      val finalColumns = updatedSchema.fieldNames.map(fieldName => {
        if (dfRearranged.schema.fieldNames.contains(fieldName)) {
          col(fieldName)
        } else {
          lit(null).as(fieldName)
        }
      })
      dfRearranged.select(finalColumns: _*)
    } else {
      // if autoExpand is set to false, and an inconsistent df is passed, we want to pass in the df as in
      // so that an exception will be thrown below
      dfRearranged
    }
    repartitionAndWrite(finalizedDf, tableName, saveMode)
  }

  def sql(query: String): DataFrame = {
    val partitionCount = sparkSession.sparkContext.getConf.getInt("spark.default.parallelism", 1000)
    println(
      s"\n----[Running query coalesced into at most $partitionCount partitions]----\n$query\n----[End of Query]----\n")
    val df = sparkSession.sql(query).coalesce(partitionCount)
    df
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
      val saltedDf = df.withColumn(saltCol, round(rand() * 10))
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

  def chunk(partitions: Set[String]): Seq[PartitionRange] = {
    val sortedDates = partitions.toSeq.sorted
    sortedDates.foldLeft(Seq[PartitionRange]()) { (ranges, nextDate) =>
      if (ranges.isEmpty || Constants.Partition.after(ranges.last.end) != nextDate) {
        ranges :+ PartitionRange(nextDate, nextDate)
      } else {
        val newRange = PartitionRange(ranges.last.start, nextDate)
        ranges.dropRight(1) :+ newRange
      }
    }
  }

  @deprecated
  def unfilledRange(outputTable: String,
                    partitionRange: PartitionRange,
                    inputTable: Option[String] = None,
                    inputSubPartitionFilters: Map[String, String] = Map.empty): Option[PartitionRange] = {
    // TODO: delete this after feature stiching PR
    val validPartitionRange = if (partitionRange.start == null) { // determine partition range automatically
      val inputStart = inputTable.flatMap(firstAvailablePartition(_, inputSubPartitionFilters))
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
    val inputMissing = inputTable.toSeq.flatMap(fillablePartitions -- partitions(_, inputSubPartitionFilters))
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

  def unfilledRanges(
      outputTable: String,
      partitionRange: PartitionRange,
      inputTables: Option[Seq[String]] = None,
      inputTableToSubPartitionFiltersMap: Map[String, Map[String, String]] = Map.empty): Option[Seq[PartitionRange]] = {
    val validPartitionRange = if (partitionRange.start == null) { // determine partition range automatically
      val inputStart = inputTables.flatMap(_.map(table =>
        firstAvailablePartition(table, inputTableToSubPartitionFiltersMap.getOrElse(table, Map.empty))).min)
      assert(
        inputStart.isDefined,
        s"""Either partition range needs to have a valid start or
           |an input table with valid data needs to be present
           |inputTables: ${inputTables}, partitionRange: ${partitionRange}
           |""".stripMargin
      )
      partitionRange.copy(start = inputStart.get)
    } else {
      partitionRange
    }
    val outputExisting = partitions(outputTable)
    // To avoid recomputing partitions removed by retention mechanisms we will not fill holes in the very beginning of the range
    // If a user fills a new partition in the newer end of the range, then we will never fill any partitions before that range.
    // We instead log a message saying why we won't fill the earliest hole.
    val cutoffPartition = if (outputExisting.nonEmpty) {
      Seq[String](outputExisting.min, partitionRange.start).filter(_ != null).max
    } else {
      validPartitionRange.start
    }
    val fillablePartitions = validPartitionRange.partitions.toSet.filter(_ >= cutoffPartition)
    val outputMissing = fillablePartitions -- outputExisting
    val allInputExisting = inputTables
      .map { tables =>
        tables.flatMap { table =>
          partitions(table, inputTableToSubPartitionFiltersMap.getOrElse(table, Map.empty))
        }
      }
      .getOrElse(fillablePartitions)

    val inputMissing = fillablePartitions -- allInputExisting
    val missingPartitions = outputMissing -- inputMissing
    val missingChunks = chunk(missingPartitions)
    println(s"""
               |Unfilled range computation:
               |   Output table: $outputTable
               |   Missing output partitions: $outputMissing
               |   Missing input partitions: $inputMissing
               |   Unfilled Partitions: $missingPartitions
               |   Unfilled ranges: $missingChunks
               |""".stripMargin)
    if (missingPartitions.isEmpty) return None
    Some(missingChunks)
  }

  def getTableProperties(tableName: String): Option[Map[String, String]] = {
    try {
      val tableId = sparkSession.sessionState.sqlParser.parseTableIdentifier(tableName)
      Some(sparkSession.sessionState.catalog.getTempViewOrPermanentTableMetadata(tableId).properties)
    } catch {
      case _: Exception => None
    }
  }

  def dropTableIfExists(tableName: String): Unit = {
    val command = s"DROP TABLE IF EXISTS $tableName"
    println(s"Dropping table with command: $command")
    sql(command)
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

  @deprecated
  def dropPartitionsAfterHole(inputTable: String,
                              outputTable: String,
                              partitionRange: PartitionRange,
                              subPartitionFilters: Map[String, String] = Map.empty): Option[String] = {

    def partitionsInRange(table: String, partitionFilter: Map[String, String] = Map.empty): Set[String] = {
      val allParts = partitions(table, partitionFilter)
      val startPrunedParts = Option(partitionRange.start).map(start => allParts.filter(_ >= start)).getOrElse(allParts)
      Option(partitionRange.end).map(end => startPrunedParts.filter(_ <= end)).getOrElse(startPrunedParts).toSet
    }

    val inputPartitions = partitionsInRange(inputTable)
    val outputPartitions = partitionsInRange(outputTable, subPartitionFilters)
    val earliestHoleOpt = (inputPartitions -- outputPartitions).reduceLeftOption(Ordering[String].min)
    earliestHoleOpt.foreach { hole =>
      val toDrop = outputPartitions.filter(_ > hole)
      println(s"""
                   |Earliest hole at $hole in output table $outputTable, relative to $inputTable
                   |Input Parts   : ${inputPartitions.toArray.sorted.mkString("Array(", ", ", ")")}
                   |Output Parts  : ${outputPartitions.toArray.sorted.mkString("Array(", ", ", ")")}
                   |Dropping Parts: ${toDrop.toArray.sorted.mkString("Array(", ", ", ")")}
                   |Sub Partitions: ${subPartitionFilters.map(kv => s"${kv._1}=${kv._2}").mkString("Array(", ", ", ")")}
          """.stripMargin)
      dropPartitions(outputTable, toDrop.toArray.sorted, Constants.PartitionColumn, subPartitionFilters)
    }
    earliestHoleOpt
  }

  def dropPartitions(tableName: String,
                     partitions: Seq[String],
                     partitionColumn: String = Constants.PartitionColumn,
                     subPartitionFilters: Map[String, String] = Map.empty): Unit = {
    if (partitions.nonEmpty && sparkSession.catalog.tableExists(tableName)) {
      val partitionSpecs = partitions
        .map { partition =>
          val mainSpec = s"$partitionColumn='$partition'"
          val specs = mainSpec +: subPartitionFilters.map {
            case (key, value) => s"${key}='${value}'"
          }.toSeq
          specs.mkString("PARTITION (", ",", ")")
        }
        .mkString(",")
      val dropSql = s"ALTER TABLE $tableName DROP IF EXISTS $partitionSpecs"
      sql(dropSql)
    } else {
      println(s"$tableName doesn't exist, please double check before drop partitions")
    }
  }

  def dropPartitionRange(tableName: String,
                         startDate: String,
                         endDate: String,
                         subPartitionFilters: Map[String, String] = Map.empty): Unit = {
    if (sparkSession.catalog.tableExists(tableName)) {
      val toDrop = Stream.iterate(startDate)(Constants.Partition.after).takeWhile(_ <= endDate)
      dropPartitions(tableName, toDrop, Constants.PartitionColumn, subPartitionFilters)
    } else {
      println(s"$tableName doesn't exist, please double check before drop partitions")
    }
  }

  /*
   * This method detects new columns that appear in newSchema but not in current table,
   * and append those new columns at the end of the existing table. This allows continuous evolution
   * of a Hive table without dropping or archiving data.
   *
   * Warning: ALTER TABLE behavior also depends on underlying storage solution.
   * To read using Hive, which differentiates Table-level schema and Partition-level schema, it is required to
   * take an extra step to sync Table-level schema into Partition-level schema in order to read updated data
   * in Hive. To read from Spark, this is not required since it always uses the Table-level schema.
   */
  private def expandTable(tableName: String, newSchema: StructType): Unit = {

    val existingSchema = getSchemaFromTable(tableName)
    val existingFieldsMap = existingSchema.fields.map(field => (field.name, field)).toMap

    val inconsistentFields = mutable.ListBuffer[(String, DataType, DataType)]()
    val newFields = mutable.ListBuffer[StructField]()

    newSchema.fields.foreach(field => {
      val fieldName = field.name
      if (existingFieldsMap.contains(fieldName)) {
        val existingDataType = existingFieldsMap(fieldName).dataType

        // compare on catalogString so that we don't check nullability which is not relevant for hive tables
        if (existingDataType.catalogString != field.dataType.catalogString) {
          inconsistentFields += ((fieldName, existingDataType, field.dataType))
        }
      } else {
        newFields += field
      }
    })

    if (inconsistentFields.nonEmpty) {
      throw IncompatibleSchemaException(inconsistentFields)
    }

    val newFieldDefinitions = newFields.map(newField => s"${newField.name} ${newField.dataType.catalogString}")
    val expandTableQueryOpt = if (newFieldDefinitions.nonEmpty) {
      val tableLevelAlterSql =
        s"""ALTER TABLE ${tableName}
           |ADD COLUMNS (
           |    ${newFieldDefinitions.mkString(",\n    ")}
           |)
           |""".stripMargin

      Some(tableLevelAlterSql)
    } else {
      None
    }

    /* check if any old columns are skipped in new field and send warning */
    val updatedFieldsMap = newSchema.fields.map(field => (field.name, field)).toMap
    val excludedFields = existingFieldsMap.filter {
      case (name, _) => !updatedFieldsMap.contains(name)
    }.toSeq

    if (excludedFields.nonEmpty) {
      val excludedFieldsStr =
        excludedFields.map(tuple => s"columnName: ${tuple._1} dataType: ${tuple._2.dataType.catalogString}")
      println(
        s"""Warning. Detected columns that exist in Hive table but not in updated schema. These are ignored in DDL.
           |${excludedFieldsStr.mkString("\n")}
           |""".stripMargin)
    }

    if (expandTableQueryOpt.nonEmpty) {
      sql(expandTableQueryOpt.get)

      // set a flag in table props to indicate that this is a dynamic table
      sql(alterTablePropertiesSql(tableName, Map(Constants.ChrononDynamicTable -> true.toString)))
    }
  }
}

sealed case class IncompatibleSchemaException(inconsistencies: Seq[(String, DataType, DataType)]) extends Exception {
  override def getMessage: String = {
    val inconsistenciesStr =
      inconsistencies.map(tuple => s"columnName: ${tuple._1} existingType: ${tuple._2} newType: ${tuple._3}")
    s"""Existing columns cannot be modified:
       |${inconsistenciesStr.mkString("\n")}
       |""".stripMargin
  }
}
