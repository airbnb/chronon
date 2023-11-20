package ai.chronon.spark

import ai.chronon.api
import ai.chronon.api.{Constants, PartitionSpec}
import ai.chronon.api.Extensions._
import org.apache.spark.sql.catalyst.plans.logical.{Filter, Project}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import java.time.format.DateTimeFormatter
import java.time.{Instant, ZoneId}
import scala.collection.{Seq, mutable}
import scala.util.{Success, Try}

trait BaseTableUtils {
  def sparkSession: SparkSession

  private val ARCHIVE_TIMESTAMP_FORMAT = "yyyyMMddHHmmss"
  private lazy val archiveTimestampFormatter = DateTimeFormatter
    .ofPattern(ARCHIVE_TIMESTAMP_FORMAT)
    .withZone(ZoneId.systemDefault())
  val partitionColumn: String = Constants.PartitionColumn
  val partitionSpec: PartitionSpec = Constants.Partition
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

  def hasValidTimeColumn(inputDf: DataFrame): Boolean = inputDf.schema.find(_.name == Constants.TimeColumn).exists(_.dataType == LongType)

  // A method to add breaks between joins. When joining many dataframes together at once,
  // the Spark driver can experience memory pressure. To alleviate this, breaks in the
  // execution plan can be added. There are various ways to do this (including choosing
  // not to which is the default here) and so we make it configurable by exposing in TableUtils.
  def addJoinBreak(dataFrame: DataFrame): DataFrame = {
    dataFrame
  }

  def tableExists(tableName: String): Boolean = sparkSession.catalog.tableExists(tableName)

  def loadEntireTable(tableName: String): DataFrame = sparkSession.table(tableName)

  def isPartitioned(tableName: String): Boolean = {
    // TODO: use proper way to detect if a table is partitioned or not
    val schema = getSchemaFromTable(tableName)
    schema.fieldNames.contains(partitionColumn)
  }

  // return all specified partition columns in a table in format of Map[partitionName, PartitionValue]
  def allPartitions(tableName: String, partitionColumnsFilter: Seq[String] = Seq.empty): Seq[Map[String, String]] = {
    if (!tableExists(tableName)) return Seq.empty[Map[String, String]]
    if (isIcebergTable(tableName)) {
      throw new NotImplementedError(
        "Multi-partitions retrieval is not supported on Iceberg tables yet." +
          "For single partition retrieval, please use 'partition' method.")
    }
    sparkSession.sqlContext
      .sql(s"SHOW PARTITIONS $tableName")
      .collect()
      .map { row =>
        {
          val partitionMap = parsePartition(row.getString(0))
          if (partitionColumnsFilter.isEmpty) {
            partitionMap
          } else {
            partitionMap.filterKeys(key => partitionColumnsFilter.contains(key)).toMap
          }
        }
      }
  }

  // IF the partition column on the datasource is not in the same format as Constants.Partition then we will need to perform logic on the column to get in the desired format.
  // Users can do this by setting the partition column in the selects
  // For example, if the ds column on our table did not have dashes:
  // selects = Map("ds" -> "from_unixtime(unix_timestamp(ds, 'yyyyMMdd'), 'yyyy-MM-dd')")
  // Which will result in us performing a `SELECT DISTINCT from_unixtime(unix_timestamp(ds, 'yyyyMMdd'), 'yyyy-MM-dd') as ds FROM table` instead of a `SHOW PARTITIONS`
  def partitions(tableName: String, subPartitionsFilter: Map[String, String] = Map.empty, partitionColumnOverride: String = partitionColumn): Seq[String] = {
    if (!tableExists(tableName)) return Seq.empty[String]
    if (isIcebergTable(tableName)) {
      if (subPartitionsFilter.nonEmpty) {
        throw new NotImplementedError("subPartitionsFilter is not supported on Iceberg tables yet.")
      }
      return getIcebergPartitions(tableName)
    }


    if(partitionColumnOverride != partitionColumn){
      val fetchPartitionsWithOverridesSql: String = subPartitionsFilter.keys.foldLeft(s"SELECT DISTINCT ${partitionColumnOverride} as `$partitionColumn` ") {
        case (result, key) => result + s", $key"
      } + s" FROM $tableName"

      sparkSession
        .sql(fetchPartitionsWithOverridesSql)
        .collect()
        .flatMap( row => {
          val partitionMap = row.schema.fieldNames.map(field => field -> row.getString(row.schema.fieldIndex(field))).toMap
          if (
            subPartitionsFilter.forall {
              case (k, v) => partitionMap.get(k).contains(v)
            }
          ) {
            partitionMap.get(partitionColumn)
          } else {
            None
          }
        })
    }
    else{
        sparkSession.sqlContext
          .sql(s"SHOW PARTITIONS $tableName")
          .collect()
          .flatMap { row => {
            val partitionMap = parsePartition(row.getString(0))
            if (
              subPartitionsFilter.forall {
                case (k, v) => partitionMap.get(k).contains(v)
              }
            ) {
              partitionMap.get(partitionColumn)
            } else {
              None
            }
          }
        }
    }
  }

  def isIcebergTable(tableName: String): Boolean =
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

  def getIcebergPartitions(tableName: String): Seq[String] = {
    val partitionsDf = sparkSession.read.format("iceberg").load(s"$tableName.partitions")
    val index = partitionsDf.schema.fieldIndex("partition")
    if (partitionsDf.schema(index).dataType.asInstanceOf[StructType].fieldNames.contains("hr")) {
      // Hour filter is currently buggy in iceberg. https://github.com/apache/iceberg/issues/4718
      // so we collect and then filter.
      partitionsDf
        .select(s"partition.${partitionColumn}", s"partition.${Constants.HourPartitionColumn}")
        .collect()
        .filter(_.get(1) == null)
        .map(_.getString(0))
        .toSeq
    } else {
      partitionsDf
        // TODO(FCOMP-2242) We should factor out a provider for getting Iceberg partitions
        //  so we can inject a Stripe-specific one that takes into account locality_zone
        .select(s"partition.${partitionColumn}")
        .where("partition.locality_zone == 'DEFAULT'")
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
      .map(_.replace("`", ""))
      .distinct
      .sorted
  }

  def getSchemaFromTable(tableName: String): StructType = {
    sql(s"SELECT * FROM $tableName LIMIT 1").schema
  }

  def lastAvailablePartition(tableName: String, subPartitionFilters: Map[String, String] = Map.empty, partitionColumnOverride: String = partitionColumn): Option[String] =
    partitions(tableName, subPartitionFilters, partitionColumnOverride).reduceOption((x, y) => Ordering[String].max(x, y))

  def firstAvailablePartition(tableName: String, subPartitionFilters: Map[String, String] = Map.empty, partitionColumnOverride: String = partitionColumn): Option[String] =
    partitions(tableName, subPartitionFilters, partitionColumnOverride).reduceOption((x, y) => Ordering[String].min(x, y))

  def insertPartitions(df: DataFrame,
                       tableName: String,
                       tableProperties: Map[String, String] = null,
                       partitionColumns: Seq[String] = Seq(partitionColumn),
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

    if (!tableExists(tableName)) {
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

    val finalizedDf = if (tableExists(tableName) && autoExpand) {
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
    repartitionAndWrite(finalizedDf, tableName, saveMode, partition = true, tableProperties)
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

    if (!tableExists(tableName)) {
      sql(createTableSql(tableName, df.schema, Seq.empty[String], tableProperties, fileFormat))
    } else {
      if (tableProperties != null && tableProperties.nonEmpty) {
        sql(alterTablePropertiesSql(tableName, tableProperties))
      }
    }

    repartitionAndWrite(df, tableName, saveMode, partition = false, tableProperties)
  }

  def columnSizeEstimator(dataType: DataType): Long = {
    dataType match {
      // TODO: improve upon this very basic estimate approach
      case ArrayType(elementType, _) => 50 * columnSizeEstimator(elementType)
      case StructType(fields) => fields.map(_.dataType).map(columnSizeEstimator).sum
      case MapType(keyType, valueType, _) => 10 * (columnSizeEstimator(keyType) + columnSizeEstimator(valueType))
      case _ => 1
    }
  }

  private def repartitionAndWrite(df: DataFrame, tableName: String, saveMode: SaveMode, partition: Boolean = true,
                                  tableProperties: Map[String, String] = null): Unit = {
    // get row count and table partition count statistics
    val (rowCount: Long, tablePartitionCount: Int) =
      if (df.schema.fieldNames.contains(partitionColumn)) {
        val result = df.select(count(lit(1)), approx_count_distinct(col(partitionColumn))).head()
        (result.getAs[Long](0), result.getAs[Long](1).toInt)
      } else {
        (df.count(), 1)
      }

    println(s"$rowCount rows requested to be written into table $tableName")
    if (rowCount > 0) {
      val columnSizeEstimate = columnSizeEstimator(df.schema)

      // check if spark is running in local mode or cluster mode
      val isLocal = sparkSession.conf.get("spark.master").startsWith("local")

      // roughly 1 partition count per 1m rows x 100 columns
      val rowCountPerPartition = df.sparkSession.conf
        .getOption(SparkConstants.ChrononRowCountPerPartition)
        .map(_.toDouble)
        .flatMap(value => if (value > 0) Some(value) else None)
        .getOrElse(1e8)

      val totalFileCountEstimate = math.ceil(rowCount * columnSizeEstimate / rowCountPerPartition).toInt
      val dailyFileCountUpperBound = 2000
      val dailyFileCountLowerBound = if (isLocal) 1 else 10
      val dailyFileCountEstimate = totalFileCountEstimate / tablePartitionCount + 1
      val dailyFileCountBounded =
        math.max(math.min(dailyFileCountEstimate, dailyFileCountUpperBound), dailyFileCountLowerBound)

      val outputParallelism = df.sparkSession.conf
        .getOption(SparkConstants.ChrononOutputParallelismOverride)
        .map(_.toInt)
        .flatMap(value => if (value > 0) Some(value) else None)

      if (outputParallelism.isDefined) {
        println(s"Using custom outputParallelism ${outputParallelism.get}")
      }
      val dailyFileCount = outputParallelism.getOrElse(dailyFileCountBounded)

      // finalized shuffle parallelism
      val shuffleParallelism = dailyFileCount * tablePartitionCount
      val saltCol = "random_partition_salt"
      val saltedDf = df.withColumn(saltCol, round(rand() * (dailyFileCount + 1)))

      println(
        s"repartitioning data for table $tableName by $shuffleParallelism spark tasks into $tablePartitionCount table partitions and $dailyFileCount files per partition")
      val repartitionCols =
        if (df.schema.fieldNames.contains(partitionColumn)) {
          Seq(partitionColumn, saltCol)
        } else { Seq(saltCol) }
      val partitionedDf = saltedDf
        .repartition(shuffleParallelism, repartitionCols.map(saltedDf.col).toSeq: _*)
        .drop(saltCol)
      writeDf(partitionedDf, tableName, saveMode, partition, tableProperties) // side-effecting- this actually writes the table
      println(s"Finished writing to $tableName")
    }
  }

  def writeDf(df: DataFrame, tableName: String, saveMode: SaveMode, partition: Boolean = true, tableProperties: Map[String, String] = null): Unit = {
    df.write
      .mode(saveMode)
      .insertInto(tableName)
  }

  def createTableSql(tableName: String,
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

  def alterTablePropertiesSql(tableName: String, properties: Map[String, String]): String = {
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
      if (ranges.isEmpty || partitionSpec.after(ranges.last.end) != nextDate) {
        ranges :+ PartitionRange(nextDate, nextDate)(this)
      } else {
        val newRange = PartitionRange(ranges.last.start, nextDate)(this)
        ranges.dropRight(1) :+ newRange
      }
    }
  }

  def unfilledRanges(outputTable: String,
                     outputPartitionRange: PartitionRange,
                     inputTables: Option[Seq[String]] = None,
                     inputTableToSubPartitionFiltersMap: Map[String, Map[String, String]] = Map.empty,
                     inputToOutputShift: Int = 0,
                     skipFirstHole: Boolean = true,
                     joinConf: Option[api.Join] = None,
                     tableToPartitionOverrideMap: Map[String, String] = Map.empty
                    ): Option[Seq[PartitionRange]] = {
    if (joinConf.map(j => !isPartitioned(j.left.table)).getOrElse(false)) {
      // If the left is unpartitioned, we fall back to just using the passed-in range.
      return Some(Seq(outputPartitionRange))
    }
    val validPartitionRange = if (outputPartitionRange.start == null) { // determine partition range automatically
      val inputStart = inputTables.flatMap(_.map(table =>
        firstAvailablePartition(table, inputTableToSubPartitionFiltersMap.getOrElse(table, Map.empty))).min)
      assert(
        inputStart.isDefined,
        s"""Either partition range needs to have a valid start or
           |an input table with valid data needs to be present
           |inputTables: ${inputTables}, partitionRange: ${outputPartitionRange}
           |""".stripMargin
      )
      outputPartitionRange.copy(start = partitionSpec.shift(inputStart.get, inputToOutputShift))(this)
    } else {
      outputPartitionRange
    }
    val outputExisting = partitions(outputTable)
    // To avoid recomputing partitions removed by retention mechanisms we will not fill holes in the very beginning of the range
    // If a user fills a new partition in the newer end of the range, then we will never fill any partitions before that range.
    // We instead log a message saying why we won't fill the earliest hole.
    val cutoffPartition = if (outputExisting.nonEmpty) {
      Seq[String](outputExisting.min, outputPartitionRange.start).filter(_ != null).max
    } else {
      validPartitionRange.start
    }
    val fillablePartitions =
      if (skipFirstHole) {
        validPartitionRange.partitions.toSet.filter(_ >= cutoffPartition)
      } else {
        validPartitionRange.partitions.toSet
      }
    val outputMissing = fillablePartitions -- outputExisting
    val allInputExisting = inputTables
      .map { tables =>
        tables
          .flatMap { table =>
            partitions(
              table,
              inputTableToSubPartitionFiltersMap.getOrElse(table, Map.empty),
              tableToPartitionOverrideMap.getOrElse(table, partitionColumn)
            )
          }
          .map(partitionSpec.shift(_, inputToOutputShift))
      }
      .getOrElse(fillablePartitions)

    val inputMissing = fillablePartitions -- allInputExisting
    val missingPartitions = outputMissing -- inputMissing
    val missingChunks = chunk(missingPartitions)
    println(s"""
               |Unfilled range computation:
               |   Output table: $outputTable
               |   Missing output partitions: ${outputMissing.toSeq.sorted.prettyInline}
               |   Input tables: ${inputTables.getOrElse(Seq("None")).mkString(", ")}
               |   Missing input partitions: ${inputMissing.toSeq.sorted.prettyInline}
               |   Unfilled Partitions: ${missingPartitions.toSeq.sorted.prettyInline}
               |   Unfilled ranges: ${missingChunks.sorted}
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

  def archiveOrDropTableIfExists(tableName: String, timestamp: Option[Instant]): Unit = {
    val archiveTry = Try(archiveTableIfExists(tableName, timestamp))
    archiveTry.failed.foreach { e =>
      println(s"""Fail to archive table ${tableName}
           |${e.getMessage}
           |Proceed to dropping the table instead.
           |""".stripMargin)
      dropTableIfExists(tableName)
    }
  }

  def archiveTableIfExists(tableName: String, timestamp: Option[Instant]): Unit = {
    if (tableExists(tableName)) {
      val humanReadableTimestamp = archiveTimestampFormatter.format(timestamp.getOrElse(Instant.now()))
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
                              subPartitionFilters: Map[String, String] = Map.empty,
                              joinConf: Option[api.Join] = None): Option[String] = {
    if (joinConf.map(j => !isPartitioned(j.left.table)).getOrElse(false)) {
      // If the left is unpartitioned, we return the start of the range and drop the entire range.
      dropPartitionRange(outputTable, partitionRange.start, partitionRange.end)
      return Some(partitionRange.start)
    }
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
      dropPartitions(outputTable, toDrop.toArray.sorted, partitionColumn, subPartitionFilters)
    }
    earliestHoleOpt
  }

  def dropPartitions(tableName: String,
                     partitions: Seq[String],
                     partitionColumn: String = partitionColumn,
                     subPartitionFilters: Map[String, String] = Map.empty): Unit = {
    if (partitions.nonEmpty && tableExists(tableName)) {
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
    if (tableExists(tableName)) {
      val toDrop = Stream.iterate(startDate)(partitionSpec.after).takeWhile(_ <= endDate)
      dropPartitions(tableName, toDrop, partitionColumn, subPartitionFilters)
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
  def expandTable(tableName: String, newSchema: StructType): Unit = {

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
      throw IncompatibleSchemaException(inconsistentFields.toSeq)
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

case class TableUtils(sparkSession: SparkSession) extends BaseTableUtils

sealed case class IncompatibleSchemaException(inconsistencies: Seq[(String, DataType, DataType)]) extends Exception {
  override def getMessage: String = {
    val inconsistenciesStr =
      inconsistencies.map(tuple => s"columnName: ${tuple._1} existingType: ${tuple._2} newType: ${tuple._3}")
    s"""Existing columns cannot be modified:
       |${inconsistenciesStr.mkString("\n")}
       |""".stripMargin
  }
}
