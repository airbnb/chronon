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

import java.io.{PrintWriter, StringWriter}
import org.slf4j.LoggerFactory
import ai.chronon.aggregator.windowing.TsUtils
import ai.chronon.api.{Constants, PartitionSpec}
import ai.chronon.api.Extensions._
import org.apache.spark.sql.catalyst.analysis.TableAlreadyExistsException
import ai.chronon.spark.Extensions.{DfStats, DfWithStats}
import io.delta.tables.DeltaTable
import jnr.ffi.annotations.Synchronized
import org.apache.hadoop.hive.metastore.api.AlreadyExistsException
import org.apache.spark.SparkException
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.plans.logical.{Filter, Project}
import org.apache.spark.sql.delta.DeltaLog
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{AnalysisException, DataFrame, Row, SaveMode, SparkSession}
import org.apache.spark.storage.StorageLevel

import java.time.format.DateTimeFormatter
import java.time.{Instant, ZoneId}
import java.util.concurrent.{ExecutorService, Executors}
import scala.collection.{Seq, mutable}
import scala.collection.immutable
import scala.concurrent.{ExecutionContext, ExecutionContextExecutor}
import scala.util.{Failure, Success, Try}

/**
  * Trait to track the table format in use by a Chronon dataset and some utility methods to help
  * retrieve metadata / configure it appropriately at creation time
  */
sealed trait Format {
  // Return a sequence for partitions where each partition entry consists of a Map of partition keys to values
  def partitions(tableName: String)(implicit sparkSession: SparkSession): Seq[Map[String, String]]

  // Help specify the appropriate table type to use in the Spark create table DDL query
  def createTableTypeString: String

  // Help specify the appropriate file format to use in the Spark create table DDL query
  def fileFormatString(format: String): String
}

case object Hive extends Format {
  def parseHivePartition(pstring: String): Map[String, String] = {
    pstring
      .split("/")
      .map { part =>
        val p = part.split("=", 2)
        p(0) -> p(1)
      }
      .toMap
  }

  override def partitions(tableName: String)(implicit sparkSession: SparkSession): Seq[Map[String, String]] = {
    // data is structured as a Df with single composite partition key column. Every row is a partition with the
    // column values filled out as a formatted key=value pair
    // Eg. df schema = (partitions: String)
    // rows = [ "day=2020-10-10/hour=00", ... ]
    sparkSession.sqlContext
      .sql(s"SHOW PARTITIONS $tableName")
      .collect()
      .map(row => parseHivePartition(row.getString(0)))
  }

  def createTableTypeString: String = ""
  def fileFormatString(format: String): String = s"STORED AS $format"
}

case object Iceberg extends Format {
  override def partitions(tableName: String)(implicit sparkSession: SparkSession): Seq[Map[String, String]] = {
    throw new NotImplementedError(
      "Multi-partitions retrieval is not supported on Iceberg tables yet." +
        "For single partition retrieval, please use 'partition' method.")
  }

  def createTableTypeString: String = "USING iceberg"
  def fileFormatString(format: String): String = ""
}

case object DeltaLake extends Format {
  override def partitions(tableName: String)(implicit sparkSession: SparkSession): Seq[Map[String, String]] = {
    // delta lake doesn't support the `SHOW PARTITIONS <tableName>` syntax - https://github.com/delta-io/delta/issues/996
    // there's alternative ways to retrieve partitions using the DeltaLog abstraction which is what we have to lean into
    // below
    // first pull table location as that is what we need to pass to the delta log
    val describeResult = sparkSession.sql(s"DESCRIBE DETAIL $tableName")
    val tablePath = describeResult.select("location").head().getString(0)

    val snapshot = DeltaLog.forTable(sparkSession, tablePath).update()
    val snapshotPartitionsDf = snapshot.allFiles.toDF().select("partitionValues")
    val partitions = snapshotPartitionsDf.collect().map(r => r.getAs[Map[String, String]](0))
    partitions
  }

  def createTableTypeString: String = "USING DELTA"
  def fileFormatString(format: String): String = ""
}

case class TableUtils(sparkSession: SparkSession) {
  @transient lazy val logger = LoggerFactory.getLogger(getClass)

  private val ARCHIVE_TIMESTAMP_FORMAT = "yyyyMMddHHmmss"
  @transient private lazy val archiveTimestampFormatter = DateTimeFormatter
    .ofPattern(ARCHIVE_TIMESTAMP_FORMAT)
    .withZone(ZoneId.of("UTC"))
  val partitionColumn: String =
    sparkSession.conf.get("spark.chronon.partition.column", "ds")
  private val partitionFormat: String =
    sparkSession.conf.get("spark.chronon.partition.format", "yyyy-MM-dd")
  val partitionSpec: PartitionSpec = PartitionSpec(partitionFormat, WindowUtils.Day.millis)
  val smallModelEnabled: Boolean =
    sparkSession.conf.get("spark.chronon.backfill.small_mode.enabled", "true").toBoolean
  val smallModeNumRowsCutoff: Int =
    sparkSession.conf.get("spark.chronon.backfill.small_mode_cutoff", "5000").toInt
  val backfillValidationEnforced: Boolean =
    sparkSession.conf.get("spark.chronon.backfill.validation.enabled", "true").toBoolean
  // Threshold to control whether or not to use bloomfilter on join backfill. If the backfill row approximate count is under this threshold, we will use bloomfilter.
  // default threshold is 100K rows
  val bloomFilterThreshold: Long =
    sparkSession.conf.get("spark.chronon.backfill.bloomfilter.threshold", "1000000").toLong

  // see what's allowed and explanations here: https://sparkbyexamples.com/spark/spark-persistence-storage-levels/
  val cacheLevelString: String = sparkSession.conf.get("spark.chronon.table_write.cache.level", "NONE").toUpperCase()
  val blockingCacheEviction: Boolean =
    sparkSession.conf.get("spark.chronon.table_write.cache.blocking", "false").toBoolean

  val useIceberg: Boolean = sparkSession.conf.get("spark.chronon.table_write.iceberg", "false").toBoolean

  // write data using the relevant supported Chronon write format
  val maybeWriteFormat: Option[Format] =
    sparkSession.conf.getOption("spark.chronon.table_write.format").map(_.toLowerCase) match {
      case Some("hive")    => Some(Hive)
      case Some("iceberg") => Some(Iceberg)
      case Some("delta")   => Some(DeltaLake)
      case _               => None
    }

  val cacheLevel: Option[StorageLevel] = Try {
    if (cacheLevelString == "NONE") None
    else Some(StorageLevel.fromString(cacheLevelString))
  }.recover {
    case (ex: Throwable) =>
      new RuntimeException(s"Failed to create cache level from string: $cacheLevelString", ex).printStackTrace()
      None
  }.get

  val joinPartParallelism: Int = sparkSession.conf.get("spark.chronon.join.part.parallelism", "1").toInt
  val aggregationParallelism: Int = sparkSession.conf.get("spark.chronon.group_by.parallelism", "1000").toInt
  val maxWait: Int = sparkSession.conf.get("spark.chronon.wait.hours", "48").toInt

  sparkSession.sparkContext.setLogLevel("ERROR")
  // converts String-s like "a=b/c=d" to Map("a" -> "b", "c" -> "d")

  def preAggRepartition(df: DataFrame): DataFrame =
    if (df.rdd.getNumPartitions < aggregationParallelism) {
      df.repartition(aggregationParallelism)
    } else {
      df
    }
  def preAggRepartition(rdd: RDD[Row]): RDD[Row] =
    if (rdd.getNumPartitions < aggregationParallelism) {
      rdd.repartition(aggregationParallelism)
    } else {
      rdd
    }

  def tableExists(tableName: String): Boolean = sparkSession.catalog.tableExists(tableName)

  def loadEntireTable(tableName: String): DataFrame = sparkSession.table(tableName)

  def isPartitioned(tableName: String): Boolean = {
    // TODO: use proper way to detect if a table is partitioned or not
    val schema = getSchemaFromTable(tableName)
    schema.fieldNames.contains(partitionColumn)
  }

  def createDatabase(database: String): Boolean = {
    try {
      val command = s"CREATE DATABASE IF NOT EXISTS $database"
      logger.info(s"Creating database with command: $command")
      sql(command)
      true
    } catch {
      case _: AlreadyExistsException =>
        false // 'already exists' is a swallowable exception
      case e: Exception =>
        logger.error(s"Failed to create database $database", e)
        throw e
    }
  }

  def tableFormat(tableName: String): Format = {
    if (isIcebergTable(tableName)) {
      Iceberg
    } else if (isDeltaTable(tableName)) {
      DeltaLake
    } else {
      Hive
    }
  }

  // return all specified partition columns in a table in format of Map[partitionName, PartitionValue]
  def allPartitions(tableName: String, partitionColumnsFilter: Seq[String] = Seq.empty): Seq[Map[String, String]] = {
    if (!tableExists(tableName)) return Seq.empty[Map[String, String]]
    val format = tableFormat(tableName)
    val partitionSeq = format.partitions(tableName)(sparkSession)
    if (partitionColumnsFilter.isEmpty) {
      partitionSeq
    } else {
      partitionSeq.map { partitionMap =>
        partitionMap.filterKeys(key => partitionColumnsFilter.contains(key)).toMap
      }
    }
  }

  def partitions(tableName: String, subPartitionsFilter: Map[String, String] = Map.empty): Seq[String] = {
    if (!tableExists(tableName)) return Seq.empty[String]
    val format = tableFormat(tableName)

    if (format == Iceberg) {
      if (subPartitionsFilter.nonEmpty) {
        throw new NotImplementedError("subPartitionsFilter is not supported on Iceberg tables yet.")
      }
      return getIcebergPartitions(tableName)
    }

    val partitionSeq = format.partitions(tableName)(sparkSession)
    partitionSeq.flatMap { partitionMap =>
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

  private def isDeltaTable(tableName: String): Boolean = {
    Try {
      val describeResult = sparkSession.sql(s"DESCRIBE DETAIL $tableName")
      describeResult.select("format").first().getString(0).toLowerCase
    } match {
      case Success(format) =>
        logger.info(s"Delta check: Successfully read the format of table: $tableName as $format")
        format == "delta"
      case _ =>
        // the describe detail calls fails for Iceberg tables
        logger.info(s"Delta check: Unable to read the format of the table $tableName using DESCRIBE DETAIL")
        false
    }
  }

  private def isIcebergTable(tableName: String): Boolean =
    Try {
      sparkSession.read.format("iceberg").load(tableName)
    } match {
      case Success(_) =>
        logger.info(s"IcebergCheck: Detected iceberg formatted table $tableName.")
        true
      case _ =>
        logger.info(s"IcebergCheck: Checked table $tableName is not iceberg format.")
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
      .map(_.replace("`", ""))
      .distinct
      .sorted
  }

  // get all the field names including nested struct type field names
  def getFieldNames(schema: StructType): Seq[String] = {
    schema.fields.flatMap { field =>
      field.dataType match {
        case nestedSchema: StructType =>
          val nestedStruct = StructType(
            nestedSchema.fields.map(nestField =>
              StructField(s"${field.name}.${nestField.name}",
                          nestField.dataType,
                          nestField.nullable,
                          nestField.metadata)))
          field.name +: getFieldNames(nestedStruct)
        case _ =>
          Seq(field.name)
      }
    }
  }

  def getSchemaFromTable(tableName: String): StructType = {
    sparkSession.sql(s"SELECT * FROM $tableName LIMIT 1").schema
  }

  // method to check if a user has access to a table
  def checkTablePermission(tableName: String,
                           fallbackPartition: String =
                             partitionSpec.before(partitionSpec.at(System.currentTimeMillis()))): Boolean = {
    logger.info(s"Checking permission for table $tableName...")
    try {
      // retrieve one row from the table
      val partitionFilter = lastAvailablePartition(tableName).getOrElse(fallbackPartition)
      sparkSession.sql(s"SELECT * FROM $tableName where $partitionColumn='$partitionFilter' LIMIT 1").collect()
      true
    } catch {
      case e: SparkException =>
        if (e.getMessage.contains("ACCESS DENIED"))
          logger.error(s"[Error] No access to table: $tableName ")
        else {
          logger.error(s"[Error] Encountered exception when reading table: $tableName.")
        }
        e.printStackTrace()
        false
      case e: Exception =>
        logger.error(s"[Error] Encountered exception when reading table: $tableName.")
        e.printStackTrace()
        true
    }
  }

  def lastAvailablePartition(tableName: String, subPartitionFilters: Map[String, String] = Map.empty): Option[String] =
    partitions(tableName, subPartitionFilters).reduceOption((x, y) => Ordering[String].max(x, y))

  def firstAvailablePartition(tableName: String, subPartitionFilters: Map[String, String] = Map.empty): Option[String] =
    partitions(tableName, subPartitionFilters).reduceOption((x, y) => Ordering[String].min(x, y))

  def insertPartitions(df: DataFrame,
                       tableName: String,
                       tableProperties: Map[String, String] = null,
                       partitionColumns: Seq[String] = Seq(partitionColumn),
                       saveMode: SaveMode = SaveMode.Overwrite,
                       fileFormat: String = "PARQUET",
                       autoExpand: Boolean = false,
                       stats: Option[DfStats] = None,
                       sortByCols: Seq[String] = Seq.empty): Unit = {
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
        case _: TableAlreadyExistsException =>
          logger.info(s"Table $tableName already exists, skipping creation")
        case e: Exception =>
          logger.error(s"Failed to create table $tableName", e)
          throw e
      }
    }
    if (tableProperties != null && tableProperties.nonEmpty) {
      alterTableProperties(tableName, tableProperties, unsetProperties = Seq(Constants.chrononArchiveFlag))
    }

    if (autoExpand) {
      expandTable(tableName, dfRearranged.schema)
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
    repartitionAndWrite(finalizedDf, tableName, saveMode, stats, sortByCols)
  }

  def sql(query: String): DataFrame = {
    val partitionCount = sparkSession.sparkContext.getConf.getInt("spark.default.parallelism", 1000)
    val sw = new StringWriter()
    val pw = new PrintWriter(sw)
    new Throwable().printStackTrace(pw)
    val stackTraceString = sw.toString
    val stackTraceStringPretty = stackTraceString
      .split("\n")
      .filter(_.contains("chronon"))
      .map(_.replace("at ai.chronon.spark.", ""))
      .mkString("\n")

    logger.info(
      s"\n----[Running query coalesced into at most $partitionCount partitions]----\n$query\n----[End of Query]----\n\n Query call path (not an error stack trace): \n$stackTraceStringPretty \n\n --------")
    try {
      // Run the query
      val df = sparkSession.sql(query).coalesce(partitionCount)
      df
    } catch {
      case e: AnalysisException if e.getMessage.contains(" already exists") =>
        logger.warn(s"Non-Fatal: ${e.getMessage}. Query may result in redefinition.")
        sparkSession.sql("SHOW USER FUNCTIONS")
      case e: Exception =>
        logger.error("Error running query:", e)
        throw e
    }
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
        alterTableProperties(tableName, tableProperties, unsetProperties = Seq(Constants.chrononArchiveFlag))
      }
    }

    repartitionAndWrite(df, tableName, saveMode, None)
  }

  def columnSizeEstimator(dataType: DataType): Long = {
    dataType match {
      // TODO: improve upon this very basic estimate approach
      case ArrayType(elementType, _)      => 50 * columnSizeEstimator(elementType)
      case StructType(fields)             => fields.map(_.dataType).map(columnSizeEstimator).sum
      case MapType(keyType, valueType, _) => 10 * (columnSizeEstimator(keyType) + columnSizeEstimator(valueType))
      case _                              => 1
    }
  }

  def wrapWithCache[T](opString: String, dataFrame: DataFrame)(func: => T): Try[T] = {
    val start = System.currentTimeMillis()
    cacheLevel.foreach { level =>
      logger.info(s"Starting to cache dataframe before $opString - start @ ${TsUtils.toStr(start)}")
      dataFrame.persist(level)
    }
    def clear(): Unit = {
      cacheLevel.foreach(_ => dataFrame.unpersist(blockingCacheEviction))
      val end = System.currentTimeMillis()
      logger.info(
        s"Cleared the dataframe cache after $opString - start @ ${TsUtils.toStr(start)} end @ ${TsUtils.toStr(end)}")
    }
    Try {
      val t: T = func
      clear()
      t
    }.recoverWith {
      case ex: Exception =>
        clear()
        Failure(ex)
    }
  }

  private def repartitionAndWrite(df: DataFrame,
                                  tableName: String,
                                  saveMode: SaveMode,
                                  stats: Option[DfStats],
                                  sortByCols: Seq[String] = Seq.empty): Unit = {
    wrapWithCache(s"repartition & write to $tableName", df) {
      logger.info(s"Repartitioning before writing...")
      repartitionAndWriteInternal(df, tableName, saveMode, stats, sortByCols)
    }.get
  }

  private def repartitionAndWriteInternal(df: DataFrame,
                                          tableName: String,
                                          saveMode: SaveMode,
                                          stats: Option[DfStats],
                                          sortByCols: Seq[String] = Seq.empty): Unit = {
    // get row count and table partition count statistics

    val (rowCount: Long, tablePartitionCount: Int) =
      if (df.schema.fieldNames.contains(partitionColumn)) {
        if (stats.isDefined && stats.get.partitionRange.wellDefined) {
          stats.get.count -> stats.get.partitionRange.partitions.length
        } else {
          val result = df.select(count(lit(1)), approx_count_distinct(col(partitionColumn))).head()
          (result.getAs[Long](0), result.getAs[Long](1).toInt)
        }
      } else {
        (df.count(), 1)
      }

    // set to one if tablePartitionCount=0 to avoid division by zero
    val nonZeroTablePartitionCount = if (tablePartitionCount == 0) 1 else tablePartitionCount

    logger.info(s"$rowCount rows requested to be written into table $tableName")
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
      val dailyFileCountEstimate = totalFileCountEstimate / nonZeroTablePartitionCount + 1
      val dailyFileCountBounded =
        math.max(math.min(dailyFileCountEstimate, dailyFileCountUpperBound), dailyFileCountLowerBound)

      val outputParallelism = df.sparkSession.conf
        .getOption(SparkConstants.ChrononOutputParallelismOverride)
        .map(_.toInt)
        .flatMap(value => if (value > 0) Some(value) else None)

      if (outputParallelism.isDefined) {
        logger.info(s"Using custom outputParallelism ${outputParallelism.get}")
      }
      val dailyFileCount = outputParallelism.getOrElse(dailyFileCountBounded)

      // finalized shuffle parallelism
      val shuffleParallelism = dailyFileCount * nonZeroTablePartitionCount
      val saltCol = "random_partition_salt"
      val saltedDf = df.withColumn(saltCol, round(rand() * (dailyFileCount + 1)))

      logger.info(
        s"repartitioning data for table $tableName by $shuffleParallelism spark tasks into $tablePartitionCount table partitions and $dailyFileCount files per partition")
      val (repartitionCols: immutable.Seq[String], partitionSortCols: immutable.Seq[String]) =
        if (df.schema.fieldNames.contains(partitionColumn)) {
          (Seq(partitionColumn, saltCol), Seq(partitionColumn) ++ sortByCols)
        } else { (Seq(saltCol), sortByCols) }
      logger.info(s"Sorting within partitions with cols: $partitionSortCols")
      saltedDf
        .repartition(shuffleParallelism, repartitionCols.map(saltedDf.col): _*)
        .drop(saltCol)
        .sortWithinPartitions(partitionSortCols.map(col): _*)
        .write
        .mode(saveMode)
        .insertInto(tableName)
      logger.info(s"Finished writing to $tableName")
    }
  }

  protected[spark] def getWriteFormat: Format = {
    (useIceberg, maybeWriteFormat) match {
      // if explicitly configured Iceberg - we go with that setting
      case (true, _) => Iceberg
      // else if there is a write format we pick that
      case (false, Some(format)) => format
      // fallback to hive (parquet)
      case (false, None) => Hive
    }
  }

  private def createTableSql(tableName: String,
                             schema: StructType,
                             partitionColumns: Seq[String],
                             tableProperties: Map[String, String],
                             fileFormat: String): String = {
    val fieldDefinitions = schema
      .filterNot(field => partitionColumns.contains(field.name))
      .map(field => s"`${field.name}` ${field.dataType.catalogString}")

    val writeFormat = getWriteFormat

    logger.info(
      s"Choosing format: $writeFormat based on useIceberg flag = $useIceberg and " +
        s"writeFormat: ${sparkSession.conf.getOption("spark.chronon.table_write.format")}")
    val tableTypString = writeFormat.createTableTypeString

    val createFragment =
      s"""CREATE TABLE $tableName (
         |    ${fieldDefinitions.mkString(",\n    ")}
         |) $tableTypString """.stripMargin
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
    val fileFormatString = writeFormat.fileFormatString(fileFormat)
    Seq(createFragment, partitionFragment, fileFormatString, propertiesFragment).mkString("\n")
  }

  def alterTableProperties(tableName: String,
                           properties: Map[String, String],
                           unsetProperties: Seq[String] = Seq()): Unit = {
    // Only SQL api exists for setting TBLPROPERTIES
    val propertiesString = properties
      .map {
        case (key, value) =>
          s"'$key' = '$value'"
      }
      .mkString(", ")
    val query = s"ALTER TABLE $tableName SET TBLPROPERTIES ($propertiesString)"
    sql(query)

    // remove any properties that were set previously during archiving
    if (unsetProperties.nonEmpty) {
      val unsetPropertiesString = unsetProperties.map(s => s"'$s'").mkString(", ")
      val unsetQuery = s"ALTER TABLE $tableName UNSET TBLPROPERTIES IF EXISTS ($unsetPropertiesString)"
      sql(unsetQuery)
    }

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
                     skipFirstHole: Boolean = true): Option[Seq[PartitionRange]] = {

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
            partitions(table, inputTableToSubPartitionFiltersMap.getOrElse(table, Map.empty))
          }
          .map(partitionSpec.shift(_, inputToOutputShift))
      }
      .getOrElse(fillablePartitions)

    val inputMissing = fillablePartitions -- allInputExisting
    val missingPartitions = outputMissing -- inputMissing
    val missingChunks = chunk(missingPartitions)
    logger.info(s"""
               |Unfilled range computation:
               |   Output table: $outputTable
               |   Missing output partitions: ${outputMissing.toSeq.sorted.prettyInline}
               |   Input tables: ${inputTables.getOrElse(Seq("None")).mkString(", ")}
               |   Missing input partitions: ${inputMissing.toSeq.sorted.prettyInline}
               |   Unfilled Partitions: ${missingPartitions.toSeq.sorted.prettyInline}
               |   Unfilled ranges: ${missingChunks.sorted.mkString("")}
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
    logger.info(s"Dropping table with command: $command")
    sql(command)
  }

  def archiveOrDropTableIfExists(tableName: String, timestamp: Option[Instant]): Unit = {
    val archiveTry = Try(archiveTableIfExists(tableName, timestamp))
    archiveTry.failed.foreach { e =>
      logger.info(s"""Fail to archive table ${tableName}
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
      logger.info(s"Archiving table with command: $command")
      sql(command)
      logger.info(s"Setting table property chronon_archived -> true")
      alterTableProperties(finalArchiveTableName, Map(Constants.chrononArchiveFlag -> "true"))
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
      logger.info(s"""
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
      logger.info(s"$tableName doesn't exist, please double check before drop partitions")
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
      logger.info(s"$tableName doesn't exist, please double check before drop partitions")
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
      logger.info(
        s"""Warning. Detected columns that exist in Hive table but not in updated schema. These are ignored in DDL.
           |${excludedFieldsStr.mkString("\n")}
           |""".stripMargin)
    }

    if (expandTableQueryOpt.nonEmpty) {
      sql(expandTableQueryOpt.get)

      // set a flag in table props to indicate that this is a dynamic table
      alterTableProperties(tableName, Map(Constants.ChrononDynamicTable -> true.toString))
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
