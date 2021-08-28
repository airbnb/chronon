package ai.zipline.spark

import ai.zipline.api.Constants
import org.apache.spark.sql.functions.{rand, round}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
case class TableUtils(sparkSession: SparkSession) {

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
    sparkSession.sqlContext
      .sql(s"SHOW PARTITIONS $tableName")
      .collect()
      .flatMap { row => parsePartition(row.getString(0)).get(Constants.PartitionColumn) }
  }

  def lastAvailablePartition(tableName: String): Option[String] =
    partitions(tableName).reduceOption(Ordering[String].max)

  def firstUnavailablePartition(tableName: String): Option[String] =
    lastAvailablePartition(tableName).map(Constants.Partition.after)

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
      sql(createTableSql(tableName, dfRearranged.schema, partitionColumns, tableProperties, fileFormat))
    } else {
      if (tableProperties != null && tableProperties.nonEmpty) {
        sql(alterTablePropertiesSql(tableName, tableProperties))
      }
    }

    repartitionAndWrite(dfRearranged, tableName, saveMode)
  }

  def sql(query: String): DataFrame = {
    println(s"\n----[Running query]----\n$query\n----[End of Query]----\n")
    val df = sparkSession.sql(query)
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
    // repartition data based on a random salt to leverage full parallelism
    val saltCol = "random_partition_salt"
    val saltedDf = df.withColumn(saltCol, round(rand() * 1000000))
    val repartitionCols =
      if (df.schema.fieldNames.contains(Constants.PartitionColumn)) {
        Seq(Constants.PartitionColumn, saltCol)
      } else { Seq(saltCol) }
    saltedDf
      .repartition(10000, repartitionCols.map(saltedDf.col): _*)
      .drop(saltCol)
      .write
      .mode(saveMode)
      .insertInto(tableName)
    println(s"Finished writing to $tableName")
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

  // logic for resuming computation from a previous job
  // applicable to join, joinPart, groupBy, daily_cache
  // TODO: Log each step - to make it easy to follow the range inference logic
  def unfilledRange(outputTable: String,
                    partitionRange: PartitionRange,
                    inputTables: Seq[String] = Seq.empty[String]): PartitionRange = {
    val inputStart = inputTables
      .flatMap(firstAvailablePartition)
      .reduceLeftOption(Ordering[String].min)
    val resumePartition = firstUnavailablePartition(outputTable)
    val effectiveStart = (inputStart ++ resumePartition ++ Option(partitionRange.start))
      .reduceLeftOption(Ordering[String].max)
    val result = PartitionRange(effectiveStart.orNull, partitionRange.end)
    // Using seconds rather than milis will result in bad dates close to start of epoch, Choosing 1980 as an arbitrary cutoff date, can be modified
    println(s"""
               |Unfilled range computation:
               |   Input start: $inputStart
               |   From tables: $inputTables
               |   first unavailable: $resumePartition
               |   effective start: $effectiveStart
               |   partition range: $partitionRange
               |   result: $result
               |""".stripMargin)
    assert(
      Option(result.start).map(_ > "1980").getOrElse(true) && Option(result.end).map(_ > "1980").getOrElse(true),
      s"Unfilled range timestamps invalid for ${outputTable}: ${result.start}, ${result.end} consider applying * 1000 to your timestamp to convert to millis"
    )
    result
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

  def dropPartitionsAfterHole(inputTable: String,
                              outputTable: String,
                              partitionRange: PartitionRange): Option[String] = {

    def partitionsInRange(table: String): Set[String] = {
      partitions(table)
        .filter(ds => ds >= partitionRange.start && ds <= partitionRange.end)
        .toSet
    }

    val inputPartitions = partitionsInRange(inputTable)
    val outputPartitions = partitionsInRange(outputTable)
    val earliestHoleOpt = (inputPartitions -- outputPartitions)
      .reduceLeftOption(Ordering[String].min)
    val maxOutputPartitionOpt = outputPartitions.reduceLeftOption(Ordering[String].max)

    for (
      earliestHole <- earliestHoleOpt;
      maxOutputPartition <- maxOutputPartitionOpt if maxOutputPartition > earliestHole
    ) {
      println(s"""
                 |Earliest hole at $earliestHole
                 |In output table $outputTable (max partition: $maxOutputPartition)
                 |Compared to input table $inputTable
                 |Dropping table partitions in range: [$earliestHole - $maxOutputPartition] 
          """.stripMargin)
      dropPartitionRange(outputTable, earliestHole, maxOutputPartition)
    }

    earliestHoleOpt
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
