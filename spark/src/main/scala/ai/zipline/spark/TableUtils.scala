package ai.zipline.spark

import ai.zipline.api.Constants
import org.apache.spark.sql.catalyst.plans.logical.{Filter, Project}
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

  // Given a table and a query extract the schema of the columns involved as input.
  def getColumnsFromQuery(query: String): Seq[String] = {
    val parser = sparkSession.sessionState.sqlParser
    val logicalPlan = parser.parsePlan(query)
    logicalPlan.collect {
      case p: Project =>
        p.projectList.map(p => parser.parseExpression(p.sql).references.map(attr => attr.name)).flatten
      case f: Filter => f.condition.references.map(attr => attr.name)
    }.flatten
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
    val rowCount = df.count()
    println(s"$rowCount rows requested to be written into table $tableName")
    if (rowCount > 0) {
      // at-least a million rows per partition - or there will be too many files.
      val rddPartitionCount = math.min(5000, math.ceil(rowCount / 1000000.0).toInt)
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
    val fillablePartitions = partitionRange.partitions.toSet
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
