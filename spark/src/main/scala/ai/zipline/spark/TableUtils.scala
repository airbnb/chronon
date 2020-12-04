package ai.zipline.spark

import ai.zipline.api.Config.{Constants, PartitionSpec}
import ai.zipline.api.QueryUtils
import ai.zipline.spark.Extensions._
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

case class TableUtils(sparkSession: SparkSession) {

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

  def sql(query: String): DataFrame = {
    println(s"Running spark sql: \n$query\n ----End of Query----")
    sparkSession.sql(query)
  }

  def partitions(tableName: String): Seq[String] = {
    if (!sparkSession.catalog.tableExists(tableName)) return Seq.empty[String]
    sparkSession.sqlContext
      .sql(s"SHOW PARTITIONS $tableName")
      .collect()
      .flatMap { row => parsePartition(row.getString(0)).get(Constants.PartitionColumn) }
  }

  def firstUnavailablePartition(tableName: String): Option[String] =
    partitions(tableName)
      .reduceOption(Ordering[String].max)
      .map(Constants.Partition.after)

  def firstAvailablePartition(tableName: String): Option[String] =
    partitions(tableName)
      .reduceOption(Ordering[String].min)

  def insertPartitions(df: DataFrame,
                       partitionColumns: Seq[String],
                       tableName: String,
                       numPartitions: Int,
                       filesPerPartition: Int = 10,
                       saveMode: SaveMode = SaveMode.Overwrite): Unit = {

    // partitions to the last
    val dfRearranged: DataFrame = if (!df.columns.endsWith(partitionColumns)) {
      val colOrder = df.columns.diff(partitionColumns) ++ partitionColumns
      df.select(colOrder.map(df.col): _*)
    } else {
      df
    }
    // this does a full re-shuffle
    val rePartitioned: DataFrame = dfRearranged.repartition(
      numPartitions * filesPerPartition,
      partitionColumns.map(df.col): _*
    )

    // write out
    if (df.sparkSession.catalog.tableExists(tableName)) {
      rePartitioned.write.mode(saveMode).insertInto(tableName)
    } else {
      rePartitioned.write.mode(saveMode).partitionBy(partitionColumns: _*).saveAsTable(tableName)
    }
  }

  // logic for resuming computation from a previous job
  // applicable to join, joinPart, groupBy, daily_cache
  // TODO: Log each step - to make it easy to follow the range inference logic
  def fillableRange(outputTable: String,
                    partitionRange: PartitionRange,
                    inputTables: Seq[String] = Seq.empty[String]): PartitionRange = {
    val inputStart = inputTables
      .flatMap(firstAvailablePartition)
      .reduceLeftOption(Ordering[String].min)
    val resumePartition = firstUnavailablePartition(outputTable)
    val effectiveStart = (inputStart ++ resumePartition ++ Option(partitionRange.start))
      .reduceLeftOption(Ordering[String].max)
    val result = PartitionRange(effectiveStart.orNull, partitionRange.end)
    assert(result.valid, s"Invalid partition range for left side: $result")
    result
  }
}
