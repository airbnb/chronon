package ai.chronon.spark

import ai.chronon.api
import ai.chronon.api.Extensions.{WindowOps, WindowUtils}
import ai.chronon.api.{Constants, PartitionSpec}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, date_format, lit}
import org.apache.spark.sql.types.{DateType, IntegerType}
import org.slf4j.{Logger, LoggerFactory}

import scala.util.Random

object S3Utils {

  @transient private[this] val logger: Logger = LoggerFactory.getLogger(this.getClass)

  private val S3 = "s3://"
  private val DS_NODASH = "{{ds_nodash}}"
  private val DS_NODASH_YESTERDAY = "{{yesterday_ds_nodash}}"

  // With Chronon we write to partition ds - 1 and this is what gets passed to endDate.
  // The most common convention in Dataland is for datasources to write to version ds.
  // To be consistent with this the date substitution logic uses (endDate + 1) = ds for ds_nodash
  // and endDate = (ds - 1) for yesterday_ds_nodash.
  def updateS3Prefix(originalS3Prefix: String, endDate: String): String =
    if (originalS3Prefix.contains(DS_NODASH)) {
      val endDatePlusOneDay =
        PartitionSpec(format = "yyyyMMdd", spanMillis = WindowUtils.Day.millis)
          .shift(endDate, 1)
      logger.info(s"Replacing $DS_NODASH in $originalS3Prefix with $endDatePlusOneDay")
      originalS3Prefix.replace(DS_NODASH, endDatePlusOneDay)
    } else if (originalS3Prefix.contains(DS_NODASH_YESTERDAY)) {
      logger.info(
        s"Replacing $DS_NODASH_YESTERDAY in $originalS3Prefix with $endDate"
      )
      originalS3Prefix.replace(DS_NODASH_YESTERDAY, endDate)
    } else originalS3Prefix


  // Chronon doesn't natively support reading S3 prefix inputs so for each s3 prefix input we create
  // a temp view over the s3 prefix and then refer to the temp view in the Chronon config.
  // This currently only works for unpartitioned inputs (either a full snapshot or a single partition of
  // a partitioned dataset).
  def readAndUpdateS3PrefixesForSource(source: api.Source, endDate: String)(
    implicit spark: SparkSession
  ): api.Source = {
    if (source.isSetEvents && source.getEvents.table.startsWith(S3)) {
      logger.info(s"Converting the following S3 Source to a temp table: ${source.getEvents.table}")
      val s3Prefix = updateS3Prefix(source.getEvents.table, endDate)
      logger.info(s"Creating a temp table from the following S3 prefix: ${s3Prefix}")
      val updatedTableName = tableNameFromS3Prefix(s3Prefix)
      spark.read.parquet(s3Prefix).createTempView(updatedTableName)
      source.getEvents.table = updatedTableName
    }
    if (source.isSetEntities && source.getEntities.snapshotTable.startsWith(S3)) {
      logger.info(s"Converting the following S3 Source to a temp table: ${source.getEntities.snapshotTable}")
      val s3Prefix = updateS3Prefix(source.getEvents.table, endDate)
      logger.info(s"Creating a temp table from the following S3 prefix: ${s3Prefix}")
      val updatedTableName = tableNameFromS3Prefix(s3Prefix)
      val df = spark.read.parquet(s3Prefix)
      if (df.columns.contains(Constants.PartitionColumn)) {
        // When defining a table from an S3 prefix, sometimes the partition column is not the correct type that
        // Chronon expects. If it is a DateType, convert it to a string with the correct format.
        (df.schema.fields
          .find(field => field.name == Constants.PartitionColumn)
          .get
          .dataType match {
          case _: DateType =>
            logger.info(
              s"converting ${Constants.PartitionColumn} partition column in $updatedTableName from DateType to string type with format ${Constants.constantNameProvider.get.Partition.format} for $s3Prefix"
            )
            df.withColumn(
              Constants.PartitionColumn,
              date_format(
                col(Constants.PartitionColumn),
                Constants.constantNameProvider.get.Partition.format
              )
            )
          case _: IntegerType =>
            logger.info(
              s"converting ${Constants.PartitionColumn} partition column in $updatedTableName from IntegerType to string type for $s3Prefix"
            )
            df.withColumn(
              Constants.PartitionColumn,
              col(Constants.PartitionColumn).cast("string")
            )
          case _ => df
        }).createTempView(updatedTableName)
      } else {
        logger.info(
          s"Adding ${Constants.PartitionColumn} partition column to $updatedTableName populated with value $endDate for $s3Prefix"
        )
        df.withColumn(Constants.PartitionColumn, lit(endDate)).createTempView(updatedTableName)
      }
      source.getEntities.snapshotTable = updatedTableName
    }
    source
  }

  def readAndUpdateS3PrefixesForJoin(join: api.Join, endDate: String)(
    implicit spark: SparkSession
  ): api.Join = {
    join.setLeft(readAndUpdateS3PrefixesForSource(join.left, endDate))
    for (i <- 0 until join.joinParts.size) {
      join.joinParts.set(i, readAndUpdateS3PrefixesForJoinPart(join.joinParts.get(i), endDate))
    }
    join
  }

  def readAndUpdateS3PrefixesForJoinPart(joinPart: api.JoinPart, endDate: String)(
    implicit spark: SparkSession
  ): api.JoinPart = {
    joinPart.groupBy = readAndUpdateS3PrefixesForGroupBy(joinPart.groupBy, endDate)
    joinPart
  }

  def readAndUpdateS3PrefixesForGroupBy(groupBy: api.GroupBy, endDate: String)(
    implicit spark: SparkSession
  ): api.GroupBy = {
    for (i <- 0 until groupBy.sources.size) {
      groupBy.sources.set(i, readAndUpdateS3PrefixesForSource(groupBy.sources.get(i), endDate))
    }
    groupBy
  }

  def tableNameFromS3Prefix(s3Prefix: String): String = {
    // Add a random string to the end of the table name because multiple Source's could
    // point to the same s3 prefix.
    val randomString = Random.alphanumeric.take(10).mkString
    // Replace any non-word character with an underscore.
    val viewBaseName = s3Prefix.stripPrefix(S3).replaceAll("\\W", "_")
    viewBaseName.substring(0, Math.min(100, viewBaseName.length)) + "_" + randomString
  }

}
