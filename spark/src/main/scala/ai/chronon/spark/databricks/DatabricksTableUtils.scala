package ai.chronon.spark.databricks

import ai.chronon.api.Constants
import ai.chronon.spark.BaseTableUtils
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Column, SparkSession}

case class DatabricksTableUtils (override val sparkSession: SparkSession) extends BaseTableUtils {

  private def isTempView(tableName: String): Boolean =
    sparkSession.sessionState.catalog.isTempView(tableName.split('.'))

  override def createTableSql(tableName: String, schema: StructType, partitionColumns: Seq[String], tableProperties: Map[String, String], fileFormat: String): String = {

    // Side effect: Creates the table in iceberg and then confirms the table creation was successfully by issuing a SHOW CREATE TABLE.
    val partitionSparkCols: Seq[Column] = partitionColumns.map(col => org.apache.spark.sql.functions.col(col))

    val emptyDf = sparkSession.createDataFrame(sparkSession.sparkContext.emptyRDD[org.apache.spark.sql.Row], schema)

    val createTableWriter = {
      if(Option(tableProperties).exists(_.nonEmpty)) {
        tableProperties
          .foldLeft(emptyDf.writeTo(tableName).using("iceberg"))((createTableWriter, tblProperty) => createTableWriter.tableProperty(tblProperty._1, tblProperty._2))
      }
      else {
        emptyDf.writeTo(tableName).using("iceberg")
      }
    }


    partitionSparkCols match {
      case Seq() =>
        logger.info(s"Creating table $tableName without partitioning")
        createTableWriter.create()
      case Seq(head) =>
        logger.info(s"Creating table $tableName partitioned by $head")
        createTableWriter.partitionedBy(head).create()
      case head +: tail =>
        logger.info(s"Creating table $tableName partitioned by $head and $tail")
        createTableWriter.partitionedBy(head, tail: _*).create()
    }

    s"SHOW CREATE TABLE $tableName" // Confirm table creation in subsequent call
  }

  override def getIcebergPartitions(tableName: String): Seq[String] = {
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
    } else if (partitionsDf.schema(index).dataType.asInstanceOf[StructType].fieldNames.contains("locality_zone")) {
      partitionsDf
        // TODO(FCOMP-2242) We should factor out a provider for getting Iceberg partitions
        //  so we can inject a Stripe-specific one that takes into account locality_zone
        .select(s"partition.${partitionColumn}")
        .where("partition.locality_zone == 'DEFAULT'")
        .collect()
        .map(_.getString(0))
        .toSeq
    }
    else {
      partitionsDf
        .select(s"partition.${partitionColumn}")
        .collect()
        .map(_.getString(0))
        .toSeq
    }
  }

  override def partitions(
                           tableName: String,
                           subPartitionsFilter: Map[String, String] = Map.empty,
                           partitionColumnOverride: String = Constants.PartitionColumn
                         ): Seq[String] =
    // This is to support s3 prefix inputs.
    if (isTempView(tableName)) {
      if (subPartitionsFilter.nonEmpty) {
        throw new NotImplementedError(
          s"partitions cannot be called with tableName ${tableName} subPartitionsFilter ${subPartitionsFilter} because subPartitionsFilter is not supported on tempViews yet."
        )
      }
      // If the table is a temp view, fallback to querying for the distinct partition columns because SHOW PARTITIONS
      // doesn't work on temp views. This can be inefficient for large tables.
      logger.info(
        s"Selecting partitions for temp view table tableName=$tableName, " +
          s"partitionColumnOverride=$partitionColumnOverride."
      )
      val outputPartitionsRowsDf = sql(
        s"select distinct ${partitionColumnOverride} from $tableName"
      )
      val outputPartitionsRows = outputPartitionsRowsDf.collect()
      // Users have ran into issues where the partition column is not a string, so add logging to facilitate debug.
      logger.info(
        s"Found ${outputPartitionsRows.length} partitions for temp view table tableName=$tableName. The partition schema is ${outputPartitionsRowsDf.schema}."
      )

      val outputPartitionsRowsDistinct = outputPartitionsRows.map(_.getString(0)).distinct
      logger.info(
        s"Converted ${outputPartitionsRowsDistinct.length} distinct partitions for temp view table to String. " +
          s"tableName=$tableName."
      )

      outputPartitionsRowsDistinct
    } else {
      super.partitions(tableName, subPartitionsFilter, partitionColumnOverride)
    }



}