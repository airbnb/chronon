package ai.chronon.spark

import ai.chronon.api.Constants
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Column, SparkSession}

case class DatabricksTableUtils (override val sparkSession: SparkSession) extends BaseTableUtils {

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
        println(s"Creating table $tableName without partitioning")
        createTableWriter.create()
      case Seq(head) =>
        println(s"Creating table $tableName partitioned by $head")
        createTableWriter.partitionedBy(head).create()
      case head +: tail =>
        println(s"Creating table $tableName partitioned by $head and $tail")
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


}