package ai.chronon.spark

import ai.chronon.spark.SampleHelper.getPriorRunManifestMetadata
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.slf4j.LoggerFactory



/*
This class works in conjunction with Sample.scala
Given a path to the output of the Sample job (which writes data to local disk), this picks up the files
and creates tables with them in a local data warehouse.
It maintains its own manifest file to track what tables need updating, to ensure that we spend as little time
as possible loading tables in between sampled data runs.
 */
object SampleDataLoader {

  @transient lazy val logger = LoggerFactory.getLogger(getClass)
  private val SEMANTIC_HASH_REGEX = "semantic_hash=(-?\\d+)".r

  private def loadTable(directory: String, tableName: String, session: SparkSession, semanticHash: Int): Unit = {
    val df: DataFrame = session.read.parquet(s"$directory/$tableName")
    session.sql(s"DROP TABLE IF EXISTS $tableName")
    session.sql(s"CREATE DATABASE IF NOT EXISTS ${tableName.split("\\.").head}")
    df.write.partitionBy("ds").saveAsTable(tableName)
    session.sql(s"ALTER TABLE $tableName SET TBLPROPERTIES ('semantic_hash' = '$semanticHash')")
  }

  private def checkLocalWarehouse(tableUtils: TableUtils): Unit = {

    print(s"\n\n\n  =========================== \n\n ${tableUtils.dataWarehouseDir.get} \n ${tableUtils.jobMode} \n ${tableUtils.hiveMetastore}   \n\n\n")
    // TODO: Is this the best way to run this safety check? Alternatively, could take the intended
    // path in as an argument and check that it matches the spark setting.
    if (tableUtils.dataWarehouseDir.get.startsWith("hdfs") || tableUtils.dataWarehouseDir.get.startsWith("s3") || !tableUtils.jobMode.startsWith("local") ) {
      throw new RuntimeException(
        """
          |SampleDataLoader is only meant to be run with a local spark directory. This is a safety precaution
          |As this module drops and overwrites tables that likely share a name with production tables.
          |Please be sure to set a local data warehouse path when calling the Chronon sample job. This
          |Can be done with the `spark.sql.warehouse.dir` config option.""".stripMargin
      )
    }
  }

  def loadAllData(sampleDirectory: String, tableUtils: TableUtils): Unit = {
    // Important: Always check that we're in local mode before doing anything
    checkLocalWarehouse(tableUtils)

    val sparkSession = tableUtils.sparkSession

    getPriorRunManifestMetadata(sampleDirectory).foreach { case(tableName, hash) =>
      logger.info(s"Checking $tableName")
      val resampleTable = if (tableUtils.tableExists(tableName)) {
        logger.info(s"Found existing table $tableName.")
        val tableDesc = sparkSession.sql(s"DESCRIBE TABLE EXTENDED $tableName")
        tableDesc.show(10)
        val tableMetadata = tableDesc.filter(col("col_name") === "Table Properties").select("data_type").collect().head.getAs[String](0)

        val semanticHashValue = SEMANTIC_HASH_REGEX.findFirstMatchIn(tableMetadata) match {
          case Some(matchFound) => matchFound.group(1)
          case None =>
            logger.error(s"Failed to parse semantic hash from $tableName. Table metadata: $tableMetadata")
            ""
        }

        // Reload table when the data hash changed
        semanticHashValue != hash.toString
      } else {
        true
      }

      if (resampleTable) {
        logger.info(s"Loading data for table $tableName")
        loadTable(sampleDirectory, tableName, sparkSession, hash)
      } else {
        logger.info(s"$tableName already up to date based on semantic hash.")
      }
    }
  }
}
