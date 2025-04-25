package ai.chronon.spark.databricks

import ai.chronon.api.Constants
import ai.chronon.spark.BaseTableUtils
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Column, SparkSession}


/**
 * DatabricksTableUtils is the table utils class used in our Databricks integration.
 * If you need any specific functionality pertaining to reads/writes for your Databricks setup,
 * you can implement it here.
 *
 * @param sparkSession The Spark session used for table operations.
 */
case class DatabricksTableUtils (override val sparkSession: SparkSession) extends BaseTableUtils