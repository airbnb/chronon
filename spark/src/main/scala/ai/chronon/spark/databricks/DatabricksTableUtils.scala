package ai.chronon.spark.databricks

import ai.chronon.spark.TableUtils

import org.apache.spark.sql.SparkSession

/**
 * DatabricksTableUtils is the table utils class used in our Databricks integration.
 * If you need any specific functionality pertaining to reads/writes for your Databricks setup,
 * you can implement it here.
 *
 * @param sparkSession The Spark session used for table operations.
 */
case class DatabricksTableUtils(sparkSession: SparkSession) extends TableUtils(sparkSession)
