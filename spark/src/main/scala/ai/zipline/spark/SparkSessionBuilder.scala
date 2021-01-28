package ai.zipline.spark

import java.io.File

import org.apache.spark.sql.SparkSession

import scala.reflect.io.Directory
import java.util.logging.{Level, Logger}

object SparkSessionBuilder {

  val warehouseDir = new File("spark-warehouse")
  val metastoreDb = new File("metastore_db")

  def build(name: String, local: Boolean): SparkSession = {
    if (local) {
      //required to run spark locally with hive support enabled - for sbt test
      System.setSecurityManager(null)
    }

    val baseBuilder = SparkSession
      .builder()
      .appName(name)
      .config("spark.sql.session.timeZone", "UTC")
      //otherwise overwrite will delete ALL partitions, not just the ones it touches
      .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
      .config("hive.exec.dynamic.partition", "true")
      .config("hive.exec.dynamic.partition.mode", "nonstrict")
      // Otherwise files left from deleting the table with the same name result in test failures
      .config("spark.sql.legacy.allowCreatingManagedTableUsingNonemptyLocation", "true")
      .config("spark.sql.warehouse.dir", warehouseDir.getAbsolutePath)
      .config("spark.sql.catalogImplementation", "hive")
      .enableHiveSupport()

    val builder = if (local) {
      baseBuilder
      // use all threads - or the tests will be slow
        .master("local[*]")
    } else {
      // hive jars need to be available on classpath - no needed for local testing
      baseBuilder
    }
    val spark = builder.getOrCreate()
    // disable log spam
    spark.sparkContext.setLogLevel("ERROR")
    Logger.getLogger("parquet.hadoop").setLevel(java.util.logging.Level.SEVERE)
    spark
  }

  private def cleanUp(file: File): Unit = {
    if (file.exists() && file.isDirectory) {
      val directory = new Directory(file)
      directory.deleteRecursively()
    }
  }

  // remove the old warehouse folders
  def cleanData(): Unit = {
    cleanUp(warehouseDir)
    cleanUp(metastoreDb)
  }

}
