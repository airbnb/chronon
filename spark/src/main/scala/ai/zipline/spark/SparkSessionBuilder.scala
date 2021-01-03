package ai.zipline.spark

import java.io.File

import org.apache.spark.sql.SparkSession

import scala.reflect.io.Directory

object SparkSessionBuilder {

  def build(name: String, local: Boolean): SparkSession = {

    val baseBuilder = SparkSession
      .builder()
      .appName(name)
      .config("spark.sql.session.timeZone", "UTC")
      //otherwise overwrite will delete ALL partitions, not just the ones it touches
      .config("spark.sql.sources.partitionOverwriteMode", "dynamic")

    val builder = if (local) {
      baseBuilder
      // use all threads - or the tests will be slow
        .master("local[*]")
    } else {
      // hive jars need to be available on classpath - no needed for local testing
      baseBuilder.enableHiveSupport()
    }
    val spark = builder.getOrCreate()
    // disable log spam
    spark.sparkContext.setLogLevel("ERROR")
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
    println("Cleaning left over spark data directories")
    val warehouseDir = new File("spark-warehouse")
    val metastoreDb = new File("metastore_db")
    cleanUp(warehouseDir)
    cleanUp(metastoreDb)
  }

}
