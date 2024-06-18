package ai.chronon.quickstart.online

import org.apache.spark.sql.SparkSession


/**
 * Dump mongo collection to hive.
 * Part of log flattening / OOC pattern
 */
object MongoLoggingDumper {
    def main(args: Array[String]): Unit = {
      if (args.length != 2) {
        println("Usage: MongoLoggingDumper <tablename> <uri>")
        sys.exit(1)
      }
      val tableName = args(0)
      val uri = args(1)

      val spark = SparkSession.builder()
        .appName(s"MongoLoggingDumper")
        .config("spark.mongodb.read.connection.uri", uri)
        .getOrCreate()

      val df = spark.read
        .format("mongodb")
        .option("database", Constants.mongoDatabase) // Replace with your MongoDB database name
        .option("collection", Constants.mongoLoggingCollection)
        .load()

      df.createOrReplaceTempView("temp_view")
      df.printSchema()

      val transformedDF = spark.sql(
        s"""
           | SELECT
           |  schemaHash AS schema_hash,
           |  BASE64(keyBytes) AS key_base64,
           |  BASE64(valueBytes) AS value_base64,
           |  atMillis AS ts_millis,
           |  ts AS ts,
           |  joinName AS name,
           |  FROM_UNIXTIME(ts / 1000, 'yyyy-MM-dd') AS ds
           | FROM temp_view
           | """.stripMargin)
      transformedDF.printSchema()

      transformedDF.write
        .partitionBy("ds", "name")
        .format("parquet")
        .mode("overwrite")
        .saveAsTable(tableName)
      spark.stop()
    }
}
