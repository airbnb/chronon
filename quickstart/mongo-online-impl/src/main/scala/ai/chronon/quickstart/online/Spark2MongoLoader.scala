package ai.chronon.quickstart.online
import org.apache.spark.sql.SparkSession

object Spark2MongoLoader {
  def main(args: Array[String]): Unit = {
    if (args.length != 2) {
      println("Usage: TableDataLoader <table_name> <mongoUri>")
      sys.exit(1)
    }

    val tableName = args(0)
    val batch_dataset = tableName.stripSuffix("_upload").split("\\.").lastOption.getOrElse(tableName).toUpperCase + "_BATCH"
    val uri = args(1)

    val spark = SparkSession.builder()
      .appName(s"Spark2MongoLoader-${tableName}")
      .config("spark.mongodb.write.connection.uri", uri)
      .getOrCreate()

    val ts = System.currentTimeMillis()

    val df = spark.sql(s"""
    | SELECT
    |   ${Constants.tableKey} AS ${Constants.mongoKey},
    |   ${Constants.tableValue} as ${Constants.mongoValue},
    |   CAST($ts AS BIGINT) as ${Constants.mongoTs}
    | FROM $tableName""".stripMargin)

    df.write
      .format("mongodb")
      .mode("overwrite")
      .option("database", Constants.mongoDatabase)
      .option("collection", batch_dataset)
      .save()

    spark.stop()
  }
}