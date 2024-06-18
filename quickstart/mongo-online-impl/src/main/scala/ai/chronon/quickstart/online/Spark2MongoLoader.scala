package ai.chronon.quickstart.online
import org.apache.spark.sql.SparkSession
import ai.chronon.api.{Constants => ApiConstants}

object Spark2MongoLoader {
  def main(args: Array[String]): Unit = {
    if (args.length != 2) {
      println("Usage: TableDataLoader <table_name> <mongoUri>")
      sys.exit(1)
    }

    val tableName = args(0)
    val dataset = tableName match {
      case tableName if tableName.endsWith("_logged_daily_stats_upload") => ApiConstants.LogStatsBatchDataset
      case tableName if tableName.endsWith("_daily_stats_upload") => ApiConstants.StatsBatchDataset
      case tableName if tableName.endsWith("_consistency_upload") => ApiConstants.ConsistencyMetricsDataset
      case tableName if tableName.endsWith("_upload") => tableName.stripSuffix("_upload").split("\\.").lastOption.getOrElse(tableName).toUpperCase + "_BATCH"
      case _ => tableName.toUpperCase + "_BATCH"
    }
    val uri = args(1)

    val spark = SparkSession.builder()
      .appName(s"Spark2MongoLoader-${tableName}")
      .config("spark.mongodb.write.connection.uri", uri)
      .getOrCreate()

    val baseDf = spark.read.table(tableName)
    val timeColumn = if (baseDf.columns.contains("ts")) "ts" else "UNIX_TIMESTAMP(DATE_ADD(ds, 0)) * 1000"

    val df = spark.sql(s"""
    | SELECT
    |   ${Constants.tableKey} AS ${Constants.mongoKey},
    |   ${Constants.tableValue} AS ${Constants.mongoValue},
    |   $timeColumn AS ${Constants.mongoTs}
    | FROM $tableName""".stripMargin)
    df.show()
    df.write
      .format("mongodb")
      .mode("overwrite")
      .option("database", Constants.mongoDatabase)
      .option("collection", dataset)
      .save()
    spark.stop()
  }
}