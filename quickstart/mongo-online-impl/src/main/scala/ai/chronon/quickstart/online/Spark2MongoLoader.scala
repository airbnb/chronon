package ai.chronon.quickstart.online
import org.apache.spark.sql.SparkSession

object Spark2MongoLoader {
  def main(args: Array[String]): Unit = {
    if (args.length != 2) {
      println("Usage: TableDataLoader <table_name> <mongoUri>")
      sys.exit(1)
    }

    val tableName = args(0)
    val uri = args(1)

    val spark = SparkSession.builder()
      .appName("TableDataLoader")
      .config("spark.mongodb.output.uri", uri)
      .getOrCreate()

    val ts = System.currentTimeMillis()

    val df = spark.sql(s"SELECT key_bytes as keyBytes, value_bytes as valueBytes, $ts as ts FROM $tableName")

    df.write
      .format("com.mongodb.spark.sql.DefaultSource")
      .mode("overwrite")
      .save()

    spark.stop()
  }
}