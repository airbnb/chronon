// Create tables from csv and load them into spark.
import java.io.File
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._


val spark = SparkSession.builder()
  .appName("CSVs to Hive Tables")
  .enableHiveSupport() // Enable Hive support
  .getOrCreate()

spark.sql("CREATE DATABASE IF NOT EXISTS data;").show()
// Directory containing CSV files
val folderPath = "/srv/chronon/data/"

val folder = new File(folderPath)

// List all files in the directory
val files = folder.listFiles.filter(_.isFile).filter(_.getName.endsWith(".csv"))

// Process each CSV file
files.foreach { file =>
  val fileName = file.getName

  // Load CSV file into DataFrame
  val df = spark.read.option("header", "true").csv(s"$folderPath/$fileName")
  // Set schema: ts -> long, _price -> long, _amt -> long, anything else -> string
  // // Get the existing column names
  val columns = df.columns

  // Define the schema based on column names
  val customSchema = StructType(
    columns.map { columnName =>
      val dataType = columnName match {
        case "ts" => LongType
        case name if name.endsWith("_price") || name.endsWith("_amt") => LongType
        case _ => StringType
      }
      StructField(columnName, dataType, nullable = true)
    }
  )
  // Create a table name based on the file name (without the extension)
  val tableName = s"data.${fileName.split('.')(0)}"

  // Save DataFrame as a Hive table
  df.write.partitionBy("ds").mode("overwrite").saveAsTable(tableName)
}

// Query one of the tables
spark.sql("SHOW TABLES IN DATA").show()

// Stop Spark session
spark.stop()
