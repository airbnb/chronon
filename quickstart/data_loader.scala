// Create tables from csv and load them into spark.
import java.io.File
import org.apache.spark.sql.SparkSession

val spark = SparkSession.builder()
  .appName("CSVs to Hive Tables")
  .config("spark.sql.warehouse.dir", "/tmp/chronon/spark-warehouse") // Set Hive warehouse location
  .enableHiveSupport() // Enable Hive support
  .getOrCreate()

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

  // Create a table name based on the file name (without the extension)
  val tableName = s"data.${fileName.split('.')(0)}"

  // Save DataFrame as a Hive table
  df.write.mode("overwrite").saveAsTable(tableName)
}

// Query one of the tables
spark.sql("SHOW TABLES").show()

// Stop Spark session
spark.stop()
