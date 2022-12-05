package ai.chronon.spark

import ai.chronon.spark.Extensions.StructTypeOps
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.util.{FailFastMode, PermissiveMode}
import org.apache.spark.sql.functions.{col, unix_timestamp}
import org.apache.spark.sql.types.StringType

import java.io.File

object LocalDataLoader {
  def writeTableFromFile(file: File, tableName: String, session: SparkSession): Unit = {
    val extension = file.getName.split("\\.").last
    if (!Seq("csv", "json", "jsonl").contains(extension)) {
      println(s"Unable to load file due to invalid extension from file: ${file.getPath}")
      return
    }

    val reader = session.read
      .option("inferSchema", "true")
      .option("mode", FailFastMode.name)
    var df = if (extension == "csv") {
      reader.option("header", true).csv(file.getPath)
    } else {
      reader.option("multiLine", extension == "json").option("allowComments", true).json(file.getPath)
    }
    val schema = df.schema

    // for readability we want to allow timestamp specified as string
    // in such cases we assume the format is 'yyyy-MM-dd HH:mm:ss'
    if (schema.fieldNames.contains("ts") && schema(schema.fieldIndex("ts")).dataType == StringType) {
      df = df
        .withColumnRenamed("ts", "ts_string")
        .withColumn("ts", unix_timestamp(col("ts_string")) * 1000)
        .drop("ts_string")
    }

    println(s"Loading data from ${file.getPath} into $tableName. Sample data and schema shown below")
    df.show(100)
    println(df.schema.pretty)

    if (!session.catalog.tableExists(tableName)) {
      if (df.schema.map(_.name).contains("ds")) {
        df.write.partitionBy("ds").saveAsTable(tableName)
      } else {
        df.write.saveAsTable(tableName)
      }
    }
  }

  def loadData(f: File, session: SparkSession, namespaces: Seq[String] = Seq.empty[String]): Unit = {
    assert(f.exists(), s"Non existent file: ${f.getPath}")
    val nsFields = if (namespaces.isEmpty) { Seq("default") }
    else { namespaces }
    val splits = f.getName.split("\\.")
    if (f.isDirectory) {
      f.listFiles.foreach(loadData(_, session, namespaces :+ f.getName))
    } else {
      val (ns, table) = if (splits.size == 2) { nsFields -> splits(0) }
      else { (nsFields :+ splits(0)) -> splits(1) }
      writeTableFromFile(f, ns.mkString("_") + "." + table, session)
    }
  }
}
