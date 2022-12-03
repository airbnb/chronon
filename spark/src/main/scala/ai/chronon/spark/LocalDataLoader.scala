package ai.chronon.spark

import ai.chronon.spark.Extensions.StructTypeOps
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, unix_timestamp}
import org.apache.spark.sql.types.StringType

import java.io.File

object LocalDataLoader {
  def writeTableFromCsv(file: File, tableName: String, session: SparkSession): Unit = {
    val extension = file.getName.split("\\.").last
    assert(Seq("csv", "json").contains(extension), s"Invalid local file to load: ${file.getPath}")
    var reader = session.read
      .format(extension)
      .option("inferSchema", "true")
    if (extension == "csv") {
      reader = reader.option("header", "true")
    }
    var csvDf = reader.load(file.getPath).toDF
    val schema = csvDf.schema

    // for readability we want to allow timestamp specified as string
    // in such cases we assume the format is 'yyyy-MM-dd HH:mm:ss'
    if (schema.fieldNames.contains("ts") && schema(schema.fieldIndex("ts")).dataType == StringType) {
      csvDf = csvDf
        .withColumnRenamed("ts", "ts_string")
        .withColumn("ts", unix_timestamp(col("ts_string")) * 1000)
        .drop("ts_string")
    }

    println(s"Loading data from ${file.getPath} into $tableName. Sample data and schema shown below")
    csvDf.show(100)
    println(csvDf.schema.pretty)

    if (!session.catalog.tableExists(tableName)) {
      if (csvDf.schema.map(_.name).contains("ds")) {
        csvDf.write.partitionBy("ds").saveAsTable(tableName)
      } else {
        csvDf.write.saveAsTable(tableName)
      }
    }
  }

  def loadData(f: File, session: SparkSession, namespaces: Seq[String] = Seq.empty[String]): Unit = {
    val nsFields = if (namespaces.isEmpty) { Seq("default") }
    else { namespaces }
    if (f.isDirectory) {
      f.listFiles.foreach(loadData(_, session, namespaces :+ f.getName))
    } else if (f.isFile && f.getName.endsWith(".csv")) {
      val splits = f.getName.split("\\.")
      val (ns, table) = if (splits.size == 2) { nsFields -> splits(0) }
      else { (nsFields :+ splits(0)) -> splits(1) }
      writeTableFromCsv(f, ns.mkString("_") + "." + table, session)
    } else {
      println(s"Not going to load data from ${f.getPath}")
    }
  }
}
