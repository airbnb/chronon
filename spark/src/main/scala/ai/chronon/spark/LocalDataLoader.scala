package ai.chronon.spark

import ai.chronon.spark.Extensions.StructTypeOps
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.util.FailFastMode
import org.apache.spark.sql.functions.{col, unix_timestamp}
import org.apache.spark.sql.types.StringType

import java.io.File

object LocalDataLoader {
  def writeTableFromFile(file: File, tableName: String, session: SparkSession): Unit = {
    println(s"Checking table: $tableName")
    if (session.catalog.tableExists(tableName)) return
    val extension = file.getName.split("\\.").last.toLowerCase
    if (!Seq("csv", "json", "jsonl", "parquet").contains(extension)) {
      println(s"Unable to load file due to invalid extension from file: ${file.getPath}")
      return
    }

    val reader = session.read
      .option("inferSchema", "true")
      .option("mode", FailFastMode.name)
    val df = if (extension == "csv") {
      reader.option("header", true).csv(file.getPath)
    } else if (extension == "json" || extension == "jsonl") {
      reader.option("multiLine", extension == "json").option("allowComments", true).json(file.getPath)
    } else {
      reader.parquet(file.getPath)
    }

    println(s"Loading data from ${file.getPath} into $tableName. Sample data and schema shown below")
    df.show(100)
    println(df.schema.pretty)

    if (df.schema.map(_.name).contains("ds")) {
      df.write.partitionBy("ds").saveAsTable(tableName)
    } else {
      df.write.saveAsTable(tableName)
    }
  }

  def loadDataRecursively(
      fileOrDirectory: File,
      session: SparkSession,
      namespaces: Seq[String] = Seq.empty[String]): Unit = {
    assert(fileOrDirectory.exists(), s"Non existent file: ${fileOrDirectory.getPath}")
    val nsFields = if (namespaces.isEmpty) { Seq("default") }
    else { namespaces }
    val splits = fileOrDirectory.getName.split("\\.")
    if (fileOrDirectory.isDirectory) {
      fileOrDirectory.listFiles.foreach(loadDataRecursively(_, session, namespaces :+ fileOrDirectory.getName))
    } else {
      val (ns, table) = if (splits.size == 2) { nsFields -> splits(0) }
      else { (nsFields :+ splits(0)) -> splits(1) }
      val namespace = ns.mkString("_")
      loadDataFile(fileOrDirectory, session, namespace, table)
    }
  }

  def loadDataFileAsTable(file: File, session: SparkSession, namespaceAndTable: String): Unit = {
    val splits = namespaceAndTable.split("\\.")
    assert(splits.nonEmpty && splits.size <= 2, s"Invalid table name $namespaceAndTable")

    val (namespace, tableName) =
      if (splits.size == 1) "default" -> splits(0)
      else splits(0) -> splits(1)

    loadDataFile(file, session, namespace, tableName)
  }

  private def loadDataFile(
      file: File,
      session: SparkSession,
      namespace: String,
      tableName: String
  ): Unit = {
    assert(file.exists(), s"Non existent file: ${file.getPath}")
    assert(file.isFile, s"Cannot load a directory as a local table: ${file.getPath}")
    println(s"Loading file(${file.getPath}) as $namespace.$tableName")

    if (!session.catalog.databaseExists(namespace))
      session.sql(s"CREATE DATABASE $namespace")
    writeTableFromFile(file, namespace + "." + tableName, session)
  }
}
