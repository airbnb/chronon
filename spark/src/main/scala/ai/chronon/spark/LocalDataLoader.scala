/*
 *    Copyright (C) 2023 The Chronon Authors.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package ai.chronon.spark

import org.slf4j.LoggerFactory
import ai.chronon.spark.Extensions.StructTypeOps
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.util.FailFastMode
import org.apache.spark.sql.functions.{col, unix_timestamp, date_format}
import org.apache.spark.sql.types.{StringType, TimestampType}

import java.io.File

object LocalDataLoader {
  @transient lazy val logger = LoggerFactory.getLogger(getClass)
  def writeTableFromFile(file: File, tableName: String, session: SparkSession): Unit = {
    logger.info(s"Checking table: ${tableName}")
    if (session.catalog.tableExists(tableName)) return
    val extension = file.getName.split("\\.").last
    if (!Seq("csv", "json", "jsonl").contains(extension)) {
      logger.error(s"Unable to load file due to invalid extension from file: ${file.getPath}")
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
        .withColumn("ts", unix_timestamp(date_format(col("ts_string"), "yyyy-MM-dd HH:mm:ss")) * 1000)
        .drop("ts_string")
    }

    logger.info(s"Loading data from ${file.getPath} into $tableName. Sample data and schema shown below")
    df.show(100)
    logger.info(df.schema.pretty)

    if (df.schema.map(_.name).contains("ds")) {
      df.write.partitionBy("ds").saveAsTable(tableName)
    } else {
      df.write.saveAsTable(tableName)
    }
  }

  def loadDataRecursively(fileOrDirectory: File,
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
    logger.info(s"Loading file(${file.getPath}) as $namespace.$tableName")

    if (!session.catalog.databaseExists(namespace))
      session.sql(s"CREATE DATABASE $namespace")
    writeTableFromFile(file, namespace + "." + tableName, session)
  }
}
