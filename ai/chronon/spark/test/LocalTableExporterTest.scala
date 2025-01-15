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

package ai.chronon.spark.test

import ai.chronon.aggregator.test.Column
import ai.chronon.api
import ai.chronon.api.{Constants, IntType, LongType, StringType}
import ai.chronon.spark.test.LocalTableExporterTest.{spark, tmpDir}
import ai.chronon.spark.{LocalTableExporter, SparkSessionBuilder, TableUtils}
import com.google.common.io.Files
import org.apache.commons.io.FileUtils
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.junit.Assert.{assertEquals, assertTrue}
import org.junit.{AfterClass, Test}

import java.io.File

object LocalTableExporterTest {

  val tmpDir: File = Files.createTempDir()
  val spark: SparkSession = SparkSessionBuilder.build("LocalTableExporterTest", local = true, Some(tmpDir.getPath))

  @AfterClass
  def teardown(): Unit = {
    FileUtils.deleteDirectory(tmpDir)
  }
}

class LocalTableExporterTest {

  @Test
  def exporterExportsTablesCorrectly(): Unit = {
    val schema = List(
      Column("user", StringType, 10),
      Column(Constants.TimeColumn, LongType, 10000), // ts = last 10000 days to avoid conflict
      Column("session_length", IntType, 10000)
    )

    val df = DataFrameGen
      .entities(spark, schema, 20, 3)
    val tableName = "default.exporter_test_1"
    df.write.mode(SaveMode.Overwrite).saveAsTable(tableName)
    val tableUtils = TableUtils(spark)

    val exporter = new LocalTableExporter(tableUtils, tmpDir.getAbsolutePath, "parquet", Some("local_test"))
    exporter.exportTable(tableName)

    // check if the file is created
    val expectedPath = s"${tmpDir.getAbsolutePath}/local_test.$tableName.parquet"
    val outputFile = new File(expectedPath)
    assertTrue(outputFile.isFile)

    // compare the content of the file with the generated
    val loadedDf = spark.read.parquet(expectedPath)
    val generatedData = df.collect().sortBy(_.getAs[Long](1))
    val loadedData = loadedDf.collect().sortBy(_.getAs[Long](1))
    assertEquals(generatedData.length, loadedData.length)

    generatedData.zip(loadedData).foreach { case (g, l) => assertEquals(g, l) }
  }

  @Test
  def exporterExportsMultipleTablesWithFilesInCorrectPlace(): Unit = {
    val schema = List(
      Column("user", StringType, 100000),
      Column(Constants.TimeColumn, LongType, 10000),
      Column("session_length", IntType, 10000)
    )

    val df = DataFrameGen.gen(spark, schema, 20)
    val tableName = "default.exporter_test_2"
    df.write.mode(SaveMode.Overwrite).saveAsTable(tableName)

    val weightSchema = List(
      Column("user", api.StringType, 100000),
      Column(Constants.TimeColumn, LongType, 10000),
      Column("country", api.StringType, 100),
      Column("weight", api.IntType, 500)
    )
    val namespace = "test_namespace"
    val weightTable = s"$namespace.weights"
    val wdf = DataFrameGen.gen(spark, weightSchema, 100)
    spark.sql(s"CREATE DATABASE $namespace")
    wdf.write.mode(SaveMode.Overwrite).saveAsTable(weightTable)

    val tableUtils = TableUtils(spark)

    val exporter = new LocalTableExporter(tableUtils, tmpDir.getAbsolutePath, "csv", Some("local_test"))
    exporter.exportTable(tableName)
    exporter.exportTable(weightTable)

    val outputFilePath1 = s"${tmpDir.getAbsolutePath}/local_test.$tableName.csv"
    val outputFile1 = new File(outputFilePath1)
    assertTrue(outputFile1.isFile)
    val generatedData1 = df.collect().sortBy(_.getAs[Long](1))
    val loadedData1 = spark.read
      .option("header", true)
      .option("inferSchema", true)
      .csv(outputFilePath1)
      .collect()
      .sortBy(_.getAs[Long](1))
    assertEquals(generatedData1.length, loadedData1.length)
    // We are using CSV in this test. CSV/JSON will lose type precision, hence we are using string format of the data
    // instead
    generatedData1.zip(loadedData1).foreach { case (g, l) => assertEquals(g.toString(), l.toString()) }

    val outputFilePath2 = s"${tmpDir.getAbsolutePath}/local_test.$weightTable.csv"
    val outputFile2 = new File(outputFilePath2)
    assertTrue(outputFile2.isFile)
    val generatedData2 = wdf.collect().sortBy(_.getAs[Long](1))
    val loadedData2 = spark.read
      .option("header", true)
      .option("inferSchema", true)
      .csv(outputFilePath2)
      .collect()
      .sortBy(_.getAs[Long](1))
    assertEquals(generatedData2.length, loadedData2.length)
    generatedData2.zip(loadedData2).foreach { case (g, l) => assertEquals(g.toString(), l.toString()) }
  }
}
