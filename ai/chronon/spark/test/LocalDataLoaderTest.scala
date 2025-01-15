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

import ai.chronon.spark.test.LocalDataLoaderTest.spark
import com.google.common.io.Files
import org.apache.spark.sql.SparkSession
import org.junit.Assert.assertEquals
import org.junit.{AfterClass, Test}

import java.io.File

object LocalDataLoaderTest {

  val tmpDir: File = Files.createTempDir()

  val spark: SparkSession = SparkSessionBuilder.build(
    "LocalDataLoaderTest",
    local = true,
    Some(tmpDir.getPath))

  @AfterClass
  def teardown(): Unit = {
    FileUtils.deleteDirectory(tmpDir)
  }
}

class LocalDataLoaderTest {

  val srcPath = "src/test/resources/local_data_csv/"

  def matchPath(srcPath: String, fileName: Option[String] = None): File = (new File (srcPath).exists (), fileName.isDefined) match {
      // if src/ path exists and a filename is provided
      case (true, true) => new File (srcPath + fileName.get)
      // if src/ path exists and no filename is provided
      case (true, false) => new File (srcPath)
      // if src/ path does not exist and a filename is provided try spark/src/
      case (false, true) => new File ("spark/" + srcPath + fileName.get)
      // if src/ path does not exist and no filename is provided try spark/src/
      case (false, false) => new File ("spark/" + srcPath)
    }

  @Test
  def loadDataFileAsTableShouldBeCorrect(): Unit = {

    val fileName = "test_table_1_data.csv"
    val file = matchPath(srcPath, Some(fileName))
    val nameSpaceAndTable = "test.table"
    LocalDataLoader.loadDataFileAsTable(file, spark, nameSpaceAndTable)

    val loadedDataDf = spark.sql(s"SELECT * FROM $nameSpaceAndTable")
    val expectedColumns = Set("id_listing_view_event", "id_product", "dim_product_type", "ds")

    loadedDataDf.columns.foreach(column => expectedColumns.contains(column))
    assertEquals(3, loadedDataDf.count())
  }

  @Test
  def loadDataRecursivelyShouldBeCorrect(): Unit = {
    val path = matchPath(srcPath)
    LocalDataLoader.loadDataRecursively(path, spark)

    val loadedDataDf = spark.sql(s"SELECT * FROM local_data_csv.test_table_1_data")
    val expectedColumns = Set("id_listing_view_event", "id_product", "dim_product_type", "ds")

    loadedDataDf.columns.foreach(column => expectedColumns.contains(column))
    assertEquals(3, loadedDataDf.count())
  }
}
