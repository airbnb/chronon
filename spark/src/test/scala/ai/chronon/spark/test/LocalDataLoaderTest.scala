package ai.chronon.spark.test

import java.io.File

import ai.chronon.spark.test.LocalDataLoaderTest.spark
import ai.chronon.spark.{LocalDataLoader, SparkSessionBuilder}
import com.google.common.io.Files
import org.apache.commons.io.FileUtils
import org.apache.spark.sql.SparkSession
import org.junit.Assert.assertEquals
import org.junit.{AfterClass, Test}

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

  @Test
  def loadDataFileAsTableShouldBeCorrect(): Unit = {
    val file = new File("spark/src/test/resources/local_data_csv/test_table_1_data.csv")
    val nameSpaceAndTable = "test.table"
    LocalDataLoader.loadDataFileAsTable(file, spark, nameSpaceAndTable)

    val loadedDataDf = spark.sql(s"SELECT * FROM $nameSpaceAndTable")
    val expectedColumns = Set("id_listing_view_event", "id_product", "dim_product_type", "ds")

    loadedDataDf.columns.foreach(column => expectedColumns.contains(column))
    assertEquals(3, loadedDataDf.count())
  }

  @Test
  def loadDataRecursivelyShouldBeCorrect(): Unit = {
    val path = new File("spark/src/test/resources/local_data_csv")
    LocalDataLoader.loadDataRecursively(path, spark)

    val loadedDataDf = spark.sql(s"SELECT * FROM local_data_csv.test_table_1_data")
    val expectedColumns = Set("id_listing_view_event", "id_product", "dim_product_type", "ds")

    loadedDataDf.columns.foreach(column => expectedColumns.contains(column))
    assertEquals(3, loadedDataDf.count())
  }
}
