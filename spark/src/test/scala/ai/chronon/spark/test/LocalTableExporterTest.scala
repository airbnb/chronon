package ai.chronon.spark.test

import ai.chronon.aggregator.test.Column
import ai.chronon.api.{Constants, IntType, LongType, StringType}
import ai.chronon.spark.test.LocalTableExporterTest.{spark, tmpDir}
import ai.chronon.spark.{LocalTableExporter, SparkSessionBuilder, TableUtils}
import com.google.common.io.Files
import org.apache.commons.io.FileUtils
import org.apache.spark.sql.SparkSession
import org.junit.Assert.{assertEquals, assertTrue}
import org.junit.{AfterClass, Test}

import java.io.File

object LocalTableExporterTest {

  val tmpDir: File = Files.createTempDir()
  val spark: SparkSession = SparkSessionBuilder.build("LocalTableExporterTest", local = true)

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
      Column(Constants.TimeColumn, LongType, 100), // ts = last 100 days
      Column("session_length", IntType, 10000)
    )

    val df = DataFrameGen.entities(spark, schema, 20, 3)
    val viewName = "exporter_test_1"
    df.createOrReplaceTempView(viewName)
    val tableUtils = TableUtils(spark)

    val exporter = new LocalTableExporter(tableUtils, tmpDir.getAbsolutePath, "parquet", Some("local_test"))
    exporter.exportAllTables()

    // check if the file is created
    val expectedPath = s"${tmpDir.getAbsolutePath}/local_test.default.$viewName.parquet"
    val outputFile = new File(expectedPath)
    assertTrue(outputFile.isFile)

    // compare the content of the file with the generated
    val loadedDf = spark.read.parquet(expectedPath)
    val generatedData = df.collect()
    val loadedData = loadedDf.collect()
    assertEquals(generatedData.length, loadedData.length)

    generatedData.zip(loadedData).foreach { case (g, l) => assertEquals(g, l) }
  }
}
