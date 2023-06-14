package ai.chronon.spark.test

import ai.chronon.aggregator.test.Column
import ai.chronon.api
import ai.chronon.spark.Extensions._
import ai.chronon.spark.{MetadataExporter, SparkSessionBuilder, TableUtils}
import com.google.common.io.Files
import junit.framework.TestCase
import org.apache.spark.sql.SparkSession

import java.io.File

class MetadataExporterTest extends TestCase {
  val sessionName = "MetadataExporter"
  val spark: SparkSession = SparkSessionBuilder.build(sessionName, local = true)
  val tableUtils: TableUtils = TableUtils(spark)
  def testMetadataExport(): Unit = {
    // Create the tables.
    val namespace = "example_namespace"
    val tablename = "table"
    tableUtils.sql(s"CREATE DATABASE IF NOT EXISTS $namespace")
    val sampleData = List(
      Column("a", api.StringType, 10),
      Column("b", api.StringType, 10),
      Column("c", api.LongType, 100),
      Column("d", api.LongType, 100),
      Column("e", api.LongType, 100),
    )
    val sampleTable = s"$namespace.$tablename"
    val sampleDf = DataFrameGen
      .events(spark, sampleData, 10000, partitions = 30)
    sampleDf.save(sampleTable)
    val confResource = getClass.getResource("/")
    val tmpDir: File = Files.createTempDir()
    MetadataExporter.run(confResource.getPath, tmpDir.getAbsolutePath)
  }

}
