package ai.zipline.spark.test

import ai.zipline.api.Constants
import ai.zipline.spark.TableUtils
import ai.zipline.aggregator.base.{IntType, StringType}
import ai.zipline.aggregator.test.Column
import ai.zipline.spark.SparkSessionBuilder
import org.apache.spark.sql.SparkSession
import org.junit.Test

class TableUtilsTest {
  lazy val spark: SparkSession = SparkSessionBuilder.build("TableUtilsTest", local = true)
  private val tableUtils = TableUtils(spark)
  private val namespace = "table_utils_zipline_test"
  spark.sql(s"CREATE DATABASE IF NOT EXISTS $namespace")
  private val schema = List(
    Column("user", StringType, 10),
    Column("session_length", IntType, 1000)
  )

  @Test
  def testInsertPartitions(): Unit = {
    val df = DataFrameGen.events(spark, schema, count = 10000, partitions = 10)
    tableUtils.insertPartitions(df, s"$namespace.basic_table")
  }

  @Test(expected=classOf[RuntimeException])
  def testNullPartitions(): Unit = {
    val df = DataFrameGen.events(spark, schema, count = 10000, partitions = 10)
    val user = df.select("user").where("user IS NOT NULL").first().getString(0)
    df.createOrReplaceTempView("df_without_nulls")
    val updatedDf = spark.sql(s"SELECT user, session_length, IF(user == '$user', NULL, ${Constants.PartitionColumn}) AS ${Constants.PartitionColumn} FROM df_without_nulls")
    tableUtils.insertPartitions(updatedDf, s"$namespace.table_with_nulls_in_partition")
  }

  @Test(expected=classOf[RuntimeException])
  def testMalformedPartitions(): Unit = {
    val df = DataFrameGen.events(spark, schema, count = 10000, partitions = 10)
    val user = df.select("user").where("user IS NOT NULL").first().getString(0)
    df.createOrReplaceTempView("df_proper_format")
    val updatedDf = spark.sql(s"SELECT user, session_length, IF(user == '$user', '25-07-20', ${Constants.PartitionColumn}) AS ${Constants.PartitionColumn} FROM df_proper_format")
    tableUtils.insertPartitions(updatedDf, s"$namespace.table_with_malformed_partitions")
  }
}