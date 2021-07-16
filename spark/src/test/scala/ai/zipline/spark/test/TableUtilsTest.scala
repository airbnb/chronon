package ai.zipline.spark.test

import ai.zipline.spark.TableUtils
import ai.zipline.aggregator.base.{IntType, StringType}
import ai.zipline.aggregator.test.Column
import ai.zipline.spark.SparkSessionBuilder
import org.apache.spark.sql.SparkSession
import org.junit.Test


class TableUtilsTest {
  lazy val spark: SparkSession = SparkSessionBuilder.build("TableUtilsTest", local = true)
  protected val tableUtils = TableUtils(spark)
  protected val namespace = "table_utils_zipline_test"
  spark.sql(s"CREATE DATABASE IF NOT EXISTS $namespace")

  @Test(expected=classOf[RuntimeException])
  def testNullPartitions(): Unit = {
    val schema = List(
      Column("user", StringType, 10),
      Column("session_length", IntType, 1000)
    )
    val df = DataFrameGen.events(spark, schema, count = 10000, partitions = 10)
    tableUtils.insertPartitions(df, s"$namespace.some_table")
    val toNull = df.limit(1).collect()(0).getAs[String](0)
    val updatedDf = spark.sql(s"SELECT user, session_length, IF(user == '$toNull', NULL, ds) AS ds from $namespace.some_table")
    tableUtils.insertPartitions(updatedDf, s"$namespace.some_table_2")
  }
}