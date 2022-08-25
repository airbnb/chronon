package ai.chronon.spark.test

import ai.chronon.api.{DoubleType, IntType, LongType, StringType, StructField, StructType}
import ai.chronon.spark.{Conversions, SparkSessionBuilder, TableUtils}
import org.apache.spark.sql.{Row, SparkSession}
import org.junit.Assert.{assertEquals, assertTrue}
import org.junit.Test

import scala.util.ScalaVersionSpecificCollectionsConverter

class TableUtilsTest {
  lazy val spark: SparkSession = SparkSessionBuilder.build("TableUtilsTest", local = true)
  private val tableUtils = TableUtils(spark)

  @Test
  def ColumnFromSqlTest(): Unit = {
    val sampleSql =
      """
        |SELECT
        |  CASE WHEN column_a IS NULL THEN 1 ELSE NULL END,
        |  column_b,
        |  column_c AS not_this_one,
        |  COALESCE(IF(column_d, column_e, NULL), column_e) AS not_this_one_either,
        |  column_nested.first.second AS not_this_one_as_well
        |FROM fake_table
        |WHERE column_f IS NOT NULL AND column_g != 'Something' AND column_d > 0
        |""".stripMargin

    val columns = tableUtils.getColumnsFromQuery(sampleSql)
    println(columns)
    val expected = Seq("column_a",
                       "column_b",
                       "column_c",
                       "column_d",
                       "column_e",
                       "column_f",
                       "column_g",
                       "`column_nested.first.second`").sorted
    assertEquals(expected, columns.sorted)
  }

  @Test
  def testExpandTable(): Unit = {

    val tableName = "db.test_table"
    val rows = List(
      Row(1L, 2, "3", "2022-10-01"),
      Row(4L, 5, "6", "2022-10-02")
    )
    val schema = StructType(
      tableName,
      Array(
        StructField("long_field", LongType),
        StructField("int_field", IntType),
        StructField("string_field", StringType),
        StructField("ds", StringType)
      )
    )
    val df = spark.createDataFrame(
      ScalaVersionSpecificCollectionsConverter.convertScalaListToJava(rows),
      Conversions.fromChrononSchema(schema)
    )
    spark.sql("CREATE DATABASE IF NOT EXISTS db")
    tableUtils.insertPartitions(df, tableName)

    val newSchemaType = StructType(
      tableName,
      Array(
        StructField("long_field", LongType),
        StructField("int_field", IntType),
        StructField("string_field", StringType),
        StructField("double_field", DoubleType),
        StructField("ds", StringType)
      )
    )
    val newRows = List(
      Row(7L, 8, "9", 10.0, "2022-10-03")
    )
    val newDf = spark.createDataFrame(
      ScalaVersionSpecificCollectionsConverter.convertScalaListToJava(newRows),
      Conversions.fromChrononSchema(newSchemaType)
    )

    tableUtils.expandTable(tableName, Conversions.fromChrononSchema(newSchemaType))
    tableUtils.insertPartitions(newDf, tableName)

    val data = spark.sql(s"select * from $tableName").collect
    assertEquals(3, data.length)
    assertEquals(5, data.head.length)
    assertEquals(null, data.filter(_.getAs[String]("ds") == "2022-10-01").head.getAs[Double]("double_field"))
    assertEquals(10.0, data.filter(_.getAs[String]("ds") == "2022-10-03").head.getAs[Double]("double_field"), 1e-8)

    val newSchemaType2 = StructType(
      tableName,
      Array(
        StructField("long_field", LongType),
        StructField("string_field", StringType),
        StructField("double_field", DoubleType),
        StructField("ds", StringType)
      )
    )
    tableUtils.expandTable(tableName, Conversions.fromChrononSchema(newSchemaType2))
    val data2 = spark.sql(s"select * from $tableName").collect

    // columns are never removed from table
    assertEquals(5, data2.head.length)
  }
}
