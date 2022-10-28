package ai.chronon.spark.test

import ai.chronon.api.{StructField, _}
import ai.chronon.spark.{Conversions, IncompatibleSchemaException, SparkSessionBuilder, TableUtils}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{AnalysisException, DataFrame, Row, SparkSession}
import org.junit.Assert.{assertEquals, assertTrue}
import ai.chronon.spark.{PartitionRange, SparkSessionBuilder, TableUtils}
import org.apache.spark.sql.SparkSession
import org.junit.Assert.assertEquals
import org.junit.Test

import scala.util.{ScalaVersionSpecificCollectionsConverter, Try}

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

  private def testInsertPartitions(tableName: String,
                                   df1: DataFrame,
                                   df2: DataFrame,
                                   ds1: String,
                                   ds2: String): Unit = {
    tableUtils.insertPartitions(df1, tableName, autoExpand = true)
    val addedColumns = df2.schema.fieldNames.filterNot(df1.schema.fieldNames.contains)
    val removedColumns = df1.schema.fieldNames.filterNot(df2.schema.fieldNames.contains)
    val inconsistentColumns = (
      for (
        (name1, dtype1) <- df1.schema.fields.map(structField => (structField.name, structField.dataType));
        (name2, dtype2) <- df2.schema.fields.map(structField => (structField.name, structField.dataType))
      ) yield {
        name1 == name2 && dtype1 != dtype2
      }
    ).filter(identity)

    if (inconsistentColumns.nonEmpty) {
      val insertTry = Try(tableUtils.insertPartitions(df2, tableName, autoExpand = true))
      val e = insertTry.failed.get.asInstanceOf[IncompatibleSchemaException]
      assertEquals(inconsistentColumns.length, e.inconsistencies.length)
      return
    }

    if (df2.schema != df1.schema) {
      val insertTry = Try(tableUtils.insertPartitions(df2, tableName, autoExpand = false))
      assertTrue(insertTry.failed.get.isInstanceOf[AnalysisException])
    }

    tableUtils.insertPartitions(df2, tableName, autoExpand = true)

    val dataRead1 = spark.table(tableName).where(col("ds") === ds1)
    val dataRead2 = spark.table(tableName).where(col("ds") === ds2)
    assertTrue(dataRead1.columns.length == dataRead2.columns.length)

    val totalColumnsCount = (df1.schema.fieldNames.toSet ++ df2.schema.fieldNames.toSet).size
    assertEquals(totalColumnsCount, dataRead1.columns.length)
    assertEquals(totalColumnsCount, dataRead2.columns.length)

    addedColumns.foreach(col => {
      dataRead1.foreach(row => assertTrue(Option(row.getAs[Any](col)).isEmpty))
    })
    removedColumns.foreach(col => {
      dataRead2.foreach(row => assertTrue(Option(row.getAs[Any](col)).isEmpty))
    })
  }

  private def makeDf(schema: StructType, rows: List[Row]): DataFrame = {
    spark.createDataFrame(
      ScalaVersionSpecificCollectionsConverter.convertScalaListToJava(rows),
      Conversions.fromChrononSchema(schema)
    )
  }

  @Test
  def testInsertPartitionsAddColumns(): Unit = {
    val tableName = "db.test_table_1"
    spark.sql("CREATE DATABASE IF NOT EXISTS db")
    val columns1 = Array(
      StructField("long_field", LongType),
      StructField("int_field", IntType),
      StructField("string_field", StringType)
    )
    val df1 = makeDf(
      StructType(
        tableName,
        columns1 :+ StructField("ds", StringType)
      ),
      List(
        Row(1L, 2, "3", "2022-10-01")
      )
    )

    val df2 = makeDf(
      StructType(
        tableName,
        columns1
          :+ StructField("double_field", DoubleType)
          :+ StructField("ds", StringType)
      ),
      List(
        Row(4L, 5, "6", 7.0, "2022-10-02")
      )
    )

    testInsertPartitions(tableName, df1, df2, ds1 = "2022-10-01", ds2 = "2022-10-02")
  }

  @Test
  def testInsertPartitionsRemoveColumns(): Unit = {
    val tableName = "db.test_table_2"
    spark.sql("CREATE DATABASE IF NOT EXISTS db")
    val columns1 = Array(
      StructField("long_field", LongType),
      StructField("int_field", IntType),
      StructField("string_field", StringType)
    )
    val df1 = makeDf(
      StructType(
        tableName,
        columns1
          :+ StructField("double_field", DoubleType)
          :+ StructField("ds", StringType)
      ),
      List(
        Row(1L, 2, "3", 4.0, "2022-10-01")
      )
    )

    val df2 = makeDf(
      StructType(
        tableName,
        columns1 :+ StructField("ds", StringType)
      ),
      List(
        Row(5L, 6, "7", "2022-10-02")
      )
    )
    testInsertPartitions(tableName, df1, df2, ds1 = "2022-10-01", ds2 = "2022-10-02")
  }

  @Test
  def testInsertPartitionsModifiedColumns(): Unit = {
    val tableName = "db.test_table_3"
    spark.sql("CREATE DATABASE IF NOT EXISTS db")
    val columns1 = Array(
      StructField("long_field", LongType),
      StructField("int_field", IntType)
    )
    val df1 = makeDf(
      StructType(
        tableName,
        columns1
          :+ StructField("string_field", StringType)
          :+ StructField("ds", StringType)
      ),
      List(
        Row(1L, 2, "3", "2022-10-01")
      )
    )

    val df2 = makeDf(
      StructType(
        tableName,
        columns1
          :+ StructField("string_field", DoubleType) // modified column data type
          :+ StructField("ds", StringType)
      ),
      List(
        Row(1L, 2, 3.0, "2022-10-02")
      )
    )

    testInsertPartitions(tableName, df1, df2, ds1 = "2022-10-01", ds2 = "2022-10-02")
  }

  @Test
  def ChunkTest(): Unit = {
    val actual = tableUtils.chunk(Set("2021-01-01", "2021-01-02", "2021-01-05", "2021-01-07"))
    val expected = Seq(
      PartitionRange("2021-01-01", "2021-01-02"),
      PartitionRange("2021-01-05", "2021-01-05"),
      PartitionRange("2021-01-07", "2021-01-07"))
    assertEquals(expected, actual)
  }

  def testDropPartitions(): Unit = {
    val tableName = "db.test_drop_partitions_table"
    spark.sql("CREATE DATABASE IF NOT EXISTS db")
    val columns1 = Array(
      StructField("long_field", LongType),
      StructField("int_field", IntType),
      StructField("ds", StringType),
      StructField("label_ds", StringType)
    )
    val df1 = makeDf(
      StructType(
        tableName,
        columns1
      ),
      List(
        Row(1L, 2, "2022-10-01", "2022-11-01"),
        Row(2L, 2, "2022-10-02", "2022-11-02"),
        Row(3L, 8, "2022-10-05", "2022-11-03")
      )
    )
    tableUtils.insertPartitions(df1, tableName, partitionColumns = Seq(Constants.PartitionColumn, Constants.LabelPartitionColumn))
    tableUtils.dropPartitions(tableName, Seq("2022-10-01", "2022-10-02"), labelPartition = "2022-11-02")
    val updated = tableUtils.sql(
      s"""
         |SELECT * from ${tableName}
         |""".stripMargin)
    assertEquals(updated.count(), 2)
    assertTrue(updated.collect().sameElements(List(
      Row(1L, 2, "2022-10-01", "2022-11-01"),
      Row(3L, 8, "2022-10-05", "2022-11-03")
    )))
  }
}
