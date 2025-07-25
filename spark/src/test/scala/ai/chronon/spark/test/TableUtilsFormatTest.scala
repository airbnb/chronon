package ai.chronon.spark.test

import ai.chronon.api.{DoubleType, IntType, LongType, StringType, StructField, StructType}
import ai.chronon.spark.SparkSessionBuilder.FormatTestEnvVar
import ai.chronon.spark.test.TestUtils.makeDf
import ai.chronon.spark.{IncompatibleSchemaException, SparkSessionBuilder, TableUtils}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{AnalysisException, DataFrame, Row, SparkSession}
import org.junit.Assert.{assertEquals, assertTrue}
import org.junit.Test

import scala.util.Try

class TableUtilsFormatTest {

  import TableUtilsFormatTest._

  // Read the format we want this instantiation of the test to run via environment vars
  val format: String = sys.env.getOrElse(FormatTestEnvVar, "hive")
  val spark = SparkSessionBuilder.build("TableUtilsFormatTest", local = true)
  val tableUtils = TableUtils(spark)

  @Test
  def testInsertPartitionsAddColumns(): Unit = {
    val dbName = s"db_${System.currentTimeMillis()}"
    val tableName = s"$dbName.test_table_1_$format"
    spark.sql(s"CREATE DATABASE IF NOT EXISTS $dbName")
    val columns1 = Array(
      StructField("long_field", LongType),
      StructField("int_field", IntType),
      StructField("string_field", StringType)
    )
    val df1 = makeDf(
      spark,
      StructType(
        tableName,
        columns1 :+ StructField("ds", StringType)
      ),
      List(
        Row(1L, 2, "3", "2022-10-01")
      )
    )

    val df2 = makeDf(
      spark,
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
    testInsertPartitions(spark, tableUtils, tableName, format, df1, df2, ds1 = "2022-10-01", ds2 = "2022-10-02")
  }

  @Test
  def testInsertPartitionsAddRemoveColumns(): Unit = {
    val dbName = s"db_${System.currentTimeMillis()}"
    val tableName = s"$dbName.test_table_2_$format"
    spark.sql(s"CREATE DATABASE IF NOT EXISTS $dbName")
    val columns1 = Array(
      StructField("long_field", LongType),
      StructField("int_field", IntType),
      StructField("string_field", StringType)
    )
    val df1 = makeDf(
      spark,
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
      spark,
      StructType(
        tableName,
        columns1 :+ StructField("ds", StringType)
      ),
      List(
        Row(5L, 6, "7", "2022-10-02")
      )
    )
    testInsertPartitions(spark, tableUtils, tableName, format, df1, df2, ds1 = "2022-10-01", ds2 = "2022-10-02")
  }

  @Test
  def testInsertPartitionsAddModifyColumns(): Unit = {
    val dbName = s"db_${System.currentTimeMillis()}"
    val tableName = s"$dbName.test_table_3_$format"
    spark.sql(s"CREATE DATABASE IF NOT EXISTS $dbName")
    val columns1 = Array(
      StructField("long_field", LongType),
      StructField("int_field", IntType)
    )
    val df1 = makeDf(
      spark,
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
      spark,
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

    testInsertPartitions(spark, tableUtils, tableName, format, df1, df2, ds1 = "2022-10-01", ds2 = "2022-10-02")
  }
}

object TableUtilsFormatTest {
  private def testInsertPartitions(spark: SparkSession,
                                   tableUtils: TableUtils,
                                   tableName: String,
                                   format: String,
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
      val insertTry = Try(tableUtils.insertPartitions(df2, tableName))
      assertTrue(insertTry.failed.get.isInstanceOf[AnalysisException])
    }

    tableUtils.insertPartitions(df2, tableName, autoExpand = true)

    // check that we wrote out a table in the right format
    val readTableFormat = tableUtils.tableReadFormat(tableName).toString
    assertTrue(s"Mismatch in table format: $readTableFormat; expected: $format", readTableFormat.toLowerCase == format)

    // check we have all the partitions written
    val returnedPartitions = tableUtils.partitions(tableName)
    assertTrue(returnedPartitions.toSet == Set(ds1, ds2))

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
}
