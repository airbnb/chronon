package ai.chronon.spark.test

import ai.chronon.api.Constants.HiveMetadataTableName
import ai.chronon.spark.{SparkSessionBuilder, TableUtils}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructField, StructType}
import org.junit.Assert.assertEquals
import org.junit.Test

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
  def getHiveSchemaEntryTest(): Unit = {
    //create test table
    spark.sql(s"CREATE DATABASE IF NOT EXISTS hive_test")
    val tableName = "hive_test." + HiveMetadataTableName
    val metadataSchema = StructType( Array(
      StructField("conf_key", StringType,true),
      StructField("entity_type", StringType,true),
      StructField("schema", StringType,true),
      StructField("schema_hash", StringType,true)
    ))
    //clean up table if already exists
    tableUtils.sql(s"DROP TABLE IF EXISTS ${tableName}")

    val createMetaTable = tableUtils.createTableSql(tableName, metadataSchema, Seq.empty[String], null, "parquet")
    tableUtils.sql(createMetaTable)
    println(s"Metadata schema table ${tableName} created")

    val confKey = "joins/testConfKey"
    val entityType = "join"
    val schemaHash = "testHash"
    val schema = StructType( Array(
      StructField("item", StringType,true),
      StructField("item_view_avg", DoubleType,true),
      StructField("item_view_sum", IntegerType,true)
    ))

    tableUtils.uploadEntityMetadata(confKey, schema, schemaHash, entityType, tableName)
    val entry = tableUtils.sql(s"SELECT * FROM ${tableName} WHERE conf_key = '$confKey' and schema_hash = '$schemaHash'")
    entry.show();
    val schemaEntry = tableUtils.getEntityMetadata(confKey, schemaHash, tableName)
    assert(schemaEntry.count() == 1)
    assert(schemaEntry.first().get(0).equals(confKey))
    assert(schemaEntry.first().get(3).equals(schemaHash))
  }
}
