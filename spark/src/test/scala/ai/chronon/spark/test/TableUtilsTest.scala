package ai.chronon.spark.test

import ai.chronon.spark.{SparkSessionBuilder, TableUtils}
import org.apache.spark.sql.SparkSession
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

}
