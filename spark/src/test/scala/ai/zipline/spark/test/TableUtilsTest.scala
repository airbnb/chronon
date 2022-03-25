package ai.zipline.spark.test
import ai.zipline.spark.{SparkSessionBuilder, TableUtils}
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
        |  COALESCE(IF(column_d, column_e, NULL), column_e) AS not_this_one_either
        |FROM fake_table
        |WHERE column_f IS NOT NULL AND column_g != 'Something'
        |""".stripMargin

    val columns = tableUtils.getColumnsFromQuery(sampleSql)
    val expected = Seq("column_a", "column_b", "column_c", "column_d", "column_e", "column_f", "column_g")
    assertEquals(columns.sorted, expected)
  }

}
