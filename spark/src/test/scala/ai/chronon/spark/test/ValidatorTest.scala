package ai.chronon.spark.test

import ai.chronon.api
import ai.chronon.spark._
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{IntegerType, LongType, StructField, StructType}
import org.junit.Assert.{assertEquals, assertTrue}
import org.junit.Test

import scala.collection.JavaConverters._

class ValidatorTest {

  lazy val spark: SparkSession = SparkSessionBuilder.build("ValidatorTest", local = true)
  implicit val tableUtils = TableUtils(spark)

  import spark.implicits._

  @Test
  def testValidatePartitionColumn_passes(): Unit = {
    val df = Seq(
      (2, "2023-07-15"),
      (4, "2023-07-15"),
      (4, "2023-07-16")
    ).toDF("a", "ds")
    val result = Validator.validatePartitionColumn(df, "leftDf")
    assertTrue(result.isEmpty)
  }

  @Test
  def testValidatePartitionColumn_wrongType(): Unit = {
    val df = Seq(
      (2, 4),
      (4, 5),
      (4, 6)
    ).toDF("a", "ds")
    val result = Validator.validatePartitionColumn(df, "leftDf")
    assertEquals(result.size, 1)
    assertEquals(result(0), "df for leftDf has wrong type for PartitionColumn ds, should be StringType")
  }

  @Test
  def testValidatePartitionColumn_wrongFormat(): Unit = {
    val df = Seq(
      (2, "2023/07/15"),
      (4, "2023/07/15"),
      (4, "2023/07/16")
    ).toDF("a", "ds")
    val result = Validator.validatePartitionColumn(df, "leftDf")
    assertEquals(result.size, 1)
    assertEquals(result(0), "df for leftDf has wrong format for PartitionColumn ds, should be yyyy-MM-dd")
  }

  def buildGroupBy(): api.GroupBy = {
    val groupBy = new api.GroupBy
    groupBy.metaData = new api.MetaData()
    groupBy.metaData.name = "groupBy1"
    val aggregation = new api.Aggregation
    aggregation.windows = List(new api.Window()).asJava
    groupBy.aggregations = List(aggregation).asJava
    groupBy
  }

  @Test
  def testValidateTimeColumn_passes(): Unit = {
    val inputSchema = List(
      StructField("a", IntegerType, false),
      StructField("ts", LongType, true)
    )
    val values = Seq(
      Row(2, 1691369564000L),
      Row(4, 1691369563000L),
      Row(4, 1691369562000L),
      Row(5, null)  // Will be skipped and not checked.
    )
    val df = spark.createDataFrame(
      spark.sparkContext.parallelize(values),
      StructType(inputSchema)
    )
    val result = Validator.validateTimeColumn(df, buildGroupBy())
    assertTrue(result.isEmpty)
  }

  @Test
  def testValidateTimeColumn_NoTsRequired(): Unit = {
    val df = Seq(
      (2, 100),
      (4, 200),
      (4, 300)
    ).toDF("a", "not_a_ts")
    val groupBy = buildGroupBy()
    // If there are no aggregations the time column isn't needed.
    groupBy.aggregations = List().asJava
    val result = Validator.validateTimeColumn(df, groupBy)
    assertTrue(result.isEmpty)
  }

  @Test
  def testValidateTimeColumn_missingTimeColumn(): Unit = {
    val df = Seq(
      (2, "foo"),
      (4, "bar"),
      (4, "foobar")
    ).toDF("a", "not_a_ts")
    val result = Validator.validateTimeColumn(df, buildGroupBy())
    assertEquals(result.size, 1)
    assertEquals(result(0), "df for groupBy groupBy1 does not contain TimeColumn ts")
  }

  @Test
  def testValidateTimeColumn_wrongType(): Unit = {
    val df = Seq(
      (2, 4),
      (4, 5),
      (4, 6)
    ).toDF("a", "ts")
    val result = Validator.validateTimeColumn(df, buildGroupBy())
    assertEquals(result.size, 1)
    assertEquals(result(0), "df for groupBy groupBy1 has wrong type for TimeColumn ts, should be LongType")
  }

  @Test
  def testValidateTimeColumn_OutOfRangeTooLow(): Unit = {
    val df = Seq(
      (2, 1691369L)
    ).toDF("a", "ts")
    val result = Validator.validateTimeColumn(df, buildGroupBy())
    assertEquals(result.size, 1)
    assertEquals(result(0), "df for groupBy groupBy1 should have TimeColumn ts that is milliseconds since Unix epoch. Example invalid ts: 1691369")
  }

  @Test
  def testValidateTimeColumn_OutOfRangeTooHigh(): Unit = {
    val df = Seq(
      (2, 1691369564000000L)
    ).toDF("a", "ts")
    val result = Validator.validateTimeColumn(df, buildGroupBy())
    assertEquals(result.size, 1)
    assertEquals(result(0), "df for groupBy groupBy1 should have TimeColumn ts that is milliseconds since Unix epoch. Example invalid ts: 1691369564000000")
  }
}
