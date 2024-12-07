package ai.chronon.spark.test

import ai.chronon.api.Builders
import ai.chronon.spark.Extensions._
import ai.chronon.spark.test.TestUtils.compareDfSchemas
import ai.chronon.spark.{Comparison, PartitionRange, SparkSessionBuilder, TableUtils}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SparkSession}
import org.junit.Assert.assertEquals
import org.junit.Test

class ExtensionsTest {

  lazy val spark: SparkSession = SparkSessionBuilder.build("ExtensionsTest", local = true)
  import spark.implicits._

  private implicit val tableUtils: TableUtils = TableUtils(spark)

  @Test
  def testPadFields(): Unit = {
    val df = Seq(
      (1, "foo", null, 1.1),
      (2, "bar", null, 2.2),
      (3, "baz", null, 3.3)
    ).toDF("field1", "field2", "field3", "field4")
    val paddedSchema = new StructType(Array(
      StructField("field2", IntegerType),
      StructField("field3", IntegerType),
      StructField("field4", IntegerType),
      StructField("field5", IntegerType),
      StructField("field6", IntegerType),
    ))

    val paddedDf = df.padFields(paddedSchema)

    val expectedDf = Seq(
      (1, "foo", null, 1.1, null, null),
      (2, "bar", null, 2.2, null, null),
      (3, "baz", null, 3.3, null, null)
    ).toDF("field1", "field2", "field3", "field4", "field5", "field6")
    val diff = Comparison.sideBySide(expectedDf, paddedDf, List("field1"))
    if (diff.count() > 0) {
      diff.show()
    }
    assertEquals(0, diff.count())
  }

  @Test
  def testPrunePartitionTest(): Unit = {
    val df = Seq(
      (1, "2024-01-03"),
      (2, "2024-01-04"),
      (3, "2024-01-04"),
      (4, "2024-01-05"),
      (5, "2024-01-05"),
      (6, "2024-01-06"),
      (7, "2024-01-07"),
      (8, "2024-01-08"),
      (9, "2024-01-08"),
      (10, "2024-01-09"),
    ).toDF("key", "ds")

    val prunedDf = df.prunePartition(PartitionRange("2024-01-05", "2024-01-07"))

    val expectedDf = Seq(
      (4, "2024-01-05"),
      (5, "2024-01-05"),
      (6, "2024-01-06"),
      (7, "2024-01-07"),
    ).toDF("key", "ds")
    val diff = Comparison.sideBySide(expectedDf, prunedDf, List("key"))
    if (diff.count() != 0) {
      diff.show()
    }
    assertEquals(0, diff.count())
  }

  @Test
  def testRenameRightColumnsForJoin(): Unit = {
    val schema = new StructType(Array(
      StructField("key1", IntegerType),
      StructField("key2", IntegerType),
      StructField("right_key1", IntegerType),
      StructField("right_key2", IntegerType),
      StructField("value1", StringType),
      StructField("value2", StringType),
      StructField("ds", StringType),
      StructField("ts", TimestampType),
    ))
    val joinPart = Builders.JoinPart(
      groupBy = Builders.GroupBy(metaData = Builders.MetaData(name = "test_gb"), keyColumns = Seq("key1", "key2")),
      keyMapping = Map("left_key1" -> "right_key1", "left_key2" -> "right_key2"),
      prefix = "test_prefix"
    )
    val df = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], schema)

    val renamedDf = df.renameRightColumnsForJoin(joinPart, Set("ts", "ds"))

    val expectedSchema = new StructType(Array(
      StructField("key1", IntegerType),
      StructField("key2", IntegerType),
      StructField("left_key1", IntegerType),
      StructField("left_key2", IntegerType),
      StructField("test_prefix_test_gb_value1", StringType),
      StructField("test_prefix_test_gb_value2", StringType),
      StructField("ds", StringType),
      StructField("ts", TimestampType),
    ))
    assertEquals(compareDfSchemas(renamedDf.schema, expectedSchema), Seq())
  }
}