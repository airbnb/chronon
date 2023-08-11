package ai.chronon.spark.test

import ai.chronon.aggregator.test.Column
import ai.chronon.api
import ai.chronon.api.Extensions.GroupByOps
import ai.chronon.api.{Builders, LongType, Operation, StringType, TimeUnit, Window}
import ai.chronon.online.SparkConversions
import ai.chronon.spark.Extensions._
import ai.chronon.spark.streaming.PutRequestBuilder
import ai.chronon.spark.{PartitionRange, SparkSessionBuilder, TableUtils}
import org.apache.spark.sql.SparkSession
import org.junit.Assert.{assertEquals, assertTrue}
import org.junit.Test

class PutRequestBuilderTest {

  val spark: SparkSession = SparkSessionBuilder.build("PutRequestBuilderTest", local = true)
  val namespace = "put_request_builder_test_ns"
  private val tableUtils = TableUtils(spark)

  private val today = tableUtils.partitionSpec.at(System.currentTimeMillis())
  private val oneYearAgo = tableUtils.partitionSpec.minus(today, new Window(365, TimeUnit.DAYS))

  @Test
  def testPutRequestFromGroupBy(): Unit = {
    val groupByConf = getTestGroupBy("put_request_builder_table")
    val putRequestBuilder = PutRequestBuilder.from(groupByConf, spark)
    val expectedStreamingDataset = groupByConf.streamingDataset
    val groupBy = {
      ai.chronon.spark.GroupBy
        .from(groupByConf, PartitionRange(today, today)(tableUtils), TableUtils(spark), computeDependency = true)
    }
    assertEquals(expectedStreamingDataset, putRequestBuilder.dataset)
    assertEquals(groupBy.keySchema, SparkConversions.fromChrononSchema(putRequestBuilder.keySchema))
    assertEquals(groupBy.preAggSchema, SparkConversions.fromChrononSchema(putRequestBuilder.valueSchema))
  }

  @Test
  def testBuildProxyServingInfo(): Unit = {
    val groupByConf = getTestGroupBy("build_serving_info_table")
    val groupBy = {
      ai.chronon.spark.GroupBy
        .from(groupByConf, PartitionRange(today, today)(tableUtils), TableUtils(spark), computeDependency = true)
    }
    val expectedAvroKeySchema = groupBy.keySchema.toAvroSchema("Key").toString(true)
    val expectedAvroValueSchema = groupBy.preAggSchema.toAvroSchema("Value").toString(true)
    val gbServingInfo = PutRequestBuilder.buildProxyServingInfo(groupByConf, spark)

    assertTrue(gbServingInfo.inputAvroSchema != null)
    assertEquals(groupBy.preAggSchema, SparkConversions.fromChrononSchema(gbServingInfo.valueChrononSchema))
    assertEquals(today, gbServingInfo.batchEndDate)
    assertEquals(expectedAvroKeySchema, gbServingInfo.keyAvroSchema)
    assertEquals(expectedAvroValueSchema, gbServingInfo.selectedAvroSchema)
  }

  def getTestLeftSource(tableName: String): api.Source = {
    val listSchema = List(
      Column("listing_id", api.StringType, 100),
      Column("user_id", api.StringType, 100),
      Column("ts", api.LongType, 100),
      Column("ds", StringType, 100)
    )

    val viewsTable = s"$namespace.$tableName"
    DataFrameGen.events(spark, listSchema, count = 1000, partitions = 50).save(viewsTable)

    Builders.Source.events(
      query = Builders.Query(selects = Builders.Selects("listing_id", "ts"), startPartition = oneYearAgo),
      table = viewsTable
    )
  }

  def getTestGroupBy(name: String): api.GroupBy = {
    spark.sql(s"CREATE DATABASE IF NOT EXISTS $namespace")
    val topic = "test_topic"
    val userCol = Column("user", StringType, 50)
    val vendorCol = Column("vendor", StringType, 50)
    // temporal events
    val paymentCols = Seq(userCol, vendorCol, Column("payment", LongType, 100), Column("notes", StringType, 20))
    val paymentsTable = s"$namespace.$name"
    val paymentsDf = DataFrameGen.events(spark, paymentCols, 100, 60)
    val tsColString = "ts_string"

    paymentsDf.withTimeBasedColumn(tsColString, format = "yyyy-MM-dd HH:mm:ss").save(paymentsTable)
    Builders.GroupBy(
      sources = Seq(Builders.Source.events(query = Builders.Query(), table = paymentsTable, topic = topic)),
      keyColumns = Seq("user"),
      aggregations = Seq(
        Builders.Aggregation(operation = Operation.COUNT,
                             inputColumn = "payment",
                             windows = Seq(new Window(6, TimeUnit.HOURS), new Window(14, TimeUnit.DAYS))),
        Builders.Aggregation(operation = Operation.COUNT, inputColumn = "payment"),
        Builders.Aggregation(operation = Operation.LAST, inputColumn = "payment"),
        Builders.Aggregation(operation = Operation.LAST_K, argMap = Map("k" -> "5"), inputColumn = "notes")
      ),
      metaData = Builders.MetaData(name = name, namespace = namespace)
    )
  }
}
