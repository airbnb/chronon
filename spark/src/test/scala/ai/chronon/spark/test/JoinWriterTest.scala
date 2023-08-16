package ai.chronon.spark.test

import ai.chronon.api.Builders
import ai.chronon.spark.streaming.JoinWriter
import ai.chronon.spark.{PartitionRange, SparkSessionBuilder, TableUtils}
import org.apache.spark.sql.SparkSession
import org.junit.Assert.assertTrue
import org.junit.Test

class JoinWriterTest {
  val spark: SparkSession = SparkSessionBuilder.build("JoinWriterTest", local = true)
  val namespace = "join_writer_test_ns"
  private val tableUtils = TableUtils(spark)
  private val today = tableUtils.partitionSpec.at(System.currentTimeMillis())

  @Test
  def testJoinWriter(): Unit = {
    val joinName = "parent_join_table"
    val parentGBName = "parent_gb"

    val joinSource = TestUtils.getParentJoin(spark, namespace, joinName, parentGBName)
    val query = Builders.Query(startPartition = today)
    val chainingGroupBy = TestUtils.getTestGBWithJoinSource(joinSource, query, namespace, "user_viewed_price_gb")
    // compute batch first to make sure batch table exists
    ai.chronon.spark.GroupBy
      .from(chainingGroupBy, PartitionRange(today, today)(tableUtils), TableUtils(spark), computeDependency = true)

    val kvStore = OnlineUtils.buildInMemoryKVStore(namespace)
    val mockApi = new MockApi(() => kvStore, namespace)
    val joinWriter = JoinWriter.from(chainingGroupBy, debug = true)(spark, mockApi)
    assertTrue(joinWriter != null)
    val now = System.currentTimeMillis()
    assertTrue(joinWriter.open(1, now))
  }
}
