package ai.zipline.spark.test

import ai.zipline.aggregator.test.{CStream, Column, NaiveAggregator}
import ai.zipline.aggregator.windowing.FiveMinuteResolution
import ai.zipline.api.Extensions._
import ai.zipline.api.{Aggregation, StringType, GroupBy => _, _}
import ai.zipline.spark.Extensions._
import ai.zipline.spark._
import com.google.gson.Gson
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{StructField, StructType, LongType => SparkLongType, StringType => SparkStringType}
import org.apache.spark.sql.{Row, SparkSession}
import org.junit.Assert._
import org.junit.Test
import scala.collection.mutable

class GroupByStreamingTest {

  lazy val spark: SparkSession = SparkSessionBuilder.build("GroupByTest", local = true)
  implicit val tableUtils: TableUtils = new TableUtils(spark)

  private val appName: String = "testApp"

  private val inMemoryKvStore: InMemoryKvStore = new InMemoryKvStore()

  private val mockOnlineImpl: MockOnlineImpl = new MockOnlineImpl(kvStore = inMemoryKvStore, Map.empty)

  @Test
  def testSnapshotEntities(): Unit = {
    //
    //    // Create batch dataset
    //    val schema = List(
    //      Column("user", StringType, 10),
    //      Column(Constants.TimeColumn, LongType, 100), // ts = last 100 days
    //      Column("session_length", IntType, 10000)
    //    )
    //    val df = DataFrameGen.entities(spark, schema, 100000, 10) // ds = last 10 days
    //    val viewName = "test_group_by_entities"
    //    df.createOrReplaceTempView(viewName)
    //    val aggregations: Seq[Aggregation] = Seq(
    //      Builders
    //        .Aggregation(Operation.AVERAGE, "session_length", Seq(new Window(10, TimeUnit.DAYS), WindowUtils.Unbounded)))

    //
    //    val groupBy = new GroupBy(aggregations, Seq("user"), df)
    //
    //    // upload to InMemoryKVstore
    //
    //    inMemoryKvStore.multiPut()
    //
    //    val inMemoryStream = new InMemoryStream
    //
    //
    //
    //    val groupByStreaming = new GroupByStreaming(inMemoryStream.getInMemoryStreamDF(spark, inputFile), spark, groupBy, onlineImpl = mockOnlineImpl)
    //    val expectedDf = df.sqlContext.sql(s"""
    //                                          |SELECT user,
    //                                          |       ds,
    //                                          |       AVG(IF(ts  >= (unix_timestamp(ds, 'yyyy-MM-dd') - (86400*10)) * 1000, session_length, null)) AS session_length_average_10d,
    //                                          |       AVG(session_length) as session_length_average
    //                                          |FROM $viewName
    //                                          |WHERE ts < unix_timestamp(ds, 'yyyy-MM-dd') * 1000
    //                                          |GROUP BY user, ds
    //                                          |""".stripMargin)
    //
    //    val diff = Comparison.sideBySide(actualDf, expectedDf, List("user", Constants.PartitionColumn))
    //    if (diff.count() > 0) {
    //      diff.show()
    //      println("diff result rows")
    //    }
    //    assertEquals(0, diff.count())
    //  }

  }
}
