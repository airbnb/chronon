package ai.zipline.spark.test

import ai.zipline.aggregator.test.Column
import ai.zipline.api.Extensions._
import ai.zipline.api.{Aggregation, StringType, GroupBy => _, _}
import ai.zipline.spark._
import ai.zipline.spark.Extensions.DataframeOps
import org.apache.spark.sql.SparkSession
import org.junit.Test

class GroupByUploadTest {

  lazy val spark: SparkSession = SparkSessionBuilder.build("GroupByUploadTest", local = true)
  private val namespace = "group_by_upload_test"
  private val tableUtils = TableUtils(spark)

  @Test
  def temporalEventsLastKTest(): Unit = {
    val today = Constants.Partition.at(System.currentTimeMillis())
    val yesterday = Constants.Partition.before(today)
    tableUtils.sql(s"CREATE DATABASE IF NOT EXISTS $namespace")
    tableUtils.sql(s"USE $namespace")
    val eventsTable = "events_last_k"
    val eventSchema = List(
      Column("user", StringType, 10),
      Column("list_event", StringType, 100),
      Column("tiny", ByteType, 10)
    )
    val eventDf = DataFrameGen.events(spark, eventSchema, count = 1000, partitions = 18)
    eventDf.save(s"$namespace.$eventsTable")

    val aggregations: Seq[Aggregation] = Seq(
      Builders.Aggregation(Operation.LAST_K, "list_event", Seq(WindowUtils.Unbounded), argMap = Map("k" -> "30")),
      Builders.Aggregation(Operation.LAST, "tiny", Seq(WindowUtils.Unbounded))
    )
    val keys = Seq("user").toArray
    val groupByConf =
      Builders.GroupBy(
        sources = Seq(Builders.Source.events(Builders.Query(), table = eventsTable)),
        keyColumns = keys,
        aggregations = aggregations,
        metaData = Builders.MetaData(namespace = namespace, name = "test_last_k_upload"),
        accuracy = Accuracy.TEMPORAL
      )
    GroupByUpload.run(groupByConf, endDs = yesterday)
  }
}
