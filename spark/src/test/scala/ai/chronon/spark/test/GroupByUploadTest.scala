package ai.chronon.spark.test

import ai.chronon.aggregator.test.Column
import ai.chronon.api.Extensions._
import ai.chronon.api._
import ai.chronon.spark.Extensions.DataframeOps
import ai.chronon.spark.{GroupByUpload, SparkSessionBuilder, TableUtils}
import org.apache.spark.sql.SparkSession
import org.junit.Test

class GroupByUploadTest {

  lazy val spark: SparkSession = SparkSessionBuilder.build("GroupByUploadTest", local = true)
  private val namespace = "group_by_upload_test"
  private val tableUtils = TableUtils(spark)

  @Test
  def temporalEventsLastKTest(): Unit = {
    val today = tableUtils.partitionSpec.at(System.currentTimeMillis())
    val yesterday = tableUtils.partitionSpec.before(today)
    tableUtils.sql(s"CREATE DATABASE IF NOT EXISTS $namespace")
    tableUtils.sql(s"USE $namespace")
    val eventsTable = "events_last_k"
    val eventSchema = List(
      Column("user", StringType, 10),
      Column("list_event", StringType, 100)
    )
    val eventDf = DataFrameGen.events(spark, eventSchema, count = 1000, partitions = 18)
    eventDf.save(s"$namespace.$eventsTable")

    val aggregations: Seq[Aggregation] = Seq(
      Builders.Aggregation(Operation.LAST_K, "list_event", Seq(WindowUtils.Unbounded), argMap = Map("k" -> "30"))
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

  @Test
  def structSupportTest(): Unit = {
    val today = tableUtils.partitionSpec.at(System.currentTimeMillis())
    val yesterday = tableUtils.partitionSpec.before(today)
    tableUtils.sql(s"CREATE DATABASE IF NOT EXISTS $namespace")
    tableUtils.sql(s"USE $namespace")
    val eventsBase = "events_source"
    val eventsTable = "events_table_struct"
    val eventSchema = List(
      Column("user", StringType, 10),
      Column("event1", IntType, 100),
      Column("event2", DoubleType, 100),
      Column("event3", LongType, 100),
      Column("event4", StringType, 100)
    )
    val eventBaseDf = DataFrameGen.events(spark, eventSchema, count = 1000, partitions = 18)
    eventBaseDf.save(s"$namespace.$eventsBase")

    val eventDf = spark.sql(s"""
      SELECT
        user
        , ts
        , ds
        , NAMED_STRUCT('event1', event1, 'event2', event2, 'nested', NAMED_STRUCT('event3', event3, 'event4', event4)) as event_struct
      FROM $namespace.$eventsBase
      """)
    eventDf.save(s"$namespace.$eventsTable")
    val aggregations: Seq[Aggregation] = Seq(
      Builders.Aggregation(Operation.LAST_K, "event_struct", Seq(WindowUtils.Unbounded), argMap = Map("k" -> "30"))
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

  @Test
  def multipleAvgCountersTest(): Unit = {
    val today = tableUtils.partitionSpec.at(System.currentTimeMillis())
    val yesterday = tableUtils.partitionSpec.before(today)
    tableUtils.sql(s"CREATE DATABASE IF NOT EXISTS $namespace")
    tableUtils.sql(s"USE $namespace")
    val eventsTable = "my_events"
    val eventSchema = List(
      Column("user", StringType, 10),
      Column("list_event", StringType, 100),
      Column("views", IntType, 10),
      Column("rating", IntType, 10)
    )
    val eventDf = DataFrameGen.events(spark, eventSchema, count = 1000, partitions = 18)
    eventDf.save(s"$namespace.$eventsTable")

    val aggregations: Seq[Aggregation] = Seq(
      Builders.Aggregation(Operation.LAST_K, "list_event", Seq(WindowUtils.Unbounded), argMap = Map("k" -> "30")),
      Builders.Aggregation(Operation.AVERAGE, "views", Seq(WindowUtils.Unbounded, new Window(1, TimeUnit.DAYS)))
    )
    val keys = Seq("user").toArray
    val groupByConf =
      Builders.GroupBy(
        sources = Seq(Builders.Source.events(Builders.Query(), table = eventsTable)),
        keyColumns = keys,
        aggregations = aggregations,
        metaData = Builders.MetaData(namespace = namespace, name = "test_multiple_avg_upload"),
        accuracy = Accuracy.TEMPORAL
      )
    GroupByUpload.run(groupByConf, endDs = yesterday)
  }
}
