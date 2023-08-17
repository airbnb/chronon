package ai.chronon.spark.test

import ai.chronon.aggregator.test.Column
import ai.chronon.aggregator.windowing.TsUtils
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

  //  joinLeft = (review, category, rating)  [ratings]
  //  joinPart = (review, user, listing)     [reviews]
  // groupBy = keys:[listing, category], aggs:[avg(rating)]
  @Test
  def listingRatingCategoryJoinSourceTest(): Unit = {
    val today = tableUtils.partitionSpec.at(System.currentTimeMillis())
    val yesterday = tableUtils.partitionSpec.before(today)
    tableUtils.sql(s"CREATE DATABASE IF NOT EXISTS $namespace")
    tableUtils.sql(s"USE $namespace")

    val ratingsTable = s"${namespace}.ratings"
    def ts(arg: String) = TsUtils.datetimeToTs(arg)

    val ratingsColumns = Seq("review", "rating", "ts", "ds")
    val ratingsData = Seq(
      ("review1", 4, ts("2023-07-13 11:00"), "2023-08-14"),
      ("review2", 5, ts("07-13 12:00"), "2023-08-14"), // to delete
      ("review3", 3, ts("08-15 09:00"), "2023-08-15"), // insert
      ("review1", 2, ts("08-15 10:00"), "2023-08-15") // update
    )
    val ratingsRdd = spark.sparkContext.parallelize(ratingsData)
    val ratingsDf = spark.createDataFrame(ratingsRdd).toDF(ratingsColumns: _*)
    ratingsDf.save(ratingsTable)
    ratingsDf.show()

    val ratingsMutationsColumns = Seq("is_before", "mutation_ts", "review", "rating", "ts", "ds")
    val ratingsMutations = Seq(
      (true, ts("08-15 06:00"), "review2", 5, ts("07-13 12:00"), "2023-08-15"), // delete
      (false, ts("08-15 09:00"), "review3", 3, ts("08-15 09:00"), "2023-08-15"), // insert
      (true, ts("08-15 10:00"), "review1", 4, ts("07-13 11:00"), "2023-08-15"), // update - before
      (false, ts("08-15 10:00"), "review1", 2, ts("08-15 10:00"), "2023-08-15") // update - after
    )
    val ratingsMutationsRdd = spark.sparkContext.parallelize(ratingsMutations)
    val ratingsMutationsDf = spark.createDataFrame(ratingsMutationsRdd).toDF(ratingsMutationsColumns: _*)
    ratingsMutationsDf.save(s"${ratingsTable}_mutations")
    ratingsMutationsDf.show()

    val reviewsTable = s"${namespace}.reviews"
    val reviewsColumns = Seq("review", "listing", "ts", "ds")
    val reviewsData = Seq(
      ("review1", "listing1", ts("07-13 10:00"), "2023-08-14"),
      ("review2", "listing1", ts("07-13 11:00"), "2023-08-14"), // delete (next day)
      ("review3", "listing2", ts("08-15 08:00"), "2023-08-15") // insert
    )
    val reviewsRdd = spark.sparkContext.parallelize(reviewsData)
    val reviewsDf = spark.createDataFrame(ratingsRdd).toDF(reviewsColumns: _*)
    reviewsDf.save(reviewsTable)
    reviewsDf.show()

    val reviewsMutationsColumns = Seq("is_before", "mutation_ts", "review", "listing", "ts", "ds")
    val reviewsMutations = Seq(
      (true, ts("08-15 06:00"), "review2", "listing1", ts("07-13 11:00"), "2023-08-15"), // delete
      (false, ts("08-15 08:00"), "review3", "listing2", ts("08-15 08:00"), "2023-08-15") // insert
    )
    val reviewsMutationsRdd = spark.sparkContext.parallelize(reviewsMutations)
    val reviewsMutationsDf = spark.createDataFrame(reviewsMutationsRdd).toDF(reviewsMutationsColumns: _*)
    reviewsMutationsDf.save(s"${reviewsTable}_mutations")
    reviewsMutationsDf.show()

    val leftRatings =
      Builders.Source.entities(
        Builders.Query(selects = Builders.Selects("review", "rating", "ts")),
        snapshotTable = ratingsTable,
        mutationTopic = s"${ratingsTable}_mutations",
        mutationTable = s"${ratingsTable}_mutations"
      )

    val rightReviewPart = Builders.JoinPart(
      groupBy = Builders.GroupBy(
        metaData = Builders.MetaData(namespace = namespace, name = "review_attrs"),
        sources = Seq(
          Builders.Source.entities(
            Builders.Query(selects = Builders.Selects("review", "listing", "ts")),
            snapshotTable = reviewsTable,
            mutationTopic = s"${reviewsTable}_mutations",
            mutationTable = s"${reviewsTable}_mutations"
          )),
        keyColumns = collection.Seq("review"),
        aggregations = Seq(
          Builders.Aggregation(
            operation = Operation.LAST,
            inputColumn = "listing"
          ))
      )
    )

    val listingRatingGroupBy = Builders.GroupBy(
      metaData = Builders.MetaData(namespace = namespace, name = "listing_ratings"),
      sources = Seq(
        Builders.Source.joinSource(
          join = Builders.Join(
            metaData = Builders.MetaData(namespace = namespace, name = "review_enrichment"),
            left = leftRatings,
            joinParts = Seq(rightReviewPart)
          ),
          query = Builders.Query(selects = Builders.Selects("review", "review_attrs_listing_last", "rating", "ts"))
        )),
      keyColumns = collection.Seq("review_attrs_listing_last"),
      aggregations = Seq(
        Builders.Aggregation(
          operation = Operation.AVERAGE,
          inputColumn = "rating"
        ))
    )

    // batch upload with endDs = yesterday
    GroupByUpload.run(listingRatingGroupBy, endDs = yesterday, jsonPercent = 100, showDf = true)
    // TODO write equivalent spark sql to verify that the join table is as expected

    // streaming put of all events in ds >= today
    OnlineUtils.putStreamingNew(session = spark,
                                originalGroupByConf = listingRatingGroupBy,
                                ds = today,
                                namespace = namespace)

  }
}
