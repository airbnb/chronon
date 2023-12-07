/*
 *    Copyright (C) 2023 The Chronon Authors.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package ai.chronon.spark.test

import org.slf4j.LoggerFactory
import ai.chronon.aggregator.test.Column
import ai.chronon.aggregator.windowing.TsUtils
import ai.chronon.api
import ai.chronon.api.{Builders, Constants, Operation, TimeUnit, Window}
import ai.chronon.spark.Extensions._
import ai.chronon.spark.{Comparison, Join, SparkSessionBuilder, TableUtils}
import org.apache.spark.sql.types.{BooleanType, DoubleType, IntegerType, LongType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.junit.Test

/** Tests for the temporal join of entities.
  * Left is an event source with definite ts.
  * Right is an entity with snapshots and mutation values through the day.
  * Join is the events and the entity value at the exact timestamp of the ts.
  */
class MutationsTest {
  @transient lazy val logger = LoggerFactory.getLogger(getClass)

  lazy val spark: SparkSession = SparkSessionBuilder.build(
    "MutationsTest",
    local = true,
    additionalConfig = Some(Map("spark.chronon.backfill.validation.enabled" -> "false")))

  private def namespace(suffix: String) = s"test_mutations_$suffix"
  private val groupByName = s"group_by_test.v0"
  private val joinName = s"join_test.v0"

  private implicit val tableUtils: TableUtils = TableUtils(spark)

  // {listing_id (key), ts (timestamp of property), rating (property: rated value), ds (partition ds)}
  private val snapshotSchema = StructType(
    Array(
      StructField("listing_id", IntegerType, false),
      StructField("ts", LongType, false),
      StructField("rating", IntegerType, true),
      StructField("ds", StringType, false)
    ))

  // {..., mutation_ts (timestamp of mutation), is_before (previous value or the updated value),...}
  private val mutationSchema = StructType(
    Array(
      StructField("listing_id", IntegerType, false),
      StructField("ts", LongType, false),
      StructField("rating", IntegerType, true),
      StructField("mutation_ts", LongType, false),
      StructField("is_before", BooleanType, true),
      StructField("ds", StringType, false)
    ))

  // {..., event (generic event column), ...}
  private val leftSchema = StructType(
    Array(
      StructField("listing_id", IntegerType, false),
      StructField("event", IntegerType, true),
      StructField("ts", LongType, false),
      StructField("ds", StringType, false)
    ))

  private val expectedSchema = StructType(
    Array(
      StructField("listing_id", IntegerType, false),
      StructField("ts", LongType, false),
      StructField("event", IntegerType, true),
      StructField("rating_average", DoubleType, true),
      StructField("ds", StringType, false)
    ))

  val snapshotTable = s"listing_ratings_snapshot_v0"
  val mutationTable = s"listing_ratings_mutations_v0"
  val eventTable = s"listing_events"
  val joinTable = s"${joinName.replace(".", "_")}"
  val groupByTable = s"${joinName.replace(".", "_")}_${groupByName.replace(".", "_")}"

  /**
    * Join the expected rows against the computed DataFrame and check the row count is exact.
    * @param computed Dataframe that's the output of the job.
    * @param expectedRows Rows
    * @return If the expected rows are in the dataframe.
    */
  def compareResult(computed: DataFrame, expectedRows: Seq[Row]): Boolean = {
    val df = spark.createDataFrame(spark.sparkContext.parallelize(expectedRows), expectedSchema)
    val totalExpectedRows = df.count()
    if (computed.count() != totalExpectedRows) return false
    // Join on keys that should be the same.
    val ratingAverageColumn = computed.schema.fields
      .map(_.name)
      .filter(_.contains("rating_average"))(0)
    val expectedRdd = df.rdd.map { row =>
      ((
         row.getAs[Int]("listing_id"),
         row.getAs[Long]("ts"),
         row.getAs[Int]("event"),
         row.getAs[Double]("rating_average"),
         row.getAs[String]("ds")
       ),
       1)
    }
    val computedRdd = computed.rdd.map { row =>
      ((
         row.getAs[Int]("listing_id"),
         row.getAs[Long]("ts"),
         row.getAs[Int]("event"),
         row.getAs[Double](ratingAverageColumn),
         row.getAs[String]("ds")
       ),
       2)
    }
    val joinRdd = expectedRdd.join(computedRdd)
    if (totalExpectedRows == joinRdd.count()) return true
    logger.info("Failed to assert equality!")
    logger.info("== Joined RDD (listing_id, ts, rating_average)")
    val readableRDD = joinRdd.map {
      case ((id, ts, event, avg, ds), _) => Row(id, ts, event, avg, ds)
    }
    spark.createDataFrame(readableRDD, expectedSchema).show()
    logger.info("== Expected")
    df.replaceWithReadableTime(Seq("ts"), false).show()
    logger.info("== Computed")
    computed.replaceWithReadableTime(Seq("ts"), false).show()
    false
  }

  def computeTemporalTestJoin(suffix: String,
                              eventData: Seq[Row],
                              snapshotData: Seq[Row],
                              mutationData: Seq[Row],
                              endPartition: String,
                              startPartition: String,
                              windows: Seq[Window] = null,
                              operation: Operation = Operation.AVERAGE): DataFrame = {
    val testNamespace = namespace(suffix)
    spark.sql(s"CREATE DATABASE IF NOT EXISTS $testNamespace")
    spark.createDataFrame(spark.sparkContext.parallelize(eventData), leftSchema).save(s"$testNamespace.$eventTable")
    spark
      .createDataFrame(spark.sparkContext.parallelize(snapshotData), snapshotSchema)
      .save(s"$testNamespace.$snapshotTable")
    spark
      .createDataFrame(spark.sparkContext.parallelize(mutationData), mutationSchema)
      .save(s"$testNamespace.$mutationTable")
    computeJoinFromTables(suffix, startPartition, endPartition, windows, operation)
  }

  def computeJoinFromTables(suffix: String,
                            startPartition: String,
                            endPartition: String,
                            windows: Seq[Window],
                            operation: Operation): DataFrame = {
    val testNamespace = namespace(suffix)
    val rightSource = Builders.Source.entities(
      query = Builders.Query(
        selects = Builders.Selects("listing_id", "ts", "rating"),
        startPartition = startPartition,
        endPartition = endPartition,
        mutationTimeColumn = "mutation_ts",
        reversalColumn = "is_before"
      ),
      snapshotTable = s"$testNamespace.$snapshotTable",
      mutationTable = s"$testNamespace.$mutationTable"
    )

    val leftSource =
      Builders.Source.events(
        query = Builders.Query(
          selects = Builders.Selects("listing_id", "ts", "event"),
          startPartition = startPartition
        ),
        table = s"$testNamespace.$eventTable"
      )

    val groupBy = Builders.GroupBy(
      sources = Seq(rightSource),
      keyColumns = Seq("listing_id"),
      aggregations = Seq(
        Builders.Aggregation(
          operation = operation,
          inputColumn = "rating",
          windows = windows
        )
      ),
      accuracy = api.Accuracy.TEMPORAL,
      metaData = Builders.MetaData(name = groupByName, namespace = namespace(suffix), team = "chronon")
    )

    val joinConf = Builders.Join(
      left = leftSource,
      joinParts = Seq(Builders.JoinPart(groupBy = groupBy)),
      metaData = Builders.MetaData(name = joinName, namespace = namespace(suffix), team = "chronon")
    )

    val runner = new Join(joinConf, endPartition, tableUtils)
    runner.computeJoin()
  }

  /**
    * Compute the no windows average based on the tables using pure sql
    * @return Expected Dataframe that should be returned by Chronon.
    */
  def computeSimpleAverageThroughSql(testNamespace: String): DataFrame = {
    val excludeCondition = "mutations.is_before AND mutations.ts <= queries.ts AND mutations.mutation_ts < queries.ts"
    val includeCondition = s"NOT $excludeCondition"
    val expected = spark.sql(s"""
         |WITH
         |queries AS (
         |  SELECT
         |    listing_id,
         |    event,
         |    ts,
         |    ds,
         |    DATE_FORMAT(FROM_UNIXTIME(ts/1000), 'yyyy-MM-dd') AS ds_of_ts
         |  FROM $testNamespace.$eventTable),
         |snapshot AS (
         |  SELECT
         |    listing_id,
         |    ds,
         |    DATE_FORMAT(DATE_ADD(TO_DATE(ds, 'yyyy-MM-dd'), 1), 'yyyy-MM-dd') AS shifted_ds,
         |    SUM(rating) AS rating_std,
         |    COUNT(rating) AS rating_ctd
         |  FROM $testNamespace.$snapshotTable
         |  WHERE listing_id IS NOT NULL
         |  GROUP BY
         |    listing_id,
         |    ds,
         |    3),
         |mutations AS (
         |  SELECT
         |    listing_id,
         |    ts,
         |    ds,
         |    mutation_ts,
         |    is_before,
         |    rating
         |  FROM $testNamespace.$mutationTable
         |  WHERE listing_id IS NOT NULL
         | )
         |  SELECT
         |   listing_id,
         |   event,
         |   ts,
         |   ds,
         |   (rating_std + rating_add_sum - rating_sub_sum)
         |   / (rating_ctd + rating_add_cnt - rating_sub_cnt) AS ${groupByName.replaceAll("\\.", "_")}_rating_average
         | FROM (
         |   SELECT
         |     queries.listing_id,
         |     queries.ts,
         |     queries.ds,
         |     queries.event,
         |     queries.ds_of_ts,
         |     snapshot.shifted_ds,
         |     mutations.ds AS mutation_ds,
         |     COALESCE(snapshot.rating_std, 0)      AS rating_std,
         |     COALESCE(snapshot.rating_ctd, 0)      AS rating_ctd,
         |     SUM(IF($includeCondition, mutations.rating, 0)) AS rating_add_sum,
         |     SUM(IF($excludeCondition, mutations.rating, 0)) AS rating_sub_sum,
         |     COUNT(IF($includeCondition, mutations.rating, NULL)) AS rating_add_cnt,
         |     COUNT(IF($excludeCondition, mutations.rating, NULL)) AS rating_sub_cnt
         |   FROM queries
         |   LEFT OUTER JOIN snapshot
         |     ON queries.listing_id = snapshot.listing_id
         |    AND queries.ds_of_ts = snapshot.shifted_ds
         |   LEFT OUTER JOIN mutations
         |     ON queries.listing_id = mutations.listing_id
         |    AND queries.ds_of_ts = mutations.ds
         |   GROUP BY
         |     queries.listing_id,
         |     queries.ts,
         |     queries.ds,
         |     queries.ds_of_ts,
         |     queries.event,
         |     snapshot.shifted_ds,
         |     snapshot.rating_std,
         |     mutations.ds,
         |     snapshot.rating_ctd) sq
         |   """.stripMargin)
    expected.show()
    expected
  }

  /**
    * Compute the no windows last based on the tables using pure sql
    * This helps cover the TimedAggregator part of the code.
    * @return Expected Dataframe that should be returned by Chronon.
    */
  def computeLastThroughSql(testNamespace: String): DataFrame = {
    val excludeCondition = "mutations.is_before AND mutations.ts <= queries.ts AND mutations.mutation_ts < queries.ts"
    val includeCondition = s"NOT $excludeCondition"
    val expected = spark.sql(s"""
         |WITH
         |queries AS (
         |  SELECT
         |    listing_id,
         |    event,
         |    ts,
         |    ds,
         |    DATE_FORMAT(FROM_UNIXTIME(ts/1000), 'yyyy-MM-dd') AS ds_of_ts
         |  FROM $testNamespace.$eventTable),
         |snapshot AS (
         |  SELECT
         |    sq2.listing_id,
         |    sq2.shifted_ds,
         |    MAX(IF(sq1.max_ts = sq2.ts, rating, NULL)) AS days_last
         |  FROM (
         |    SELECT
         |      listing_id,
         |      ds,
         |      MAX(ts) as max_ts
         |    FROM $testNamespace.$snapshotTable
         |    WHERE listing_id IS NOT NULL
         |    GROUP BY listing_id, ds
         |    ) sq1
         |   JOIN (
         |     SELECT
         |       listing_id,
         |       ds,
         |       DATE_FORMAT(DATE_ADD(TO_DATE(ds, 'yyyy-MM-dd'), 1), 'yyyy-MM-dd') AS shifted_ds,
         |       ts,
         |       rating
         |      FROM $testNamespace.$snapshotTable
         |    ) sq2
         |    ON sq1.listing_id = sq2.listing_id
         |   AND sq1.ds = sq2.ds
         |  GROUP BY 1, 2),
         |mutations AS (
         |  SELECT
         |    listing_id,
         |    ts,
         |    ds,
         |    mutation_ts,
         |    is_before,
         |    rating
         |  FROM $testNamespace.$mutationTable
         |  WHERE listing_id IS NOT NULL
         | )
         | SELECT
         |   listing_id
         |   , event
         |   , ts
         |   , ds
         |   , COALESCE(latest_mutation, rating_end_of_day) AS ${groupByName.replaceAll("\\.", "_")}_rating_last
         |  FROM (
         |    SELECT
         |     sq.listing_id,
         |     sq.event,
         |     sq.ts,
         |     sq.ds,
         |     sq.rating_end_of_day,
         |     MAX(IF(mutations_agg.mutation_ts = sq.last_rating_mutation_ts, mutations_agg.rating, NULL)) AS latest_mutation
         |    FROM (
         |      SELECT
         |        queries.listing_id,
         |        queries.ts,
         |        queries.ds,
         |        queries.event,
         |        queries.ds_of_ts,
         |        snapshot.shifted_ds,
         |        mutations.ds AS mutation_ds,
         |        snapshot.days_last      AS rating_end_of_day,
         |        MAX(IF($includeCondition, mutations.ts, NULL)) AS last_rating_ts,
         |        MAX(IF($includeCondition, mutations.mutation_ts, NULL)) AS last_rating_mutation_ts
         |      FROM queries
         |      LEFT OUTER JOIN snapshot
         |        ON queries.listing_id = snapshot.listing_id
         |       AND queries.ds_of_ts = snapshot.shifted_ds
         |      LEFT OUTER JOIN mutations
         |        ON queries.listing_id = mutations.listing_id
         |       AND queries.ds_of_ts = mutations.ds
         |      GROUP BY
         |        queries.listing_id,
         |        queries.ts,
         |        queries.ds,
         |        queries.ds_of_ts,
         |        queries.event,
         |        snapshot.shifted_ds,
         |        mutations.ds,
         |        snapshot.days_last) sq
         |    LEFT OUTER JOIN (
         |      SELECT
         |        ds,
         |        listing_id,
         |        ts,
         |        mutation_ts,
         |        rating
         |      FROM $testNamespace.$mutationTable
         |      WHERE listing_id IS NOT NULL AND NOT is_before
         |    ) mutations_agg
         |      ON sq.ds_of_ts = mutations_agg.ds
         |     AND sq.listing_id = mutations_agg.listing_id
         |    GROUP BY 1, 2, 3, 4, 5
         |   ) esq
         |   """.stripMargin)
    expected.show()
    expected
  }

  def compareAgainstSql(suffix: String, computed: DataFrame, operation: Operation) = {
    val testNamespace = namespace(suffix)
    val fromSql = operation match {
      case Operation.AVERAGE => computeSimpleAverageThroughSql(testNamespace)
      case Operation.LAST    => computeLastThroughSql(testNamespace)
      case _                 => null
    }
    val diff = Comparison.sideBySide(computed, fromSql, List("listing_id", "ts", "ds"))
    assert(diff.count() == 0)
  }

  /** Simplest Case:
    *
    * Inputs:
    * Left events -> generic event with listings and timestamps.
    * Right entities -> Source: listings, ratings; Agg AVG(ratings)
    *
    * Compute Join for when mutations are just insert on values.
    */
  @Test
  def testSimplestCase(): Unit = {
    val suffix = "simple"
    val eventData = Seq(
      Row(1, 1, TsUtils.datetimeToTs("2021-04-10 01:00:00"), "2021-04-10"),
      Row(1, 1, TsUtils.datetimeToTs("2021-04-10 02:30:00"), "2021-04-10"),
      Row(2, 1, TsUtils.datetimeToTs("2021-04-10 23:00:00"), "2021-04-10")
    )
    val snapshotData = Seq(
      // {listing_id, ts, rating, ds}
      Row(1, TsUtils.datetimeToTs("2021-04-04 00:30:00"), 4, "2021-04-09"),
      Row(2, TsUtils.datetimeToTs("2021-04-04 01:40:00"), 3, "2021-04-09"),
      Row(3, TsUtils.datetimeToTs("2021-04-04 06:00:00"), 4, "2021-04-09"),
      Row(1, TsUtils.datetimeToTs("2021-04-04 12:30:00"), 5, "2021-04-09"),
      Row(1, TsUtils.datetimeToTs("2021-04-05 00:30:00"), 4, "2021-04-09"),
      Row(2, TsUtils.datetimeToTs("2021-04-05 03:40:00"), 3, "2021-04-09"),
      Row(3, TsUtils.datetimeToTs("2021-04-05 04:00:00"), 4, "2021-04-09"),
      Row(1, TsUtils.datetimeToTs("2021-04-05 02:30:00"), 5, "2021-04-09")
    )
    val mutationData = Seq(
      // {listing_id, ts, rating, mutation_ts, is_before, ds}
      Row(1,
          TsUtils.datetimeToTs("2021-04-10 02:00:00"),
          2,
          TsUtils.datetimeToTs("2021-04-10 02:00:00"),
          false,
          "2021-04-10"),
      Row(2,
          TsUtils.datetimeToTs("2021-04-10 10:00:00"),
          3,
          TsUtils.datetimeToTs("2021-04-10 02:10:00"),
          false,
          "2021-04-10"),
      Row(3,
          TsUtils.datetimeToTs("2021-04-10 23:00:00"),
          4,
          TsUtils.datetimeToTs("2021-04-10 02:15:00"),
          false,
          "2021-04-10")
    )
    val (startPartition, endPartition) = ("2021-04-08", "2021-04-10")
    val result = computeTemporalTestJoin(suffix,
                                         eventData,
                                         snapshotData,
                                         mutationData,
                                         startPartition = startPartition,
                                         endPartition = endPartition)
    val expected = Seq(
      // {listing_id, ts (query), event, rating_average, ds}
      Row(1, TsUtils.datetimeToTs("2021-04-10 01:00:00"), 1, 4.5, "2021-04-10"),
      Row(1, TsUtils.datetimeToTs("2021-04-10 02:30:00"), 1, 4.0, "2021-04-10"),
      Row(2, TsUtils.datetimeToTs("2021-04-10 23:00:00"), 1, 3.0, "2021-04-10")
    )
    assert(compareResult(result, expected))
    compareAgainstSql(suffix, result, Operation.AVERAGE)
    val resultLast = computeTemporalTestJoin(suffix,
                                             eventData,
                                             snapshotData,
                                             mutationData,
                                             startPartition = startPartition,
                                             endPartition = endPartition,
                                             operation = Operation.LAST)
    compareAgainstSql(suffix, resultLast, Operation.LAST)
  }

  /** Simple Value Update Case
    *
    * Inputs:
    * Left events -> generic event with listings and timestamps.
    * Right entities -> Source: listings, ratings; Agg AVG(ratings)
    *
    * Compute Join when mutations have an update on values.
    */
  @Test
  def testUpdateValueCase(): Unit = {
    val suffix = "update_value"
    val eventData = Seq(
      // {listing_id, ts, event, ds}
      Row(1, 1, TsUtils.datetimeToTs("2021-04-10 01:00:00"), "2021-04-10"),
      Row(1, 1, TsUtils.datetimeToTs("2021-04-10 02:30:00"), "2021-04-10"),
      Row(2, 1, TsUtils.datetimeToTs("2021-04-10 23:00:00"), "2021-04-10")
    )
    val snapshotData = Seq(
      // {listing_id, ts, rating, ds}
      Row(1, TsUtils.datetimeToTs("2021-04-04 00:30:00"), 4, "2021-04-09"),
      Row(1, TsUtils.datetimeToTs("2021-04-04 12:30:00"), 5, "2021-04-09"),
      Row(1, TsUtils.datetimeToTs("2021-04-05 00:30:00"), 4, "2021-04-09"),
      Row(1, TsUtils.datetimeToTs("2021-04-05 02:30:00"), 5, "2021-04-09"),
      Row(2, TsUtils.datetimeToTs("2021-04-04 01:40:00"), 3, "2021-04-09"),
      Row(2, TsUtils.datetimeToTs("2021-04-05 03:40:00"), 3, "2021-04-09"),
      Row(3, TsUtils.datetimeToTs("2021-04-04 06:00:00"), 4, "2021-04-09"),
      Row(3, TsUtils.datetimeToTs("2021-04-05 04:00:00"), 4, "2021-04-09")
    )
    val mutationData = Seq(
      // {listing_id, ts, rating, mutation_ts, is_before, ds}
      Row(1,
          TsUtils.datetimeToTs("2021-04-05 00:30:00"),
          4,
          TsUtils.datetimeToTs("2021-04-10 02:00:00"),
          true,
          "2021-04-10"),
      Row(1,
          TsUtils.datetimeToTs("2021-04-05 00:30:00"),
          2,
          TsUtils.datetimeToTs("2021-04-10 02:00:00"),
          false,
          "2021-04-10"),
      Row(2,
          TsUtils.datetimeToTs("2021-04-10 10:00:00"),
          3,
          TsUtils.datetimeToTs("2021-04-10 02:10:00"),
          false,
          "2021-04-10"),
      Row(3,
          TsUtils.datetimeToTs("2021-04-10 23:00:00"),
          4,
          TsUtils.datetimeToTs("2021-04-10 02:15:00"),
          false,
          "2021-04-10")
    )
    val (startPartition, endPartition) = ("2021-04-08", "2021-04-10")
    val result = computeTemporalTestJoin(suffix,
                                         eventData,
                                         snapshotData,
                                         mutationData,
                                         startPartition = startPartition,
                                         endPartition = endPartition)
    val expected = Seq(
      // {listing_id, ts (query), event, rating_average, ds}
      Row(1, TsUtils.datetimeToTs("2021-04-10 01:00:00"), 1, 4.5, "2021-04-10"),
      Row(1, TsUtils.datetimeToTs("2021-04-10 02:30:00"), 1, 4.0, "2021-04-10"),
      Row(2, TsUtils.datetimeToTs("2021-04-10 23:00:00"), 1, 3.0, "2021-04-10")
    )
    assert(compareResult(result, expected))
    compareAgainstSql(suffix, result, Operation.AVERAGE)
  }

  /** Simple Key Update Case
    *
    * Inputs:
    * Left events -> generic event with listings and timestamps.
    * Right entities -> Source: listings, ratings; Agg AVG(ratings)
    *
    * Compute Join when mutations have an update on keys.
    */
  @Test
  def testUpdateKeyCase(): Unit = {
    val suffix = "update_key"
    val eventData = Seq(
      Row(1, 1, TsUtils.datetimeToTs("2021-04-10 01:00:00"), "2021-04-10"),
      Row(2, 1, TsUtils.datetimeToTs("2021-04-10 02:30:00"), "2021-04-10"),
      Row(2, 1, TsUtils.datetimeToTs("2021-04-10 23:00:00"), "2021-04-10")
    )
    val snapshotData = Seq(
      // {listing_id, ts, rating, ds}
      Row(1, TsUtils.datetimeToTs("2021-04-04 00:30:00"), 4, "2021-04-09"),
      Row(1, TsUtils.datetimeToTs("2021-04-04 12:30:00"), 4, "2021-04-09"),
      Row(1, TsUtils.datetimeToTs("2021-04-05 00:30:00"), 4, "2021-04-09"),
      Row(1, TsUtils.datetimeToTs("2021-04-05 02:30:00"), 5, "2021-04-09"),
      Row(2, TsUtils.datetimeToTs("2021-04-04 01:40:00"), 3, "2021-04-09"),
      Row(2, TsUtils.datetimeToTs("2021-04-05 03:40:00"), 3, "2021-04-09"),
      Row(2, TsUtils.datetimeToTs("2021-04-06 03:45:00"), 3, "2021-04-09"),
      Row(3, TsUtils.datetimeToTs("2021-04-04 06:00:00"), 4, "2021-04-09"),
      Row(3, TsUtils.datetimeToTs("2021-04-05 04:00:00"), 4, "2021-04-09")
    )
    val mutationData = Seq(
      // {listing_id, ts, rating, mutation_ts, is_before, ds}
      Row(1,
          TsUtils.datetimeToTs("2021-04-05 02:30:00"),
          5,
          TsUtils.datetimeToTs("2021-04-10 00:30:00"),
          true,
          "2021-04-10"),
      Row(2,
          TsUtils.datetimeToTs("2021-04-05 00:30:00"),
          5,
          TsUtils.datetimeToTs("2021-04-10 00:30:00"),
          false,
          "2021-04-10"),
      Row(2,
          TsUtils.datetimeToTs("2021-04-10 10:00:00"),
          3,
          TsUtils.datetimeToTs("2021-04-10 10:00:00"),
          false,
          "2021-04-10"),
      Row(3,
          TsUtils.datetimeToTs("2021-04-10 23:00:00"),
          4,
          TsUtils.datetimeToTs("2021-04-10 02:15:00"),
          false,
          "2021-04-10")
    )
    val (startPartition, endPartition) = ("2021-04-08", "2021-04-10")
    val result = computeTemporalTestJoin(suffix,
                                         eventData,
                                         snapshotData,
                                         mutationData,
                                         startPartition = startPartition,
                                         endPartition = endPartition)
    val expected = Seq(
      // {listing_id, ts (query), event, rating_average, ds}
      Row(1, TsUtils.datetimeToTs("2021-04-10 01:00:00"), 1, 4.0, "2021-04-10"),
      Row(2, TsUtils.datetimeToTs("2021-04-10 02:30:00"), 1, 3.5, "2021-04-10"),
      Row(2, TsUtils.datetimeToTs("2021-04-10 23:00:00"), 1, 3.4, "2021-04-10")
    )
    assert(compareResult(result, expected))
    compareAgainstSql(suffix, result, Operation.AVERAGE)
  }

  /** Case where left has a inconsistent ds/ts combination (i.e. ds contains ts outside of ds range)
    *
    * Inputs:
    * Left events -> generic event with listings and timestamps, w/ timestamps not necessarily belonging to ts.
    * Right entities -> Source: listings, ratings; Agg AVG(ratings)
    *
    * Compute Join, the aggregation should partition on dsOf[ts] and then
    * when writing the output write it into the proper ts for left.
    * The request comes at a given ds, but the mutation data needs to be consistent with the ts of the request,
    * Not with the ds of the request (else it'd be based on a snapshot that may have incorporated mutations that
    * may not have happened at the time of the request)
    * For this test we request a value for id 2, w/ mutations happening in the day before and after the time requested.
    * The consistency constraint here is that snapshot 4/8 + mutations 4/8 = snapshot 4/9
    */
  @Test
  def testInconsistentTsLeftCase(): Unit = {
    val suffix = "inconsistent_ts"
    val eventData = Seq(
      Row(1, 1, TsUtils.datetimeToTs("2021-04-10 01:00:00"), "2021-04-10"),
      Row(2, 1, TsUtils.datetimeToTs("2021-04-09 04:30:00"), "2021-04-10"),
      Row(2, 1, TsUtils.datetimeToTs("2021-04-09 06:30:00"), "2021-04-10"),
      Row(2, 1, TsUtils.datetimeToTs("2021-04-09 08:30:00"), "2021-04-10"),
      Row(2, 1, TsUtils.datetimeToTs("2021-04-10 23:00:00"), "2021-04-10")
    )
    val snapshotData = Seq(
      Row(1, TsUtils.datetimeToTs("2021-04-04 00:30:00"), 4, "2021-04-08"),
      Row(1, TsUtils.datetimeToTs("2021-04-04 12:30:00"), 4, "2021-04-08"),
      Row(1, TsUtils.datetimeToTs("2021-04-05 00:30:00"), 4, "2021-04-08"),
      Row(1, TsUtils.datetimeToTs("2021-04-05 02:30:00"), 5, "2021-04-08"),
      Row(2, TsUtils.datetimeToTs("2021-04-04 01:40:00"), 3, "2021-04-08"),
      Row(2, TsUtils.datetimeToTs("2021-04-05 03:40:00"), 3, "2021-04-08"),
      Row(2, TsUtils.datetimeToTs("2021-04-06 03:45:00"), 3, "2021-04-08"),
      Row(3, TsUtils.datetimeToTs("2021-04-04 06:00:00"), 4, "2021-04-08"),
      Row(3, TsUtils.datetimeToTs("2021-04-05 04:00:00"), 4, "2021-04-08"),
      // {listing_id, ts, rating, ds}
      Row(1, TsUtils.datetimeToTs("2021-04-04 00:30:00"), 4, "2021-04-09"),
      Row(1, TsUtils.datetimeToTs("2021-04-04 12:30:00"), 4, "2021-04-09"),
      Row(1, TsUtils.datetimeToTs("2021-04-05 00:30:00"), 4, "2021-04-09"),
      Row(1, TsUtils.datetimeToTs("2021-04-05 02:30:00"), 5, "2021-04-09"),
      Row(2, TsUtils.datetimeToTs("2021-04-04 01:40:00"), 3, "2021-04-09"),
      Row(2, TsUtils.datetimeToTs("2021-04-05 03:40:00"), 3, "2021-04-09"),
      Row(2, TsUtils.datetimeToTs("2021-04-06 03:45:00"), 3, "2021-04-09"),
      Row(2, TsUtils.datetimeToTs("2021-04-09 05:45:00"), 5, "2021-04-09"),
      Row(3, TsUtils.datetimeToTs("2021-04-04 06:00:00"), 4, "2021-04-09"),
      Row(3, TsUtils.datetimeToTs("2021-04-05 04:00:00"), 4, "2021-04-09")
    )
    val mutationData = Seq(
      Row(2,
          TsUtils.datetimeToTs("2021-04-09 05:45:00"),
          4,
          TsUtils.datetimeToTs("2021-04-09 05:45:00"),
          false,
          "2021-04-09"),
      Row(2,
          TsUtils.datetimeToTs("2021-04-09 05:45:00"),
          4,
          TsUtils.datetimeToTs("2021-04-09 07:00:00"),
          true,
          "2021-04-09"),
      Row(2,
          TsUtils.datetimeToTs("2021-04-09 05:45:00"),
          5,
          TsUtils.datetimeToTs("2021-04-09 07:00:00"),
          false,
          "2021-04-09"),
      // {listing_id, ts, rating, mutation_ts, is_before, ds}
      Row(1,
          TsUtils.datetimeToTs("2021-04-10 00:30:00"),
          5,
          TsUtils.datetimeToTs("2021-04-10 00:30:00"),
          false,
          "2021-04-10"),
      Row(2,
          TsUtils.datetimeToTs("2021-04-10 10:00:00"),
          3,
          TsUtils.datetimeToTs("2021-04-10 10:00:00"),
          false,
          "2021-04-10"),
      Row(3,
          TsUtils.datetimeToTs("2021-04-10 23:00:00"),
          4,
          TsUtils.datetimeToTs("2021-04-10 02:15:00"),
          false,
          "2021-04-10")
    )

    val (startPartition, endPartition) = ("2021-04-08", "2021-04-10")
    val result = computeTemporalTestJoin(suffix,
                                         eventData,
                                         snapshotData,
                                         mutationData,
                                         startPartition = startPartition,
                                         endPartition = endPartition)
    val expected = Seq(
      // {listing_id, ts (query), event, rating_average, ds}
      Row(1, TsUtils.datetimeToTs("2021-04-10 01:00:00"), 1, 4.4, "2021-04-10"),
      Row(2, TsUtils.datetimeToTs("2021-04-09 04:30:00"), 1, 3.0, "2021-04-10"),
      Row(2, TsUtils.datetimeToTs("2021-04-09 06:30:00"), 1, 3.25, "2021-04-10"),
      Row(2, TsUtils.datetimeToTs("2021-04-09 08:30:00"), 1, 3.5, "2021-04-10"),
      Row(2, TsUtils.datetimeToTs("2021-04-10 23:00:00"), 1, 3.4, "2021-04-10")
    )
    assert(compareResult(result, expected))
    compareAgainstSql(suffix, result, Operation.AVERAGE)
  }

  /** Case aggregation has a window that needs to decay.
    *
    * Inputs:
    * Left events -> generic event with listings and timestamps.
    * Right entities -> Source: listings, ratings; Agg AVG(ratings) Window 8 hrs
    *
    * Compute Join, the snapshot aggregation should decay, this is the main reason to have
    * resolution in snapshot IR
    */
  @Test
  def testDecayedWindowCase(): Unit = {
    val suffix = "decayed"
    val eventData = Seq(
      Row(2, 1, TsUtils.datetimeToTs("2021-04-09 01:30:00"), "2021-04-10"),
      Row(2, 1, TsUtils.datetimeToTs("2021-04-09 04:30:00"), "2021-04-10"),
      Row(2, 1, TsUtils.datetimeToTs("2021-04-09 06:30:00"), "2021-04-10"),
      Row(2, 1, TsUtils.datetimeToTs("2021-04-09 08:30:00"), "2021-04-10"),
      Row(2, 1, TsUtils.datetimeToTs("2021-04-10 09:00:00"), "2021-04-10"),
      Row(2, 1, TsUtils.datetimeToTs("2021-04-10 23:00:00"), "2021-04-10")
    )
    val snapshotData = Seq(
      Row(1, TsUtils.datetimeToTs("2021-04-04 00:30:00"), 4, "2021-04-08"),
      Row(1, TsUtils.datetimeToTs("2021-04-04 12:30:00"), 4, "2021-04-08"),
      Row(1, TsUtils.datetimeToTs("2021-04-05 00:30:00"), 4, "2021-04-08"),
      Row(1, TsUtils.datetimeToTs("2021-04-08 02:30:00"), 4, "2021-04-08"),
      Row(2, TsUtils.datetimeToTs("2021-04-04 01:40:00"), 3, "2021-04-08"),
      Row(2, TsUtils.datetimeToTs("2021-04-05 03:40:00"), 3, "2021-04-08"),
      Row(2, TsUtils.datetimeToTs("2021-04-06 03:45:00"), 4, "2021-04-08"),
      // {listing_id, ts, rating, ds}
      Row(1, TsUtils.datetimeToTs("2021-04-04 00:30:00"), 4, "2021-04-09"),
      Row(1, TsUtils.datetimeToTs("2021-04-04 12:30:00"), 4, "2021-04-09"),
      Row(1, TsUtils.datetimeToTs("2021-04-05 00:30:00"), 4, "2021-04-09"),
      Row(1, TsUtils.datetimeToTs("2021-04-08 02:30:00"), 4, "2021-04-09"),
      Row(2, TsUtils.datetimeToTs("2021-04-04 01:40:00"), 3, "2021-04-09"),
      Row(2, TsUtils.datetimeToTs("2021-04-05 03:40:00"), 3, "2021-04-09"),
      Row(2, TsUtils.datetimeToTs("2021-04-06 03:45:00"), 4, "2021-04-09"),
      Row(2, TsUtils.datetimeToTs("2021-04-09 05:45:00"), 5, "2021-04-09")
    )
    val mutationData = Seq(
      Row(2,
          TsUtils.datetimeToTs("2021-04-09 05:45:00"),
          2,
          TsUtils.datetimeToTs("2021-04-09 05:45:00"),
          false,
          "2021-04-09"),
      Row(2,
          TsUtils.datetimeToTs("2021-04-09 05:45:00"),
          2,
          TsUtils.datetimeToTs("2021-04-09 07:00:00"),
          true,
          "2021-04-09"),
      Row(2,
          TsUtils.datetimeToTs("2021-04-09 05:45:00"),
          5,
          TsUtils.datetimeToTs("2021-04-09 07:00:00"),
          false,
          "2021-04-09"),
      // {listing_id, ts, rating, mutation_ts, is_before, ds}
      Row(1,
          TsUtils.datetimeToTs("2021-04-10 00:30:00"),
          5,
          TsUtils.datetimeToTs("2021-04-10 00:30:00"),
          false,
          "2021-04-10"),
      Row(2,
          TsUtils.datetimeToTs("2021-04-10 10:00:00"),
          4,
          TsUtils.datetimeToTs("2021-04-10 10:00:00"),
          false,
          "2021-04-10")
    )

    val result = computeTemporalTestJoin(suffix,
                                         eventData,
                                         snapshotData,
                                         mutationData,
                                         startPartition = "2021-04-08",
                                         endPartition = "2021-04-10",
                                         windows = Seq(new Window(4, TimeUnit.DAYS)))
    val expected = Seq(
      // {listing_id, ts (query), event, rating_average, ds}
      // Anything after 4/5 1:30 but before 4/9 1:30 [3@4/5 3:40 | 4@4/6 3:45] -> 3.5
      Row(2, TsUtils.datetimeToTs("2021-04-09 01:30:00"), 1, 3.5, "2021-04-10"),
      // Anything after 4/5 4:30 but before 4/9 4:30 [4@4/6 3:45] -> 4.0
      Row(2, TsUtils.datetimeToTs("2021-04-09 04:30:00"), 1, 4.0, "2021-04-10"),
      // Anything after 4/5 6:30 but before 4/9 6:30 [4@4/6 3:45 | 2@4/9 5:45] (before mutation ts)
      Row(2, TsUtils.datetimeToTs("2021-04-09 06:30:00"), 1, 3.0, "2021-04-10"),
      // Anything after 4/5 8:30 but before 4/9 8:30 [4@4/6 3:45 | 5@4/9 5:45] (after mutation ts)
      Row(2, TsUtils.datetimeToTs("2021-04-09 08:30:00"), 1, 4.5, "2021-04-10"),
      // Anything after 4/6 23:00 but before 4/10 23:00 [5@4/9 5:45] -> 5.0
      Row(2, TsUtils.datetimeToTs("2021-04-10 09:00:00"), 1, 5.0, "2021-04-10"),
      // Anything after 4/6 23:00 but before 4/10 23:00 [5@4/9 5:45 | 4@4/10 10:00] -> 4.5
      Row(2, TsUtils.datetimeToTs("2021-04-10 23:00:00"), 1, 4.5, "2021-04-10")
    )
    assert(compareResult(result, expected))
  }

  /** Case aggregation has a window that needs to decay.
    *
    * Inputs:
    * Left events -> generic event with listings and timestamps.
    * Right entities -> Source: listings, ratings; Agg AVG(ratings) Window 8 hrs
    *
    * Compute Join, the snapshot aggregation should decay.
    * When there are no mutations returning the collapsed is not enough depending on the time.
    */
  @Test
  def testDecayedWindowCaseNoMutation(): Unit = {
    val suffix = "decayed_v2"
    val eventData = Seq(
      Row(2, 1, TsUtils.datetimeToTs("2021-04-10 01:00:00"), "2021-04-10"),
      Row(2, 1, TsUtils.datetimeToTs("2021-04-10 23:00:00"), "2021-04-10")
    )
    val snapshotData = Seq(
      // {listing_id, ts, rating, ds}
      Row(1, TsUtils.datetimeToTs("2021-04-04 00:30:00"), 4, "2021-04-09"),
      Row(1, TsUtils.datetimeToTs("2021-04-04 12:30:00"), 4, "2021-04-09"),
      Row(1, TsUtils.datetimeToTs("2021-04-05 00:30:00"), 4, "2021-04-09"),
      Row(1, TsUtils.datetimeToTs("2021-04-08 02:30:00"), 4, "2021-04-09"),
      Row(2, TsUtils.datetimeToTs("2021-04-04 01:40:00"), 3, "2021-04-09"),
      Row(2, TsUtils.datetimeToTs("2021-04-05 03:40:00"), 3, "2021-04-09"),
      Row(2, TsUtils.datetimeToTs("2021-04-06 03:45:00"), 4, "2021-04-09"),
      Row(2, TsUtils.datetimeToTs("2021-04-09 05:45:00"), 5, "2021-04-09")
    )
    val mutationData = Seq(
      // {listing_id, ts, rating, mutation_ts, is_before, ds}
      Row(1,
          TsUtils.datetimeToTs("2021-04-10 00:30:00"),
          5,
          TsUtils.datetimeToTs("2021-04-10 00:30:00"),
          false,
          "2021-04-10")
    )

    val result = computeTemporalTestJoin(suffix,
                                         eventData,
                                         snapshotData,
                                         mutationData,
                                         startPartition = "2021-04-09",
                                         endPartition = "2021-04-10",
                                         windows = Seq(new Window(4, TimeUnit.DAYS)))
    val expected = Seq(
      // {listing_id, ts (query), event, rating_average, ds}
      // Anything after 4/6 01:00 but before 4/10 01:00 [4@4/6 3:45, 5@4/9 5:45] -> 4.5
      Row(2, TsUtils.datetimeToTs("2021-04-10 01:00:00"), 1, 4.5, "2021-04-10"),
      // Anything after 4/6 23:00 but before 4/10 23:00 [5@4/9 5:45] -> 5.0
      Row(2, TsUtils.datetimeToTs("2021-04-10 23:00:00"), 1, 5.0, "2021-04-10")
    )
    assert(compareResult(result, expected))
  }

  /** Case aggregation has a window that needs to decay.
    *
    * Inputs:
    * Left events -> generic event with listings and timestamps.
    * Right entities -> Source: listings, ratings; Agg AVG(ratings) Window 8 hrs
    *
    * Compute Join, the snapshot aggregation should decay.
    * When there's no snapshot the value would depend only on mutations of the day.
    */
  @Test
  def testNoSnapshotJustMutation(): Unit = {
    val suffix = "no_mutation"
    val eventData = Seq(
      Row(2, 1, TsUtils.datetimeToTs("2021-04-10 00:07:00"), "2021-04-10"),
      Row(2, 1, TsUtils.datetimeToTs("2021-04-10 01:07:00"), "2021-04-10"),
      Row(2, 1, TsUtils.datetimeToTs("2021-04-10 05:00:00"), "2021-04-10"),
      Row(2, 1, TsUtils.datetimeToTs("2021-04-10 23:00:00"), "2021-04-10")
    )
    val snapshotData = Seq(
      // {listing_id, ts, rating, ds}
      Row(1, TsUtils.datetimeToTs("2021-04-04 00:30:00"), 4, "2021-04-09"),
      Row(1, TsUtils.datetimeToTs("2021-04-04 12:30:00"), 4, "2021-04-09"),
      Row(1, TsUtils.datetimeToTs("2021-04-05 00:30:00"), 4, "2021-04-09"),
      Row(1, TsUtils.datetimeToTs("2021-04-08 02:30:00"), 4, "2021-04-09")
    )
    val mutationData = Seq(
      // {listing_id, ts, rating, mutation_ts, is_before, ds}
      Row(2,
          TsUtils.datetimeToTs("2021-04-10 00:30:00"),
          5,
          TsUtils.datetimeToTs("2021-04-10 00:30:00"),
          false,
          "2021-04-10"),
      Row(2,
          TsUtils.datetimeToTs("2021-04-10 04:30:00"),
          2,
          TsUtils.datetimeToTs("2021-04-10 04:30:00"),
          false,
          "2021-04-10"),
      Row(2,
          TsUtils.datetimeToTs("2021-04-10 04:30:00"),
          2,
          TsUtils.datetimeToTs("2021-04-10 12:30:00"),
          true,
          "2021-04-10"),
      Row(2,
          TsUtils.datetimeToTs("2021-04-10 04:30:00"),
          4,
          TsUtils.datetimeToTs("2021-04-10 12:30:00"),
          false,
          "2021-04-10")
    )

    val result = computeTemporalTestJoin(suffix,
                                         eventData,
                                         snapshotData,
                                         mutationData,
                                         startPartition = "2021-04-09",
                                         endPartition = "2021-04-10",
                                         windows = Seq(new Window(4, TimeUnit.DAYS)))
    val expected = Seq(
      // {listing_id, ts (query), event, rating_average, ds}
      Row(2, TsUtils.datetimeToTs("2021-04-10 00:07:00"), 1, null, "2021-04-10"),
      Row(2, TsUtils.datetimeToTs("2021-04-10 01:07:00"), 1, 5.0, "2021-04-10"),
      Row(2, TsUtils.datetimeToTs("2021-04-10 05:00:00"), 1, 3.5, "2021-04-10"),
      Row(2, TsUtils.datetimeToTs("2021-04-10 23:00:00"), 1, 4.5, "2021-04-10")
    )
    assert(compareResult(result, expected))
  }

  @Test
  def testWithGeneratedData(): Unit = {
    val suffix = "generated"
    val reviews = List(
      Column("listing_id", api.StringType, 100),
      Column("rating", api.LongType, 100)
    )
    val events = List(
      Column("listing_id", api.StringType, 100),
      Column("event", api.IntType, 6)
    )
    val (snapshotDf, mutationsDf) = DataFrameGen.mutations(spark, reviews, 10000, 20, 0.2, 1, "listing_id")
    val (_, maxDs) = mutationsDf.range[String](tableUtils.partitionColumn)
    val (minDs, _) = snapshotDf.range[String](tableUtils.partitionColumn)
    val leftDf = DataFrameGen
      .events(spark, events, 100, 15)
      .drop()
      .withShiftedPartition(tableUtils.partitionColumn, -1)
    val testNamespace = namespace(suffix)
    spark.sql(s"CREATE DATABASE IF NOT EXISTS $testNamespace")
    snapshotDf.save(s"$testNamespace.$snapshotTable")
    mutationsDf.save(s"$testNamespace.$mutationTable")
    leftDf.save(s"$testNamespace.$eventTable")
    val result = computeJoinFromTables(suffix, minDs, maxDs, null, Operation.AVERAGE)
    val expected = computeSimpleAverageThroughSql(testNamespace)
    val diff = Comparison.sideBySide(result, expected, List("listing_id", "ts", "ds"))
    if (diff.count() > 0) {
      logger.info(s"Actual count: ${result.count()}")
      logger.info(s"Expected count: ${expected.count()}")
      logger.info(s"Diff count: ${diff.count()}")
      logger.info(s"diff result rows")
      diff.show()
      val recomputedResult = computeJoinFromTables(suffix, minDs, maxDs, null, Operation.AVERAGE)
      val recomputedDiff = Comparison.sideBySide(recomputedResult, expected, List("listing_id", "ts", "ds"))
      logger.info("Checking second run of the same data.")
      logger.info(s"recomputed diff result rows")
      recomputedDiff.show()
      assert(recomputedDiff.count() == 0)
    }
  }
}
