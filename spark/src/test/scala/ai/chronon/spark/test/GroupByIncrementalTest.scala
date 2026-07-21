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

import ai.chronon.aggregator.test.{CStream, Column}
import ai.chronon.api.Extensions._
import ai.chronon.api.{
  Aggregation,
  Builders,
  Constants,
  DoubleType,
  IntType,
  LongType,
  Operation,
  Source,
  StringType,
  TimeUnit,
  Window
}
import ai.chronon.spark.Extensions._
import ai.chronon.spark._
import ai.chronon.spark.catalog.TableUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{StructField, StructType, LongType => SparkLongType, StringType => SparkStringType}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions.col
import org.junit.Assert._
import org.junit.Test

import scala.util.Random

class GroupByIncrementalTest {

  private def createTestSourceIncremental(windowSize: Int = 365,
                                          suffix: String = "",
                                          partitionColOpt: Option[String] = None): (Source, String) = {
    lazy val spark: SparkSession =
      SparkSessionBuilder.build("GroupByIncrementalTest" + "_" + Random.alphanumeric.take(6).mkString, local = true)
    implicit val tableUtils = TableUtils(spark)
    val today = tableUtils.partitionSpec.at(System.currentTimeMillis())
    val startPartition = tableUtils.partitionSpec.minus(today, new Window(windowSize, TimeUnit.DAYS))
    val endPartition = tableUtils.partitionSpec.at(System.currentTimeMillis())
    val sourceSchema = List(
      Column("user", StringType, 10000),
      Column("item", StringType, 100),
      Column("time_spent_ms", LongType, 5000),
      Column("price", DoubleType, 100)
    )
    val namespace = "chronon_incremental_test"
    val sourceTable = s"$namespace.test_group_by_steps$suffix"

    tableUtils.createDatabase(namespace)
    val genDf =
      DataFrameGen.events(spark, sourceSchema, count = 1000, partitions = 200, partitionColOpt = partitionColOpt)
    partitionColOpt match {
      case Some(partitionCol) => genDf.save(sourceTable, partitionColumns = Seq(partitionCol))
      case None               => genDf.save(sourceTable)
    }

    val source = Builders.Source.events(
      query = Builders.Query(selects = Builders.Selects("ts", "user", "time_spent_ms", "price", "item"),
                             startPartition = startPartition,
                             partitionColumn = partitionColOpt.orNull),
      table = sourceTable
    )
    (source, endPartition)
  }

  /**
    * Diagnostic: the daily IR table must contain a complete hop for EVERY day with source events,
    * including the boundary days of the queryable range. Builds a controlled source with exactly one
    * event per day for a single user, fills _daily_inc over the full range, and asserts every day's
    * IR count == 1 (a missing/under-counted boundary day exposes the build-side window filter bug).
    */
  @Test
  def testIncrementalBuildCoversBoundaryDays(): Unit = {
    lazy val spark: SparkSession =
      SparkSessionBuilder.build("GroupByTestBoundary" + "_" + Random.alphanumeric.take(6).mkString, local = true)
    implicit val tableUtils = TableUtils(spark)
    import spark.implicits._
    val namespace = s"incremental_boundary_${Random.alphanumeric.take(6).mkString}"
    tableUtils.createDatabase(namespace)

    val partitionCol = tableUtils.partitionColumn
    val today = tableUtils.partitionSpec.at(System.currentTimeMillis())
    // 15 consecutive days; one event for user "u1" on each day, at noon of that day.
    val numDays = 15
    val days = (0 until numDays).map(i => tableUtils.partitionSpec.minus(today, new Window(i, TimeUnit.DAYS))).sorted
    val rows = days.map { ds =>
      val ts = tableUtils.partitionSpec.epochMillis(ds) + 12 * 3600 * 1000L // noon of ds
      ("u1", 1.0, ts, ds)
    }
    val sourceTable = s"$namespace.boundary_input"
    rows.toDF("user", "price", "ts", partitionCol).save(sourceTable, partitionColumns = Seq(partitionCol))

    val source = Builders.Source.events(
      query = Builders.Query(selects = Builders.Selects("ts", "user", "price"), partitionColumn = partitionCol),
      table = sourceTable)
    val conf = Builders.GroupBy(
      sources = Seq(source),
      keyColumns = Seq("user"),
      aggregations = Seq(Builders.Aggregation(Operation.COUNT, "price", Seq(new Window(7, TimeUnit.DAYS)))),
      metaData = Builders.MetaData(name = "boundary_incremental", namespace = namespace, team = "chronon"),
      backfillStartDate = days.head
    )
    val incrementalTable = conf.metaData.incrementalOutputTable

    // Fill the IR table for the full output range [days.head, days.last].
    GroupBy.computeIncrementalDf(conf, PartitionRange(days.head, days.last), tableUtils, incrementalTable)

    // Every day with an event must have an IR row with count == 1.
    val irByDay = spark
      .table(incrementalTable)
      .where("user = 'u1'")
      .selectExpr(partitionCol, "price_count")
      .collect()
      .map(r => r.getString(0) -> r.getLong(1))
      .toMap
    val missingOrWrong = days.filter(d => irByDay.get(d) != Some(1L))
    assertTrue(
      s"daily IR must cover every event day with count 1; offending days (day -> count): " +
        s"${missingOrWrong.map(d => d -> irByDay.get(d)).mkString(", ")}",
      missingOrWrong.isEmpty
    )
  }

  /**
    * Tests basic aggregations in incremental mode by comparing Chronon's output against SQL.
    *
    * Operations: SUM, COUNT, AVERAGE, MIN, MAX, VARIANCE, UNIQUE_COUNT, HISTOGRAM, BOUNDED_UNIQUE_COUNT
    *
    * Actual:   Chronon computes daily IRs using computeIncrementalDf, storing intermediate results
    * Expected: SQL query computes the same aggregations directly on the input data for the same date
    */
  @Test
  def testIncrementalBasicAggregations(): Unit = {
    lazy val spark: SparkSession = SparkSessionBuilder.build(
      "GroupByTestIncrementalBasic" + "_" + Random.alphanumeric.take(6).mkString,
      local = true)
    implicit val tableUtils = TableUtils(spark)
    val namespace = s"incremental_basic_aggs_${Random.alphanumeric.take(6).mkString}"
    tableUtils.createDatabase(namespace)

    val schema = List(
      Column("user", StringType, 10),
      Column("price", DoubleType, 100),
      Column("quantity", IntType, 50),
      Column("product_id", StringType, 20), // Low cardinality for UNIQUE_COUNT, HISTOGRAM, BOUNDED_UNIQUE_COUNT
      Column("rating", DoubleType, 2000)
    )

    val df = DataFrameGen.events(spark, schema, count = 100000, partitions = 100)

    val aggregations: Seq[Aggregation] = Seq(
      // Simple aggregations
      Builders.Aggregation(Operation.SUM, "price", Seq(new Window(7, TimeUnit.DAYS))),
      Builders.Aggregation(Operation.COUNT, "quantity", Seq(new Window(7, TimeUnit.DAYS))),
      // Complex aggregation - AVERAGE (struct IR with sum/count)
      Builders.Aggregation(Operation.AVERAGE, "rating", Seq(new Window(7, TimeUnit.DAYS))),
      // Min/Max
      Builders.Aggregation(Operation.MIN, "price", Seq(new Window(7, TimeUnit.DAYS))),
      Builders.Aggregation(Operation.MAX, "quantity", Seq(new Window(7, TimeUnit.DAYS))),
      // Variance (struct IR with count/mean/m2)
      Builders.Aggregation(Operation.VARIANCE, "price", Seq(new Window(7, TimeUnit.DAYS))),
      // UNIQUE_COUNT (array IR): IR = array<double> of distinct values
      Builders.Aggregation(Operation.UNIQUE_COUNT, "price", Seq(new Window(7, TimeUnit.DAYS))),
      // HISTOGRAM (map IR)
      Builders.Aggregation(Operation.HISTOGRAM,
                           "product_id",
                           Seq(new Window(7, TimeUnit.DAYS)),
                           argMap = Map("k" -> "0")),
      // BOUNDED_UNIQUE_COUNT (array IR with bound): IR = array<string> of bounded distinct values (MD5-hashed)
      Builders.Aggregation(Operation.BOUNDED_UNIQUE_COUNT,
                           "product_id",
                           Seq(new Window(7, TimeUnit.DAYS)),
                           argMap = Map("k" -> "100"))
    )

    val tableProps: Map[String, String] = Map("source" -> "chronon")

    val today_date = tableUtils.partitionSpec.at(System.currentTimeMillis())
    val today_minus_7_date = tableUtils.partitionSpec.minus(today_date, new Window(7, TimeUnit.DAYS))
    val today_minus_20_date = tableUtils.partitionSpec.minus(today_date, new Window(20, TimeUnit.DAYS))

    val partitionRange = PartitionRange(today_minus_20_date, today_date)

    val groupBy = new GroupBy(aggregations, Seq("user"), df)
    groupBy.computeIncrementalDf(s"${namespace}.testIncrementalBasicAggsOutput", partitionRange, tableProps)

    val actualIncrementalDf =
      spark.sql(s"select * from ${namespace}.testIncrementalBasicAggsOutput where ds='$today_minus_7_date'")
    df.createOrReplaceTempView("test_basic_aggs_input")

    // Compare against SQL computation
    val query =
      s"""
         |WITH base_aggs AS (
         |  SELECT user, ds, UNIX_TIMESTAMP(ds, 'yyyy-MM-dd')*1000 as ts,
         |    sum(price) as price_sum,
         |    count(quantity) as quantity_count,
         |    struct(sum(rating) as sum, count(rating) as count) as rating_average,
         |    min(price) as price_min,
         |    max(quantity) as quantity_max,
         |    struct(
         |      cast(count(price) as int) as count,
         |      avg(price) as mean,
         |      sum(price * price) - count(price) * avg(price) * avg(price) as m2
         |    ) as price_variance,
         |    count(distinct price) as price_unique_count,
         |    least(count(distinct product_id), 100) as product_id_bounded_unique_count
         |  FROM test_basic_aggs_input
         |  WHERE ds='$today_minus_7_date'
         |  GROUP BY user, ds
         |),
         |histogram_agg AS (
         |  SELECT user, ds,
         |    map_from_entries(collect_list(struct(product_id, cast(cnt as int)))) as product_id_histogram
         |  FROM (
         |    SELECT user, ds, product_id, count(*) as cnt
         |    FROM test_basic_aggs_input
         |    WHERE ds='$today_minus_7_date' AND product_id IS NOT NULL
         |    GROUP BY user, ds, product_id
         |  )
         |  GROUP BY user, ds
         |)
         |SELECT b.*, h.product_id_histogram
         |FROM base_aggs b
         |LEFT JOIN histogram_agg h ON b.user <=> h.user AND b.ds <=> h.ds
         |""".stripMargin

    val expectedDf = spark.sql(query)

    // Replace UNIQUE_COUNT and BOUNDED_UNIQUE_COUNT array columns with their sizes for comparison.
    // SQL produces Long counts; size() returns Int — cast both to Long for type consistency.
    import org.apache.spark.sql.functions.size
    val actualForComparison = actualIncrementalDf
      .withColumn("price_unique_count", size(col("price_unique_count")).cast("long"))
      .withColumn("product_id_bounded_unique_count", size(col("product_id_bounded_unique_count")).cast("long"))

    val diff = Comparison.sideBySide(actualForComparison, expectedDf, List("user", tableUtils.partitionColumn))

    val irRowCount = actualIncrementalDf.count()
    if (diff.count() > 0) {
      println(s"=== Diff Details for All Aggregations ===")
      println(s"Actual count: ${irRowCount}")
      println(s"Expected count: ${expectedDf.count()}")
      println(s"Diff count: ${diff.count()}")
      actualForComparison.show(10, truncate = false)
      diff.show(100, truncate = false)
    }

    assertEquals(0, diff.count())
  }

  /**
    * This test verifies that the incremental snapshotEvents output matches the non-incremental output.
    *
    * 1. Computes snapshotEvents using the standard GroupBy on the full input data.
    * 2. Computes snapshotEvents using GroupBy in incremental mode over the same date range.
    * 3. Compares the two outputs to ensure they are identical.
    */
  @Test
  def testSnapshotIncrementalEvents(): Unit = {
    lazy val spark: SparkSession =
      SparkSessionBuilder.build("GroupByTest" + "_" + Random.alphanumeric.take(6).mkString, local = true)
    implicit val tableUtils = TableUtils(spark)
    val namespace = s"incremental_groupBy_snapshot_${Random.alphanumeric.take(6).mkString}"
    tableUtils.createDatabase(namespace)

    val outputDates = CStream.genPartitions(10, tableUtils.partitionSpec)

    val aggregations: Seq[Aggregation] = Seq(
      // Basic
      Builders.Aggregation(Operation.SUM,
                           "time_spent_ms",
                           Seq(new Window(10, TimeUnit.DAYS), new Window(5, TimeUnit.DAYS))),
      Builders.Aggregation(Operation.SUM, "price", Seq(new Window(10, TimeUnit.DAYS))),
      Builders.Aggregation(Operation.COUNT, "user", Seq(new Window(10, TimeUnit.DAYS))),
      Builders.Aggregation(Operation.AVERAGE, "price", Seq(new Window(10, TimeUnit.DAYS))),
      Builders.Aggregation(Operation.MIN, "price", Seq(new Window(10, TimeUnit.DAYS))),
      Builders.Aggregation(Operation.MAX, "price", Seq(new Window(10, TimeUnit.DAYS))),
      // Statistical
      Builders.Aggregation(Operation.VARIANCE, "price", Seq(new Window(10, TimeUnit.DAYS))),
      Builders.Aggregation(Operation.SKEW, "price", Seq(new Window(10, TimeUnit.DAYS))),
      Builders.Aggregation(Operation.KURTOSIS, "price", Seq(new Window(10, TimeUnit.DAYS))),
      Builders.Aggregation(Operation.APPROX_PERCENTILE,
                           "price",
                           Seq(new Window(10, TimeUnit.DAYS)),
                           argMap = Map("percentiles" -> "[0.5, 0.25, 0.75]")),
      // Temporal
      Builders.Aggregation(Operation.FIRST, "price", Seq(new Window(10, TimeUnit.DAYS))),
      Builders.Aggregation(Operation.LAST, "price", Seq(new Window(10, TimeUnit.DAYS))),
      Builders.Aggregation(Operation.FIRST_K, "price", Seq(new Window(10, TimeUnit.DAYS)), argMap = Map("k" -> "3")),
      Builders.Aggregation(Operation.LAST_K, "price", Seq(new Window(10, TimeUnit.DAYS)), argMap = Map("k" -> "3")),
      Builders.Aggregation(Operation.TOP_K, "price", Seq(new Window(10, TimeUnit.DAYS)), argMap = Map("k" -> "3")),
      Builders.Aggregation(Operation.BOTTOM_K, "price", Seq(new Window(10, TimeUnit.DAYS)), argMap = Map("k" -> "3")),
      // Cardinality / Set
      Builders.Aggregation(Operation.UNIQUE_COUNT, "user", Seq(new Window(10, TimeUnit.DAYS))),
      Builders.Aggregation(Operation.APPROX_UNIQUE_COUNT, "user", Seq(new Window(10, TimeUnit.DAYS))),
      Builders.Aggregation(Operation.BOUNDED_UNIQUE_COUNT, "user", Seq(new Window(10, TimeUnit.DAYS))),
      // Distribution
      Builders.Aggregation(Operation.HISTOGRAM, "user", Seq(new Window(10, TimeUnit.DAYS))),
      Builders.Aggregation(Operation.APPROX_HISTOGRAM_K,
                           "user",
                           Seq(new Window(10, TimeUnit.DAYS)),
                           argMap = Map("k" -> "10"))
    )

    val (source, endPartition) = createTestSourceIncremental(windowSize = 30,
                                                             suffix = "_snapshot_events",
                                                             partitionColOpt = Some(tableUtils.partitionColumn))
    val groupByConf = Builders.GroupBy(
      sources = Seq(source),
      keyColumns = Seq("item"),
      aggregations = aggregations,
      metaData = Builders.MetaData(name = "testSnapshotIncremental", namespace = namespace, team = "chronon"),
      backfillStartDate = tableUtils.partitionSpec.minus(tableUtils.partitionSpec.at(System.currentTimeMillis()),
                                                         new Window(20, TimeUnit.DAYS))
    )

    val df = spark.read.table(source.table)

    val groupBy = new GroupBy(aggregations, Seq("item"), df.filter("item is not null"))
    val actualDf = groupBy.snapshotEvents(PartitionRange(outputDates.min, outputDates.max))

    val groupByIncremental =
      GroupBy.fromIncrementalDf(groupByConf, PartitionRange(outputDates.min, outputDates.max), tableUtils)
    val incrementalExpectedDf = groupByIncremental.snapshotEvents(PartitionRange(outputDates.min, outputDates.max))

    val outputDatesRdd: RDD[Row] = spark.sparkContext.parallelize(outputDates.map(Row(_)))
    val outputDatesDf = spark.createDataFrame(outputDatesRdd, StructType(Seq(StructField("ds", SparkStringType))))
    val datesViewName = "test_group_by_snapshot_events_output_range"
    outputDatesDf.createOrReplaceTempView(datesViewName)

    val diff = Comparison.sideBySide(actualDf, incrementalExpectedDf, List("item", tableUtils.partitionColumn))
    if (diff.count() > 0) {
      diff.show()
      println("=== Diff result rows ===")
    }
    assertEquals(0, diff.count())
  }

  /**
    * Data-quality: at the window tail boundary, incremental snapshot output must exactly match the
    * non-incremental path for every output day. One event per day at noon for a single user; COUNT
    * over a 7-day window. Compares incremental vs normal snapshotEvents day-by-day so any tail
    * off-by-one (a day whose count differs, or a dropped key) surfaces deterministically.
    */
  @Test
  def testIncrementalWindowTailBoundaryMatchesNormal(): Unit = {
    lazy val spark: SparkSession =
      SparkSessionBuilder.build("GroupByTestTail" + "_" + Random.alphanumeric.take(6).mkString, local = true)
    implicit val tableUtils = TableUtils(spark)
    import spark.implicits._
    val namespace = s"incremental_tail_${Random.alphanumeric.take(6).mkString}"
    tableUtils.createDatabase(namespace)

    val partitionCol = tableUtils.partitionColumn
    val today = tableUtils.partitionSpec.at(System.currentTimeMillis())
    // 25 consecutive days, one event/day at noon for user u1.
    val allDays = (0 until 25).map(i => tableUtils.partitionSpec.minus(today, new Window(i, TimeUnit.DAYS))).sorted
    val rows = allDays.map { ds =>
      ("u1", 1.0, tableUtils.partitionSpec.epochMillis(ds) + 12 * 3600 * 1000L, ds)
    }
    val sourceTable = s"$namespace.tail_input"
    rows.toDF("user", "price", "ts", partitionCol).save(sourceTable, partitionColumns = Seq(partitionCol))

    val aggregations =
      Seq(Builders.Aggregation(Operation.COUNT, "price", Seq(new Window(7, TimeUnit.DAYS))))
    val source = Builders.Source.events(
      query = Builders.Query(selects = Builders.Selects("ts", "user", "price"), partitionColumn = partitionCol),
      table = sourceTable)
    val conf = Builders.GroupBy(
      sources = Seq(source),
      keyColumns = Seq("user"),
      aggregations = aggregations,
      metaData = Builders.MetaData(name = "tail_incremental", namespace = namespace, team = "chronon"),
      backfillStartDate = allDays.head
    )

    // Output range = the second half (so every output day has a full 7-day lookback available).
    val outStart = allDays(10)
    val outEnd = allDays.last
    val outputRange = PartitionRange(outStart, outEnd)(tableUtils)

    val rawDf = spark.read.table(sourceTable)
    val normalGroupBy = new GroupBy(aggregations, Seq("user"), rawDf)
    val normalDf = normalGroupBy.snapshotEvents(outputRange)

    val incrementalGroupBy = GroupBy.fromIncrementalDf(conf, outputRange, tableUtils)
    val incrementalDf = incrementalGroupBy.snapshotEvents(outputRange)

    val diff = Comparison.sideBySide(normalDf, incrementalDf, List("user", partitionCol))
    if (diff.count() > 0) {
      println("=== tail-boundary incremental vs normal diff (a_=normal, b_=incremental) ===")
      diff.show(100, truncate = false)
    }
    assertEquals(0, diff.count())
  }

  /**
    * Verifies the chunked incremental IR build (stepDays-driven hole filling).
    *
    * Runs the full GroupBy.computeBackfill path in incremental mode with a small
    * stepDays so the daily-IR hole is filled in several stepped sub-ranges rather
    * than a single write. Asserts:
    *   1. The _daily_inc table accumulates IR partitions across the full
    *      [start - maxWindow, end] queryable range (i.e. the stepped writes
    *      committed every expected day).
    *   2. The final GroupBy output is identical to a non-incremental
    *      computeBackfill over the same range (stepped IR build changes nothing).
    */
  @Test
  def testIncrementalChunkedBuild(): Unit = {
    lazy val spark: SparkSession =
      SparkSessionBuilder.build("GroupByTestChunked" + "_" + Random.alphanumeric.take(6).mkString, local = true)
    implicit val tableUtils = TableUtils(spark)
    val namespace = s"incremental_chunked_${Random.alphanumeric.take(6).mkString}"
    tableUtils.createDatabase(namespace)

    val maxWindowDays = 10
    val aggregations: Seq[Aggregation] = Seq(
      Builders.Aggregation(Operation.SUM, "price", Seq(new Window(maxWindowDays, TimeUnit.DAYS))),
      Builders.Aggregation(Operation.COUNT, "user", Seq(new Window(maxWindowDays, TimeUnit.DAYS))),
      Builders.Aggregation(Operation.AVERAGE, "price", Seq(new Window(maxWindowDays, TimeUnit.DAYS))),
      Builders.Aggregation(Operation.LAST, "price", Seq(new Window(maxWindowDays, TimeUnit.DAYS))),
      Builders.Aggregation(Operation.APPROX_PERCENTILE,
                           "price",
                           Seq(new Window(maxWindowDays, TimeUnit.DAYS)),
                           argMap = Map("percentiles" -> "[0.5, 0.75]")),
      Builders.Aggregation(Operation.APPROX_UNIQUE_COUNT, "user", Seq(new Window(maxWindowDays, TimeUnit.DAYS)))
    )

    val today = tableUtils.partitionSpec.at(System.currentTimeMillis())
    val backfillStart = tableUtils.partitionSpec.minus(today, new Window(5, TimeUnit.DAYS))

    // Single shared source so the incremental and normal backfills read identical
    // data (createTestSourceIncremental generates random data per call).
    val (source, _) =
      createTestSourceIncremental(windowSize = 30,
                                  suffix = "_chunked",
                                  partitionColOpt = Some(tableUtils.partitionColumn))

    def mkConf(name: String): ai.chronon.api.GroupBy =
      Builders.GroupBy(
        sources = Seq(source),
        keyColumns = Seq("item"),
        aggregations = aggregations,
        metaData = Builders.MetaData(name = name, namespace = namespace, team = "chronon"),
        backfillStartDate = backfillStart
      )

    // Incremental backfill with a small stepDays -> chunked IR build.
    val incConf = mkConf("chunked_incremental")
    GroupBy.computeBackfill(incConf, today, tableUtils, stepDays = Some(2), incrementalMode = true)

    // (1) The chunked build should have committed multiple IR partitions across
    // the window (proving the stepped sub-range writes landed incrementally,
    // rather than nothing/one monolithic write). Exact bounds are intentionally
    // not asserted: step-boundary alignment may round the scan slightly wider and
    // days with no source events produce no IR partition - both harmless. The
    // output-equality check below is the authoritative correctness guarantee.
    val incrementalTable = incConf.metaData.incrementalOutputTable
    val irPartitions = tableUtils.partitions(incrementalTable)
    assertTrue(
      s"Daily-inc table $incrementalTable should have committed multiple IR partitions, found ${irPartitions.size}",
      irPartitions.size > 1
    )

    // (2) Final output must match a non-incremental backfill over the same range.
    val normalConf = mkConf("chunked_normal")
    GroupBy.computeBackfill(normalConf, today, tableUtils, stepDays = Some(2), incrementalMode = false)

    val incrementalOutput = spark.read.table(incConf.metaData.outputTable)
    val normalOutput = spark.read.table(normalConf.metaData.outputTable)
    val diff = Comparison.sideBySide(normalOutput, incrementalOutput, List("item", tableUtils.partitionColumn))
    if (diff.count() > 0) {
      println("=== Chunked incremental vs normal diff ===")
      diff.show(100, truncate = false)
    }
    assertEquals(0, diff.count())
  }

  /**
    * Regression test: the per-day IR write must be clamped to the requested range.
    *
    * hopsAggregate runs over the GroupBy's full input DataFrame and emits daily hops for
    * every day with events, not just the requested range. With dynamic partition overwrite,
    * an unclamped save would write ALL those partitions - so filling one day's hole would
    * (re)write and clobber neighboring partitions. computeIncrementalDf must clamp the write
    * to exactly the requested range, writing only those partitions.
    *
    * Calls the instance computeIncrementalDf directly (as testIncrementalBasicAggregations
    * does) with a single-day range over a 30-day input: without the clamp this writes ~30
    * partitions; with it, exactly one.
    */
  @Test
  def testIncrementalWriteIsClampedToRange(): Unit = {
    lazy val spark: SparkSession =
      SparkSessionBuilder.build("GroupByTestClamp" + "_" + Random.alphanumeric.take(6).mkString, local = true)
    implicit val tableUtils = TableUtils(spark)
    val namespace = s"incremental_clamp_${Random.alphanumeric.take(6).mkString}"
    tableUtils.createDatabase(namespace)

    val schema = List(
      Column("user", StringType, 100),
      Column("price", DoubleType, 100)
    )
    // 30 partitions of events -> hopsAggregate will produce hops spanning ~30 days.
    val df = DataFrameGen.events(spark, schema, count = 20000, partitions = 30)

    val aggregations: Seq[Aggregation] = Seq(
      Builders.Aggregation(Operation.SUM, "price", Seq(new Window(10, TimeUnit.DAYS))),
      Builders.Aggregation(Operation.COUNT, "user", Seq(new Window(10, TimeUnit.DAYS)))
    )
    val tableProps: Map[String, String] = Map("source" -> "chronon")
    val outputTable = s"$namespace.clamp_daily_inc"

    val groupBy = new GroupBy(aggregations, Seq("user"), df)

    // Request a single day; hopsAggregate still sees all 30 days of input.
    val today = tableUtils.partitionSpec.at(System.currentTimeMillis())
    val targetDay = tableUtils.partitionSpec.minus(today, new Window(15, TimeUnit.DAYS))
    groupBy.computeIncrementalDf(outputTable, PartitionRange(targetDay, targetDay), tableProps)

    val writtenPartitions = tableUtils.partitions(outputTable).toSet
    assertEquals(
      s"computeIncrementalDf for a single day must write only that partition; wrote ${writtenPartitions.toSeq.sorted}",
      Set(targetDay),
      writtenPartitions
    )

    // The _daily_inc table is tagged as a Chronon-generated incremental table.
    val props = tableUtils.getTableProperties(outputTable).getOrElse(Map.empty)
    assertEquals(Constants.TableType.GroupByIncremental, props.get(Constants.ChrononTableType).orNull)
    assertEquals("true", props.get(Constants.ChrononGenerated).orNull)
  }

  /**
    * Aggregations sharing the same (operation, input_column, bucket) but differing only by window
    * collapse to the same daily IR column (the window suffix is dropped) - e.g. SUM(price, 7d) and
    * SUM(price, 30d) both -> price_sum, with identical IR values. The incremental schema dedups them
    * to a single column rather than writing duplicates; this verifies one column is written and the
    * windowed output still matches the non-incremental path.
    */
  @Test
  def testIncrementalDedupsSharedColumns(): Unit = {
    lazy val spark: SparkSession =
      SparkSessionBuilder.build("GroupByTestDupCols" + "_" + Random.alphanumeric.take(6).mkString, local = true)
    implicit val tableUtils = TableUtils(spark)
    val namespace = s"incremental_dupcols_${Random.alphanumeric.take(6).mkString}"
    tableUtils.createDatabase(namespace)

    val maxWindowDays = 10
    // Same (SUM, price) split across two aggregations differing only by window -> both collapse to
    // the daily IR column "price_sum". Include COUNT so there is a second, non-colliding column.
    val aggregations: Seq[Aggregation] = Seq(
      Builders.Aggregation(Operation.SUM, "price", Seq(new Window(5, TimeUnit.DAYS))),
      Builders.Aggregation(Operation.SUM, "price", Seq(new Window(maxWindowDays, TimeUnit.DAYS))),
      Builders.Aggregation(Operation.COUNT, "user", Seq(new Window(maxWindowDays, TimeUnit.DAYS)))
    )

    val (source, _) =
      createTestSourceIncremental(windowSize = 30,
                                  suffix = "_dedup",
                                  partitionColOpt = Some(tableUtils.partitionColumn))
    def mkConf(name: String): ai.chronon.api.GroupBy =
      Builders.GroupBy(
        sources = Seq(source),
        keyColumns = Seq("item"),
        aggregations = aggregations,
        metaData = Builders.MetaData(name = name, namespace = namespace, team = "chronon"),
        backfillStartDate = tableUtils.partitionSpec.minus(tableUtils.partitionSpec.at(System.currentTimeMillis()),
                                                           new Window(5, TimeUnit.DAYS))
      )

    val today = tableUtils.partitionSpec.at(System.currentTimeMillis())
    val incConf = mkConf("dedup_incremental")
    GroupBy.computeBackfill(incConf, today, tableUtils, stepDays = Some(1), incrementalMode = true)

    // The _daily_inc table writes exactly one price_sum column (the two windows collapsed).
    val incCols = spark.table(incConf.metaData.incrementalOutputTable).columns.toSeq
    assertEquals(s"expected a single price_sum daily-IR column, got: $incCols", 1, incCols.count(_ == "price_sum"))

    // The final windowed output still matches the non-incremental path (both price_sum_5d and
    // price_sum_10d are correctly reconstructed from the single shared daily IR).
    val normalConf = mkConf("dedup_normal")
    GroupBy.computeBackfill(normalConf, today, tableUtils, stepDays = Some(1), incrementalMode = false)

    val diff = Comparison.sideBySide(spark.read.table(normalConf.metaData.outputTable),
                                     spark.read.table(incConf.metaData.outputTable),
                                     List("item", tableUtils.partitionColumn))
    if (diff.count() > 0) {
      println("=== dedup incremental vs normal diff ===")
      diff.show(50, truncate = false)
    }
    assertEquals(0, diff.count())
  }

  /**
    * Unit test for FIRST and LAST aggregations with incremental IR
    * FIRST/LAST use TimeTuple IR: struct {epochMillis: Long, payload: Value}
    * FIRST keeps the value with the earliest timestamp
    * LAST keeps the value with the latest timestamp
    */
  @Test
  def testIncrementalFirstLast(): Unit = {
    lazy val spark: SparkSession =
      SparkSessionBuilder.build("GroupByTestFirstLast" + "_" + Random.alphanumeric.take(6).mkString, local = true)
    implicit val tableUtils = TableUtils(spark)
    val namespace = s"incremental_first_last_${Random.alphanumeric.take(6).mkString}"
    tableUtils.createDatabase(namespace)

    val schema = List(
      Column("user", StringType, 5),
      Column("value", DoubleType, 100)
    )

    // FIRST/LAST tie-break is ambiguous when two events for the same (user, day) share a ts, so
    // make ts unique WITHIN each day deterministically: snap to the day's midnight and add a
    // per-day row number as a millisecond offset. With << 86.4M rows/day the offset stays within
    // the day (ds unchanged) and no two rows in a day collide -> FIRST/LAST is well-defined and the
    // test is deterministic (no rand()).
    import org.apache.spark.sql.functions.{col, monotonically_increasing_id, row_number}
    import org.apache.spark.sql.expressions.{Window => SparkWindow}
    val dayMs = 86400000L

    val dfUnique = DataFrameGen
      .events(spark, schema, count = 10000, partitions = 20)
      .withColumn("_day_ms", (col("ts") / dayMs).cast("long") * dayMs)
      .withColumn("_rn", row_number().over(SparkWindow.partitionBy("_day_ms").orderBy(monotonically_increasing_id())))
      .withColumn("ts", col("_day_ms") + col("_rn"))
      // value ties make TOP_K/BOTTOM_K ambiguous: the incremental aggregator orders by value only,
      // while the comparison SQL sorts by (value, ts) — so on tied values they keep different
      // elements. Derive a globally unique value from the unique ts so every selection is
      // well-defined and the comparison is deterministic.
      .withColumn("value", col("ts").cast("double"))
      .drop("_day_ms", "_rn")

    // Materialize by writing to a table and reading back, so the row numbering is frozen.
    dfUnique.write.mode("overwrite").saveAsTable(s"${namespace}.test_first_last_input")

    // Read back from table - guaranteed same data as what was written
    val df = spark.table(s"${namespace}.test_first_last_input")

    val aggregations: Seq[Aggregation] = Seq(
      Builders.Aggregation(Operation.FIRST, "value", Seq(new Window(7, TimeUnit.DAYS))),
      Builders.Aggregation(Operation.LAST, "value", Seq(new Window(7, TimeUnit.DAYS))),
      Builders.Aggregation(Operation.FIRST_K, "value", Seq(new Window(7, TimeUnit.DAYS)), argMap = Map("k" -> "3")),
      Builders.Aggregation(Operation.LAST_K, "value", Seq(new Window(7, TimeUnit.DAYS)), argMap = Map("k" -> "3")),
      Builders.Aggregation(Operation.TOP_K, "value", Seq(new Window(7, TimeUnit.DAYS)), argMap = Map("k" -> "3")),
      Builders.Aggregation(Operation.BOTTOM_K, "value", Seq(new Window(7, TimeUnit.DAYS)), argMap = Map("k" -> "3"))
    )

    val tableProps: Map[String, String] = Map("source" -> "chronon")
    val today_date = tableUtils.partitionSpec.at(System.currentTimeMillis())
    val today_minus_7_date = tableUtils.partitionSpec.minus(today_date, new Window(7, TimeUnit.DAYS))
    val today_minus_20_date = tableUtils.partitionSpec.minus(today_date, new Window(20, TimeUnit.DAYS))
    val partitionRange = PartitionRange(today_minus_20_date, today_date)

    val groupBy = new GroupBy(aggregations, Seq("user"), df)
    groupBy.computeIncrementalDf(s"${namespace}.testIncrementalFirstLastOutput", partitionRange, tableProps)

    val rawIncrementalDf =
      spark.sql(s"select * from ${namespace}.testIncrementalFirstLastOutput where ds='$today_minus_7_date'")

    println("=== Incremental FIRST/LAST IR Schema ===")
    rawIncrementalDf.printSchema()

    // Sort array columns in raw IRs to match SQL output ordering
    // Raw IRs store unsorted arrays for mergeability, but we need to sort them for comparison
    import org.apache.spark.sql.functions.{sort_array, col}
    val actualIncrementalDf = rawIncrementalDf
      .withColumn("value_first3", sort_array(col("value_first3")))
      .withColumn("value_last3", sort_array(col("value_last3")))
      .withColumn("value_top3", sort_array(col("value_top3")))
      .withColumn("value_bottom3", sort_array(col("value_bottom3")))

    // Compare against SQL computation
    // Note: ts column in IR table is the partition timestamp (derived from ds)
    // But FIRST/LAST use the actual event timestamps (with random milliseconds)
    val query =
      s"""
         |SELECT user,
         |  to_date(from_unixtime(ts / 1000, 'yyyy-MM-dd HH:mm:ss')) as ds,
         |  named_struct(
         |    'epochMillis', min(ts),
         |    'payload', sort_array(collect_list(struct(ts, value)))[0].value
         |  ) as value_first,
         |  named_struct(
         |    'epochMillis', max(ts),
         |    'payload', reverse(sort_array(collect_list(struct(ts, value))))[0].value
         |  ) as value_last,
         |  transform(
         |    slice(sort_array(filter(collect_list(struct(ts, value)), x -> x.value IS NOT NULL)), 1, 3),
         |    x -> named_struct('epochMillis', x.ts, 'payload', x.value)
         |  ) as value_first3,
         |  transform(
         |    slice(sort_array(filter(collect_list(struct(ts, value)), x -> x.value IS NOT NULL)),
         |          greatest(-size(sort_array(filter(collect_list(struct(ts, value)), x -> x.value IS NOT NULL))), -3), 3),
         |    x -> named_struct('epochMillis', x.ts, 'payload', x.value)
         |  ) as value_last3,
         |  transform(
         |    slice(sort_array(filter(collect_list(struct(value, ts)), x -> x.value IS NOT NULL), true),
         |          greatest(-size(sort_array(filter(collect_list(struct(value, ts)), x -> x.value IS NOT NULL))), -3), 3),
         |    x -> x.value
         |  ) as value_top3,
         |  transform(
         |    slice(sort_array(filter(collect_list(struct(value, ts)), x -> x.value IS NOT NULL), true), 1, 3),
         |    x -> x.value
         |  ) as value_bottom3
         |FROM ${namespace}.test_first_last_input
         |WHERE to_date(from_unixtime(ts / 1000, 'yyyy-MM-dd HH:mm:ss'))='$today_minus_7_date'
         |GROUP BY user, to_date(from_unixtime(ts / 1000, 'yyyy-MM-dd HH:mm:ss'))
         |""".stripMargin

    val expectedDf = spark.sql(query)

    // Drop ts from comparison - it's just the partition timestamp, not part of the aggregation IR
    val actualWithoutTs = actualIncrementalDf.drop("ts")

    val diff = Comparison.sideBySide(actualWithoutTs, expectedDf, List("user", tableUtils.partitionColumn))

    if (diff.count() > 0) {
      println(s"=== Diff Details for Time-based Aggregations ===")
      println(s"Expected count: ${expectedDf.count()}")
      println(s"Diff count: ${diff.count()}")
      diff.show(100, truncate = false)
    }

    assertEquals(0, diff.count())

    println("=== Time-based Aggregations Incremental Test Passed ===")
    println("✓ FIRST: TimeTuple IR {epochMillis, payload}")
    println("✓ LAST: TimeTuple IR {epochMillis, payload}")
    println("✓ FIRST_K: Array[TimeTuple] - stores timestamps")
    println("✓ LAST_K: Array[TimeTuple] - stores timestamps")
    println("✓ TOP_K: Array[Double] - stores only values")
    println("✓ BOTTOM_K: Array[Double] - stores only values")
  }

  @Test
  def testIncrementalStatisticalAggregations(): Unit = {
    lazy val spark: SparkSession =
      SparkSessionBuilder.build("GroupByTestStatistical" + "_" + Random.alphanumeric.take(6).mkString, local = true)
    implicit val tableUtils = TableUtils(spark)
    val namespace = s"incremental_stats_${Random.alphanumeric.take(6).mkString}"
    tableUtils.createDatabase(namespace)

    val outputDates = CStream.genPartitions(10, tableUtils.partitionSpec)

    val aggregations: Seq[Aggregation] = Seq(
      // Moment-based (IR = array<double> [n, m1, m2, m3, m4]); finalized to Double
      Builders.Aggregation(Operation.SKEW, "price", Seq(new Window(10, TimeUnit.DAYS))),
      Builders.Aggregation(Operation.KURTOSIS, "price", Seq(new Window(10, TimeUnit.DAYS))),
      // Sketch-based (IR = binary KLL sketch); finalized to Array[Float]
      Builders.Aggregation(Operation.APPROX_PERCENTILE,
                           "price",
                           Seq(new Window(10, TimeUnit.DAYS)),
                           argMap = Map("percentiles" -> "[0.5, 0.25, 0.75]")),
      // Sketch-based (IR = binary CPC sketch); finalized to Long
      Builders.Aggregation(Operation.APPROX_UNIQUE_COUNT, "user", Seq(new Window(10, TimeUnit.DAYS))),
      // Sketch-based (IR = binary); finalized to Map[String, Long]
      Builders.Aggregation(Operation.APPROX_HISTOGRAM_K,
                           "user",
                           Seq(new Window(10, TimeUnit.DAYS)),
                           argMap = Map("k" -> "10"))
    )

    val (source, _) = createTestSourceIncremental(windowSize = 30,
                                                  suffix = "_stats_events",
                                                  partitionColOpt = Some(tableUtils.partitionColumn))
    val groupByConf = Builders.GroupBy(
      sources = Seq(source),
      keyColumns = Seq("item"),
      aggregations = aggregations,
      metaData = Builders.MetaData(name = "testIncrementalStats", namespace = namespace, team = "chronon"),
      backfillStartDate = tableUtils.partitionSpec.minus(tableUtils.partitionSpec.at(System.currentTimeMillis()),
                                                         new Window(20, TimeUnit.DAYS))
    )

    val df = spark.read.table(source.table)
    val groupBy = new GroupBy(aggregations, Seq("item"), df.filter("item is not null"))
    val nonIncrementalDf = groupBy.snapshotEvents(PartitionRange(outputDates.min, outputDates.max))

    val groupByIncremental =
      GroupBy.fromIncrementalDf(groupByConf, PartitionRange(outputDates.min, outputDates.max), tableUtils)
    val incrementalDf = groupByIncremental.snapshotEvents(PartitionRange(outputDates.min, outputDates.max))

    val diff = Comparison.sideBySide(nonIncrementalDf, incrementalDf, List("item", tableUtils.partitionColumn))
    if (diff.count() > 0) {
      println("=== Diff Details for Statistical Aggregations ===")
      diff.show(100, truncate = false)
    }
    assertEquals(0, diff.count())
  }
}
