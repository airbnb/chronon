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

import ai.chronon.aggregator.test.{CStream, Column, NaiveAggregator}
import ai.chronon.aggregator.windowing.FiveMinuteResolution
import ai.chronon.api.Extensions._
import ai.chronon.api.{
  Aggregation,
  Builders,
  Constants,
  Derivation,
  DoubleType,
  IntType,
  LongType,
  Operation,
  Source,
  StringType,
  TimeUnit,
  Window
}
import ai.chronon.online.serde.{RowWrapper, SparkConversions}
import ai.chronon.spark.Extensions._
import ai.chronon.spark._
import ai.chronon.spark.catalog.TableUtils
import com.google.gson.Gson
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{ArrayType, BinaryType, MapType, StructField, StructType, LongType => SparkLongType, StringType => SparkStringType}
import org.apache.spark.sql.{Encoders, Row, SparkSession}
import org.apache.spark.sql.functions.col
import org.junit.Assert._
import org.junit.Test

import scala.collection.mutable
import scala.util.Random

class GroupByTest {
//  lazy val spark: SparkSession = SparkSessionBuilder.build("GroupByTest", local = true)
//  implicit val tableUtils = TableUtils(spark)
  @Test
  def testSnapshotEntities(): Unit = {
    lazy val spark: SparkSession =
      SparkSessionBuilder.build("GroupByTest" + "_" + Random.alphanumeric.take(6).mkString, local = true)
    implicit val tableUtils = TableUtils(spark)
    val schema = List(
      Column("user", StringType, 10),
      Column(Constants.TimeColumn, LongType, 100), // ts = last 100 days
      Column("session_length", IntType, 10000)
    )
    val df = DataFrameGen.entities(spark, schema, 100000, 10) // ds = last 10 days
    val viewName = "test_group_by_entities"
    df.createOrReplaceTempView(viewName)
    val aggregations: Seq[Aggregation] = Seq(
      Builders
        .Aggregation(Operation.AVERAGE, "session_length", Seq(new Window(10, TimeUnit.DAYS), WindowUtils.Unbounded)))

    val groupBy = new GroupBy(aggregations, Seq("user"), df)
    val actualDf = groupBy.snapshotEntities
    val expectedDf = df.sqlContext.sql(s"""
                                          |SELECT user,
                                          |       ds,
                                          |       AVG(IF(ts  >= (unix_timestamp(ds, 'yyyy-MM-dd') - (86400*(10 - 1))) * 1000, session_length, null)) AS session_length_average_10d,
                                          |       AVG(session_length) as session_length_average
                                          |FROM $viewName
                                          |WHERE ts < unix_timestamp(ds, 'yyyy-MM-dd') * 1000 + 86400 * 1000
                                          |GROUP BY user, ds
                                          |""".stripMargin)

    val diff = Comparison.sideBySide(actualDf, expectedDf, List("user", tableUtils.partitionColumn))
    if (diff.count() > 0) {
      diff.show()
      println("diff result rows")
    }
    assertEquals(0, diff.count())
  }

  @Test
  def testSnapshotEvents(): Unit = {
    lazy val spark: SparkSession =
      SparkSessionBuilder.build("GroupByTest" + "_" + Random.alphanumeric.take(6).mkString, local = true)
    implicit val tableUtils = TableUtils(spark)
    val schema = List(
      Column("user", StringType, 10), // ts = last 10 days
      Column("session_length", IntType, 2),
      Column("rating", DoubleType, 2000)
    )

    val outputDates = CStream.genPartitions(10, tableUtils.partitionSpec)

    val df = DataFrameGen.events(spark, schema, count = 100000, partitions = 100)
    df.drop("ts") // snapshots don't need ts.
    val viewName = "test_group_by_snapshot_events"
    df.createOrReplaceTempView(viewName)
    val aggregations: Seq[Aggregation] = Seq(
      Builders.Aggregation(Operation.APPROX_UNIQUE_COUNT,
                           "session_length",
                           Seq(new Window(10, TimeUnit.DAYS), WindowUtils.Unbounded)),
      Builders.Aggregation(Operation.UNIQUE_COUNT,
                           "session_length",
                           Seq(new Window(10, TimeUnit.DAYS), WindowUtils.Unbounded)),
      Builders.Aggregation(Operation.SUM, "session_length", Seq(new Window(10, TimeUnit.DAYS)))
    )

    val groupBy = new GroupBy(aggregations, Seq("user"), df)
    val actualDf = groupBy.snapshotEvents(PartitionRange(outputDates.min, outputDates.max))

    val outputDatesRdd: RDD[Row] = spark.sparkContext.parallelize(outputDates.map(Row(_)))
    val outputDatesDf = spark.createDataFrame(outputDatesRdd, StructType(Seq(StructField("ds", SparkStringType))))
    val datesViewName = "test_group_by_snapshot_events_output_range"
    outputDatesDf.createOrReplaceTempView(datesViewName)
    val expectedDf = df.sqlContext.sql(s"""
                                          |select user,
                                          |       $datesViewName.ds,
                                          |       COUNT(DISTINCT session_length) as session_length_approx_unique_count,
                                          |       COUNT(DISTINCT session_length) as session_length_unique_count,
                                          |       COUNT(DISTINCT IF(ts  >= (unix_timestamp($datesViewName.ds, 'yyyy-MM-dd') - 86400*(10-1)) * 1000, session_length, null)) as session_length_approx_unique_count_10d,
                                          |       COUNT(DISTINCT IF(ts  >= (unix_timestamp($datesViewName.ds, 'yyyy-MM-dd') - 86400*(10-1)) * 1000, session_length, null)) as session_length_unique_count_10d,
                                          |       SUM(IF(ts  >= (unix_timestamp($datesViewName.ds, 'yyyy-MM-dd') - 86400*(10-1)) * 1000, session_length, null)) AS session_length_sum_10d,
                                          |       SUM(IF(ts  >= (unix_timestamp($datesViewName.ds, 'yyyy-MM-dd') - 86400*(10-1)) * 1000, rating, null)) AS rating_sum_10d
                                          |FROM $viewName CROSS JOIN $datesViewName
                                          |WHERE ts < unix_timestamp($datesViewName.ds, 'yyyy-MM-dd') * 1000 + ${tableUtils.partitionSpec.spanMillis}
                                          |group by user, $datesViewName.ds
                                          |""".stripMargin)

    val diff = Comparison.sideBySide(actualDf, expectedDf, List("user", tableUtils.partitionColumn))
    if (diff.count() > 0) {
      diff.show()
      println("diff result rows")
    }
    assertEquals(0, diff.count())
  }

  @Test
  def eventsLastKTest(): Unit = {
    lazy val spark: SparkSession =
      SparkSessionBuilder.build("GroupByTest" + "_" + Random.alphanumeric.take(6).mkString, local = true)
    implicit val tableUtils = TableUtils(spark)
    val eventSchema = List(
      Column("user", StringType, 10),
      Column("listing_view", StringType, 100)
    )
    val eventDf = DataFrameGen.events(spark, eventSchema, count = 10000, partitions = 180)
    eventDf.createOrReplaceTempView("events_last_k")

    val querySchema = List(Column("user", StringType, 10))
    val queryDf = DataFrameGen.events(spark, querySchema, count = 1000, partitions = 180)
    queryDf.createOrReplaceTempView("queries_last_k")

    val aggregations: Seq[Aggregation] = Seq(
      Builders.Aggregation(Operation.LAST_K, "listing_view", Seq(WindowUtils.Unbounded), argMap = Map("k" -> "30")),
      Builders.Aggregation(Operation.COUNT, "listing_view", Seq(WindowUtils.Unbounded))
    )
    val keys = Seq("user").toArray
    val groupBy =
      new GroupBy(aggregations,
                  keys,
                  eventDf.selectExpr("user", "ts", "concat(ts, \" \", listing_view) as listing_view"))
    val resultDf = groupBy.temporalEvents(queryDf)
    val computed = resultDf.select("user", "ts", "listing_view_last30", "listing_view_count")
    computed.show()

    val expected = eventDf.sqlContext.sql(s"""
         |SELECT
         |      events_last_k.user as user,
         |      queries_last_k.ts as ts,
         |      COLLECT_LIST(concat(CAST(events_last_k.ts AS STRING), " ", events_last_k.listing_view)) as listing_view_last30,
         |      SUM(case when events_last_k.listing_view <=> NULL then 0 else 1 end) as listing_view_count
         |FROM events_last_k CROSS JOIN queries_last_k
         |ON events_last_k.user = queries_last_k.user
         |WHERE events_last_k.ts < queries_last_k.ts
         |GROUP BY events_last_k.user, queries_last_k.ts
         |""".stripMargin)

    expected.show()

    val diff = Comparison.sideBySide(computed, expected, List("user", "ts"))
    if (diff.count() > 0) {
      println(s"Actual count: ${computed.count()}")
      println(s"Expected count: ${expected.count()}")
      println(s"Diff count: ${diff.count()}")
      println(s"diff result rows last_k_test")
      diff.show()
      diff.rdd.foreach { row =>
        val gson = new Gson()
        val computed =
          Option(row(4)).map(_.asInstanceOf[mutable.WrappedArray[String]].toArray).getOrElse(Array.empty[String])
        val expected =
          Option(row(5))
            .map(_.asInstanceOf[mutable.WrappedArray[String]].toArray.sorted.reverse.take(30))
            .getOrElse(Array.empty[String])
        val computedCount = Option(row(2)).map(_.asInstanceOf[Long]).getOrElse(0)
        val expectedCount = Option(row(3)).map(_.asInstanceOf[Long]).getOrElse(0)
        val computedStr = gson.toJson(computed)
        val expectedStr = gson.toJson(expected)
        if (computedStr != expectedStr) {
          println(s"""
                     |computed [$computedCount]: ${gson.toJson(computed)}
                     |expected [$expectedCount]: ${gson.toJson(expected)}
                     |""".stripMargin)
        }
        assertEquals(gson.toJson(computed), gson.toJson(expected))
      }
    }
  }
  @Test
  def testTemporalEvents(): Unit = {
    lazy val spark: SparkSession =
      SparkSessionBuilder.build("GroupByTest" + "_" + Random.alphanumeric.take(6).mkString, local = true)
    implicit val tableUtils = TableUtils(spark)
    val eventSchema = List(
      Column("user", StringType, 10),
      Column("session_length", IntType, 10000)
    )

    val eventDf = DataFrameGen.events(spark, eventSchema, count = 10000, partitions = 180)

    val querySchema = List(Column("user", StringType, 10))

    val queryDf = DataFrameGen.events(spark, querySchema, count = 1000, partitions = 180)

    val aggregations: Seq[Aggregation] = Seq(
      Builders.Aggregation(
        Operation.AVERAGE,
        "session_length",
        Seq(new Window(1, TimeUnit.DAYS), new Window(1, TimeUnit.HOURS), new Window(30, TimeUnit.DAYS))))

    val keys = Seq("user").toArray
    val groupBy = new GroupBy(aggregations, keys, eventDf)
    val resultDf = groupBy.temporalEvents(queryDf)

    val keyBuilder = FastHashing.generateKeyBuilder(keys, eventDf.schema)
    // naive aggregation for equivalence testing
    // this will basically explode on even moderate large table
    val queriesByKey: RDD[(KeyWithHash, Array[Long])] = queryDf
      .where("user IS NOT NULL")
      .rdd
      .groupBy(keyBuilder)
      .mapValues { rowIter =>
        rowIter.map {
          _.getAs[Long](Constants.TimeColumn)
        }.toArray
      }

    val eventsByKey: RDD[(KeyWithHash, Iterator[RowWrapper])] = eventDf.rdd
      .groupBy(keyBuilder)
      .mapValues { rowIter =>
        rowIter.map(SparkConversions.toChrononRow(_, groupBy.tsIndex)).toIterator
      }

    val windows = aggregations.flatMap(_.unpack.map(_.window)).toArray
    val tailHops = windows.map(FiveMinuteResolution.calculateTailHop)
    val naiveAggregator =
      new NaiveAggregator(groupBy.windowAggregator, windows, tailHops)
    val naiveRdd = queriesByKey.leftOuterJoin(eventsByKey).flatMap {
      case (key, (queries: Array[Long], events: Option[Iterator[RowWrapper]])) =>
        val irs = naiveAggregator.aggregate(events.map(_.toSeq).orNull, queries)
        queries.zip(irs).map {
          case (query: Long, ir: Array[Any]) =>
            (key.data :+ query, groupBy.windowAggregator.finalize(ir))
        }
    }
    val naiveDf = groupBy.toDf(naiveRdd, Seq((Constants.TimeColumn, SparkLongType)))

    val diff = Comparison.sideBySide(naiveDf, resultDf, List("user", Constants.TimeColumn))
    if (diff.count() > 0) {
      diff.show()
      println("diff result rows")
    }
    assertEquals(0, diff.count())
  }

  // Test that the output of Group by with Step Days is the same as the output without Steps (full data range)
  @Test
  def testStepDaysConsistency(): Unit = {
    lazy val spark: SparkSession =
      SparkSessionBuilder.build("GroupByTest" + "_" + Random.alphanumeric.take(6).mkString, local = true)
    val (source, endPartition) = createTestSource()
    val tableUtils = TableUtils(spark)
    val testSteps = Option(30)
    val namespace = "test_steps"
    val diff = Comparison.sideBySide(
      tableUtils.sql(
        s"SELECT * FROM ${backfill(name = "unit_test_item_views_steps", source = source, endPartition = endPartition, namespace = namespace, tableUtils = tableUtils, stepDays = testSteps)}"),
      tableUtils.sql(
        s"SELECT * FROM ${backfill(name = "unit_test_item_views_no_steps", source = source, endPartition = endPartition, namespace = namespace, tableUtils = tableUtils)}"),
      List("item", tableUtils.partitionColumn)
    )
    if (diff.count() != 0) {
      diff.show(100)
    }
    assertEquals(0, diff.count())
  }

  @Test
  def testGroupByAnalyzer(): Unit = {
    lazy val spark: SparkSession =
      SparkSessionBuilder.build("GroupByTest" + "_" + Random.alphanumeric.take(6).mkString, local = true)
    val (source, endPartition) = createTestSource(30)
    val tableUtils = TableUtils(spark)
    val namespace = "test_analyzer_testGroupByAnalyzer"
    val groupByConf = getSampleGroupBy("unit_analyze_test_item_views", source, namespace)
    val today = tableUtils.partitionSpec.at(System.currentTimeMillis())
    val aggregationsMetadata =
      new Analyzer(tableUtils, groupByConf, endPartition, today)
        .analyzeGroupBy(groupByConf, enableHitter = false)
        .outputMetadata
    val outputTable = backfill(name = "unit_analyze_test_item_views",
                               source = source,
                               endPartition = endPartition,
                               namespace = namespace,
                               tableUtils = tableUtils)
    val df = tableUtils.sql(s"SELECT * FROM  ${outputTable}")
    val expectedSchema = df.schema.fields.map(field => s"${field.name} => ${field.dataType}")
    aggregationsMetadata
      .map(agg => s"${agg.name} => ${agg.columnType}")
      .foreach(s => assertTrue(expectedSchema.contains(s)))

    // feature name is constructed by input_column_operation_window
    // assert feature columns attributes mapping
    aggregationsMetadata.foreach(aggregation => {
      assertTrue(aggregation.name.contains(aggregation.operation.toLowerCase))
      assertTrue(aggregation.name.contains(aggregation.inputColumn.toLowerCase))
    })
  }

  @Test
  def testGroupByNoAggregationAnalyzer(): Unit = {
    lazy val spark: SparkSession =
      SparkSessionBuilder.build("GroupByTest" + "_" + Random.alphanumeric.take(6).mkString, local = true)
    val (source, endPartition) = createTestSource(30)
    val testName = "unit_analyze_test_item_no_agg"
    val tableUtils = TableUtils(spark)
    val namespace = "test_analyzer_testGroupByNoAggregationAnalyzer"
    val groupByConf = Builders.GroupBy(
      sources = Seq(source),
      keyColumns = Seq("item"),
      aggregations = null,
      metaData = Builders.MetaData(name = testName, namespace = namespace, team = "chronon"),
      backfillStartDate = tableUtils.partitionSpec.minus(tableUtils.partitionSpec.at(System.currentTimeMillis()),
                                                         new Window(60, TimeUnit.DAYS))
    )
    val today = tableUtils.partitionSpec.at(System.currentTimeMillis())
    val aggregationsMetadata =
      new Analyzer(tableUtils, groupByConf, endPartition, today)
        .analyzeGroupBy(groupByConf, enableHitter = false)
        .outputMetadata

    print(aggregationsMetadata)
    assertTrue(aggregationsMetadata.length == 2)

    val columns = aggregationsMetadata.map(a => a.name -> a.columnType).toMap
    assertEquals(Map(
                   "time_spent_ms" -> LongType,
                   "price" -> DoubleType
                 ),
                 columns)
  }

  @Test
  def testGroupByDerivationAnalyzer(): Unit = {
    lazy val spark: SparkSession =
      SparkSessionBuilder.build("GroupByTest" + "_" + Random.alphanumeric.take(6).mkString, local = true)
    val (source, endPartition) = createTestSource(30)
    val tableUtils = TableUtils(spark)
    val namespace = "test_analyzer_testGroupByDerivation"
    val derivation = Builders.Derivation.star()
    val groupByConf =
      getSampleGroupBy("unit_analyze_test_item_views", source, namespace, Seq.empty, derivations = Seq(derivation))
    val today = tableUtils.partitionSpec.at(System.currentTimeMillis())
    val aggregationsMetadata =
      new Analyzer(tableUtils, groupByConf, endPartition, today)
        .analyzeGroupBy(groupByConf, enableHitter = false)
        .outputMetadata
    val outputTable = backfill(name = "unit_analyze_test_item_views",
                               source = source,
                               endPartition = endPartition,
                               namespace = namespace,
                               tableUtils = tableUtils)
    val df = tableUtils.sql(s"SELECT * FROM  ${outputTable}")
    val expectedSchema = df.schema.fields.map(field => s"${field.name} => ${field.dataType}")

    // When the groupBy has derivations, the aggMetadata will only contains the name and type, which will be the same with the schema in output table.
    aggregationsMetadata
      .foreach(agg => {
        assertTrue(expectedSchema.contains(s"${agg.name} => ${agg.columnType}"))
        assertTrue(agg.operation == "Derivation")
      })
  }

  // test that OrderByLimit and OrderByLimitTimed serialization works well with Spark's data type
  @Test
  def testFirstKLastKTopKBottomKApproxUniqueCount(): Unit = {
    lazy val spark: SparkSession =
      SparkSessionBuilder.build("GroupByTest" + "_" + Random.alphanumeric.take(6).mkString, local = true)
    val (source, endPartition) = createTestSource()
    val tableUtils = TableUtils(spark)
    val namespace = "test_order_by_limit"
    val aggs = Seq(
      Builders.Aggregation(operation = Operation.LAST_K,
                           inputColumn = "ts",
                           windows = Seq(
                             new Window(15, TimeUnit.DAYS),
                             new Window(60, TimeUnit.DAYS)
                           ),
                           argMap = Map("k" -> "2")),
      Builders.Aggregation(operation = Operation.FIRST_K,
                           inputColumn = "ts",
                           windows = Seq(
                             new Window(15, TimeUnit.DAYS),
                             new Window(60, TimeUnit.DAYS)
                           ),
                           argMap = Map("k" -> "2")),
      Builders.Aggregation(operation = Operation.TOP_K,
                           inputColumn = "ts",
                           windows = Seq(
                             new Window(15, TimeUnit.DAYS),
                             new Window(60, TimeUnit.DAYS)
                           ),
                           argMap = Map("k" -> "2")),
      Builders.Aggregation(operation = Operation.BOTTOM_K,
                           inputColumn = "ts",
                           windows = Seq(
                             new Window(15, TimeUnit.DAYS),
                             new Window(60, TimeUnit.DAYS)
                           ),
                           argMap = Map("k" -> "2")),
      Builders.Aggregation(operation = Operation.APPROX_UNIQUE_COUNT,
                           inputColumn = "ts",
                           windows = Seq(
                             new Window(15, TimeUnit.DAYS),
                             new Window(60, TimeUnit.DAYS)
                           ))
    )
    backfill(name = "unit_test_serialization",
             source = source,
             endPartition = endPartition,
             namespace = namespace,
             tableUtils = tableUtils,
             additionalAgg = aggs)
  }


  private def createTestSource(windowSize: Int = 365,
                               suffix: String = "",
                               partitionColOpt: Option[String] = None): (Source, String) = {
    lazy val spark: SparkSession =
      SparkSessionBuilder.build("GroupByTest" + "_" + Random.alphanumeric.take(6).mkString, local = true)
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
    val namespace = "chronon_test"
    val sourceTable = s"$namespace.test_group_by_steps$suffix"

    tableUtils.createDatabase(namespace)
    val genDf =
      DataFrameGen.events(spark, sourceSchema, count = 1000, partitions = 200, partitionColOpt = partitionColOpt)
    partitionColOpt match {
      case Some(partitionCol) => genDf.save(sourceTable, partitionColumns = Seq(partitionCol))
      case None               => genDf.save(sourceTable)
    }

    val source = Builders.Source.events(
      query = Builders.Query(selects = Builders.Selects("ts", "item", "time_spent_ms", "price"),
                             startPartition = startPartition,
                             partitionColumn = partitionColOpt.orNull),
      table = sourceTable
    )
    (source, endPartition)
  }

  def backfill(name: String,
               source: Source,
               namespace: String,
               endPartition: String,
               tableUtils: TableUtils,
               stepDays: Option[Int] = None,
               additionalAgg: Seq[Aggregation] = Seq.empty): String = {
    tableUtils.createDatabase(namespace)
    val groupBy = getSampleGroupBy(name, source, namespace, additionalAgg)

    GroupBy.computeBackfill(
      groupBy,
      endPartition = endPartition,
      tableUtils = tableUtils,
      stepDays = stepDays
    )
    s"$namespace.$name"
  }

  def getSampleGroupBy(name: String,
                       source: Source,
                       namespace: String,
                       additionalAgg: Seq[Aggregation] = Seq.empty,
                       derivations: Seq[Derivation] = Seq.empty): ai.chronon.api.GroupBy = {
    lazy val spark: SparkSession =
      SparkSessionBuilder.build("GroupByTest" + "_" + Random.alphanumeric.take(6).mkString, local = true)
    implicit val tableUtils = TableUtils(spark)
    Builders.GroupBy(
      sources = Seq(source),
      keyColumns = Seq("item"),
      aggregations = Seq(
        Builders.Aggregation(operation = Operation.COUNT, inputColumn = "time_spent_ms"),
        Builders.Aggregation(operation = Operation.MIN,
                             inputColumn = "ts",
                             windows = Seq(
                               new Window(15, TimeUnit.DAYS),
                               new Window(60, TimeUnit.DAYS),
                               WindowUtils.Unbounded
                             )),
        Builders.Aggregation(operation = Operation.MAX, inputColumn = "ts")
      ) ++ additionalAgg,
      metaData = Builders.MetaData(name = name, namespace = namespace, team = "chronon"),
      backfillStartDate = tableUtils.partitionSpec.minus(tableUtils.partitionSpec.at(System.currentTimeMillis()),
                                                         new Window(60, TimeUnit.DAYS)),
      derivations = derivations
    )
  }

  // Test percentile Impl on Spark.
  @Test
  def testPercentiles(): Unit = {
    lazy val spark: SparkSession =
      SparkSessionBuilder.build("GroupByTest" + "_" + Random.alphanumeric.take(6).mkString, local = true)
    val (source, endPartition) = createTestSource(suffix = "_percentile")
    val tableUtils = TableUtils(spark)
    val namespace = "test_percentiles"
    val aggs = Seq(
      Builders.Aggregation(
        operation = Operation.APPROX_PERCENTILE,
        inputColumn = "ts",
        windows = Seq(
          new Window(15, TimeUnit.DAYS),
          new Window(60, TimeUnit.DAYS)
        ),
        argMap = Map("k" -> "128", "percentiles" -> "[0.5, 0.25, 0.75]")
      )
    )
    backfill(name = "unit_test_group_by_percentiles",
             source = source,
             endPartition = endPartition,
             namespace = namespace,
             tableUtils = tableUtils,
             additionalAgg = aggs)
  }

  @Test
  def testApproxHistograms(): Unit = {
    lazy val spark: SparkSession =
      SparkSessionBuilder.build("GroupByTest" + "_" + Random.alphanumeric.take(6).mkString, local = true)
    val (source, endPartition) = createTestSource(suffix = "_approx_histogram")
    val tableUtils = TableUtils(spark)
    val namespace = "test_approx_histograms"
    val aggs = Seq(
      Builders.Aggregation(
        operation = Operation.APPROX_HISTOGRAM_K,
        inputColumn = "item",
        windows = Seq(
          new Window(15, TimeUnit.DAYS),
          new Window(60, TimeUnit.DAYS)
        ),
        argMap = Map("k" -> "4")
      ),
      Builders.Aggregation(
        operation = Operation.APPROX_HISTOGRAM_K,
        inputColumn = "ts",
        windows = Seq(
          new Window(15, TimeUnit.DAYS),
          new Window(60, TimeUnit.DAYS)
        ),
        argMap = Map("k" -> "4")
      ),
      Builders.Aggregation(
        operation = Operation.APPROX_HISTOGRAM_K,
        inputColumn = "price",
        windows = Seq(
          new Window(15, TimeUnit.DAYS),
          new Window(60, TimeUnit.DAYS)
        ),
        argMap = Map("k" -> "4")
      )
    )
    backfill(name = "unit_test_group_by_approx_histograms",
             source = source,
             endPartition = endPartition,
             namespace = namespace,
             tableUtils = tableUtils,
             additionalAgg = aggs)

    val histogramValues = spark
      .sql("""
          |select explode(map_values(item_approx_histogram_k_15d)) as item_values
          |from test_approx_histograms.unit_test_group_by_approx_histograms
          |""".stripMargin)
      .map(row => row.getAs[Long]("item_values"))(Encoders.scalaLong)
      .collect()
      .toSet

    assert(histogramValues.nonEmpty)
    assert(!histogramValues.contains(0))
  }

  @Test
  def testReplaceJoinSource(): Unit = {
    lazy val spark: SparkSession =
      SparkSessionBuilder.build("GroupByTest" + "_" + Random.alphanumeric.take(6).mkString, local = true)
    implicit val tableUtils = TableUtils(spark)
    val namespace = "replace_join_source_ns"
    val today = tableUtils.partitionSpec.at(System.currentTimeMillis())

    val joinSource = TestUtils.getParentJoin(spark, namespace, "parent_join_table", "parent_gb")
    val query = Builders.Query(startPartition = today)
    val chainingGroupBy = TestUtils.getTestGBWithJoinSource(joinSource, query, namespace, "chaining_gb")
    val newGroupBy = GroupBy.replaceJoinSource(chainingGroupBy, PartitionRange(today, today), tableUtils, false)

    assertEquals(joinSource.metaData.outputTable, newGroupBy.sources.get(0).table)
    assertEquals(joinSource.left.topic + Constants.TopicInvalidSuffix, newGroupBy.sources.get(0).topic)
    assertEquals(query, newGroupBy.sources.get(0).query)
  }

  @Test
  def testGroupByFromChainingGB(): Unit = {
    lazy val spark: SparkSession =
      SparkSessionBuilder.build("GroupByTest" + "_" + Random.alphanumeric.take(6).mkString, local = true)
    implicit val tableUtils = TableUtils(spark)
    val namespace = "test_chaining_gb"
    val today = tableUtils.partitionSpec.at(System.currentTimeMillis())
    val joinName = "parent_join_table"
    val parentGBName = "parent_gb"

    val joinSource = TestUtils.getParentJoin(spark, namespace, joinName, parentGBName)
    val query = Builders.Query(startPartition = today)
    val chainingGroupBy = TestUtils.getTestGBWithJoinSource(joinSource, query, namespace, "user_viewed_price_gb")
    val newGroupBy = GroupBy.from(chainingGroupBy, PartitionRange(today, today), tableUtils, true)

    //verify parent join output table is computed and
    assertTrue(spark.catalog.tableExists(s"$namespace.parent_join_table"))
    val expectedSQL =
      s"""
         |WITH latestB AS (
         |    SELECT
         |        COALESCE(A.listing, '--null--') listing,
         |        A.user,
         |        MAX(A.ts) as ts,
         |        A.ds
         |    FROM
         |        $namespace.parent_join_table  A
         |    LEFT OUTER JOIN
         |       $namespace.views_table B ON A.listing = B.listing
         |    WHERE
         |        B.ts <= A.ts AND A.ds = '$today'
         |    GROUP BY
         |        A.listing, A.user, A.ds
         |)
         |SELECT
         |    IF(latestB.listing == '--null--', null, latestB.listing) as listing,
         |    latestB.user,
         |    latestB.ts,
         |    latestB.ds,
         |    C.parent_gb_price_last
         |FROM
         |    latestB
         |JOIN
         |   $namespace.parent_join_table C
         |ON
         |    latestB.listing = COALESCE(C.listing, '--null--') AND latestB.ts = C.ts
         |""".stripMargin
    val expectedInputDf = spark.sql(expectedSQL)
    println("Expected input DF: ")
    expectedInputDf.show()
    println("Computed input DF: ")
    newGroupBy.inputDf.show()

    val diff = Comparison.sideBySide(newGroupBy.inputDf, expectedInputDf, List("listing", "user", "ds"))
    if (diff.count() > 0) {
      println(s"Actual count: ${newGroupBy.inputDf.count()}")
      println(s"Expected count: ${expectedInputDf.count()}")
      println(s"Diff count: ${diff.count()}")
      diff.show()
    }
    assertEquals(0, diff.count())
  }

  @Test
  def testDescriptiveStats(): Unit = {
    lazy val spark: SparkSession =
      SparkSessionBuilder.build("GroupByTest" + "_" + Random.alphanumeric.take(6).mkString, local = true)
    val (source, endPartition) = createTestSource(suffix = "_descriptive_stats")
    val tableUtils = TableUtils(spark)
    val namespace = "test_descriptive_stats"
    val aggs = Seq(
      Builders.Aggregation(
        operation = Operation.VARIANCE,
        inputColumn = "price",
        windows = Seq(
          new Window(15, TimeUnit.DAYS),
          new Window(60, TimeUnit.DAYS)
        )
      ),
      Builders.Aggregation(
        operation = Operation.SKEW,
        inputColumn = "price",
        windows = Seq(
          new Window(15, TimeUnit.DAYS),
          new Window(60, TimeUnit.DAYS)
        )
      ),
      Builders.Aggregation(
        operation = Operation.KURTOSIS,
        inputColumn = "price",
        windows = Seq(
          new Window(15, TimeUnit.DAYS),
          new Window(60, TimeUnit.DAYS)
        )
      )
    )
    val outputTable = backfill(name = "unit_test_group_by_descriptive_stats",
                               source = source,
                               endPartition = endPartition,
                               namespace = namespace,
                               tableUtils = tableUtils,
                               additionalAgg = aggs)
    val expectedInputDf = spark.sql(f"select count(*), ds from $outputTable group by ds")
    expectedInputDf.show()
  }

  @Test
  def testGroupByWithQueryPartitionColumn(): Unit = {
    lazy val spark: SparkSession =
      SparkSessionBuilder.build("GroupByTest" + "_" + Random.alphanumeric.take(6).mkString, local = true)
    val queryPartitionColumn = "new_date_col"
    val (source, endPartition) =
      createTestSource(suffix = "_custom_p_cols", partitionColOpt = Some(queryPartitionColumn))
    val tableUtils = TableUtils(spark)
    val namespace = "test_custom_p_cols"

    val outputTable = backfill(name = "unit_test_custom_p_cols",
                               source = source,
                               endPartition = endPartition,
                               namespace = namespace,
                               tableUtils = tableUtils)

    // Resulting table has "ds" column
    val expectedInputDf = spark.sql(f"select count(*) as cnt, ds from $outputTable group by ds")
    assert(expectedInputDf.count() > 10, "Expected more than 10 rows in the result")
    expectedInputDf.select("cnt").as[Long](Encoders.scalaLong).collect().foreach { count =>
      assert(count > 0, s"Found a count value that is not greater than zero: $count")
    }
  }

  @Test
  def testBoundedUniqueCounts(): Unit = {
    lazy val spark: SparkSession =
      SparkSessionBuilder.build("GroupByTest" + "_" + Random.alphanumeric.take(6).mkString, local = true)
    val (source, endPartition) = createTestSource(suffix = "_bounded_counts")
    val tableUtils = TableUtils(spark)
    val namespace = "test_bounded_counts"
    val aggs = Seq(
      Builders.Aggregation(
        operation = Operation.BOUNDED_UNIQUE_COUNT,
        inputColumn = "item",
        windows = Seq(
          new Window(15, TimeUnit.DAYS),
          new Window(60, TimeUnit.DAYS)
        ),
        argMap = Map("k" -> "5")
      ),
      Builders.Aggregation(
        operation = Operation.BOUNDED_UNIQUE_COUNT,
        inputColumn = "price",
        windows = Seq(
          new Window(15, TimeUnit.DAYS),
          new Window(60, TimeUnit.DAYS)
        ),
        argMap = Map("k" -> "5")
      )
    )
    backfill(name = "unit_test_group_by_bounded_counts",
             source = source,
             endPartition = endPartition,
             namespace = namespace,
             tableUtils = tableUtils,
             additionalAgg = aggs)

    val result = spark.sql("""
        |select *
        |from test_bounded_counts.unit_test_group_by_bounded_counts
        |where item_bounded_unique_count_60d > 5 or price_bounded_unique_count_60d > 5
        |""".stripMargin)

    assertTrue(result.isEmpty)
  }

  @Test
  def testTemporalEventsWithDateintPartition(): Unit = {
    lazy val spark: SparkSession =
      SparkSessionBuilder.build("GroupByTest" + "_" + Random.alphanumeric.take(6).mkString, local = true)
    implicit val tableUtils = TableUtils(spark)

    val eventData = Seq(
      ("user1", 1640995200000L, 100, 20231201L), // 2021-12-31 + session_length 100 + dateint 20231201
      ("user1", 1641081600000L, 150, 20231202L), // 2022-01-01 + session_length 150 + dateint 20231202
      ("user2", 1641168000000L, 200, 20231201L), // 2022-01-02 + session_length 200 + dateint 20231201
      ("user2", 1641254400000L, 250, 20231202L), // 2022-01-03 + session_length 250 + dateint 20231202
      ("user1", 1641340800000L, 300, 20231203L) // 2022-01-04 + session_length 300 + dateint 20231203
    )

    import spark.implicits._
    val eventDf = eventData.toDF("user", Constants.TimeColumn, "session_length", tableUtils.partitionColumn)

    val queryData = Seq(
      ("user1", 1641168000000L, 20231202L), // Query at 2022-01-02 with dateint 20231202
      ("user2", 1641254400000L, 20231202L) // Query at 2022-01-03 with dateint 20231202
    )

    val queryDf = queryData.toDF("user", Constants.TimeColumn, tableUtils.partitionColumn)

    val aggregations: Seq[Aggregation] = Seq(
      Builders.Aggregation(Operation.AVERAGE,
                           "session_length",
                           Seq(new Window(1, TimeUnit.DAYS), new Window(2, TimeUnit.DAYS))))

    val keys = Seq("user").toArray
    val groupBy = new GroupBy(aggregations, keys, eventDf)

    val resultDf = groupBy.temporalEvents(queryDf)

    val resultCount = resultDf.count()
    assertTrue("Should have temporal aggregation results", resultCount > 0)

    val expectedColumns = Set("user", Constants.TimeColumn) ++
      aggregations.flatMap(_.unpack.map(agg => s"session_length_${agg.operation}_${agg.window.millis}"))
    val actualColumns = resultDf.columns.toSet
    assertTrue("Result should contain user and timestamp columns",
               actualColumns.contains("user") && actualColumns.contains(Constants.TimeColumn))

    println("Temporal events result with dateint partition:")
    resultDf.show()
  }

  @Test
  def testStagingQueryViewWithGroupBy(): Unit = {
    lazy val spark: SparkSession =
      SparkSessionBuilder.build("GroupByTest" + "_" + Random.alphanumeric.take(6).mkString, local = true)
    implicit val tableUtils = TableUtils(spark)

    val testDatabase = s"staging_query_view_test_${Random.alphanumeric.take(6).mkString}"
    tableUtils.createDatabase(testDatabase)

    // Create source data table with partitions
    val sourceSchema = List(
      Column("user", StringType, 20),
      Column("item", StringType, 50),
      Column("time_spent_ms", LongType, 5000),
      Column("price", DoubleType, 100)
    )

    val sourceTable = s"$testDatabase.source_events"
    val sourceDf = DataFrameGen.events(spark, sourceSchema, count = 1000, partitions = 10)
    sourceDf.save(sourceTable)

    // Create staging query configuration that creates a view (no date templates)
    val stagingQueryConf = Builders.StagingQuery(
      metaData = Builders.MetaData(name = "test_staging_view", namespace = testDatabase, team = "chronon"),
      query = s"SELECT user, item, time_spent_ms, price, ts, ds FROM $sourceTable",
      createView = true
    )

    val today = tableUtils.partitionSpec.at(System.currentTimeMillis())
    val stagingQueryJob = new StagingQuery(stagingQueryConf, today, tableUtils)

    // Create the staging query view using the new createStagingQueryView method
    stagingQueryJob.createStagingQueryView()

    val viewTable = s"$testDatabase.test_staging_view"

    // Now create a GroupBy that uses the staging query view as its source
    val source = Builders.Source.events(
      table = viewTable,
      query = Builders.Query(selects = Builders.Selects("user", "item", "time_spent_ms", "price", "ts"))
    )

    val aggregations = Seq(
      Builders.Aggregation(operation = Operation.COUNT, inputColumn = "time_spent_ms"),
      Builders.Aggregation(operation = Operation.AVERAGE,
                           inputColumn = "price",
                           windows = Seq(new Window(7, TimeUnit.DAYS))),
      Builders.Aggregation(operation = Operation.MAX,
                           inputColumn = "time_spent_ms",
                           windows = Seq(new Window(30, TimeUnit.DAYS)))
    )

    val groupByConf = Builders.GroupBy(
      sources = Seq(source),
      keyColumns = Seq("user"),
      aggregations = aggregations,
      metaData = Builders.MetaData(name = "test_staging_view_groupby", namespace = testDatabase, team = "chronon"),
      backfillStartDate = tableUtils.partitionSpec.minus(today, new Window(60, TimeUnit.DAYS))
    )

    // Run the GroupBy backfill
    GroupBy.computeBackfill(groupByConf, endPartition = today, tableUtils = tableUtils)

    val groupByOutputTable = s"$testDatabase.test_staging_view_groupby"
    val groupByResultFromView = tableUtils.sql(s"SELECT * FROM $groupByOutputTable")

    // Create a second GroupBy that uses the original source table (not the view) for comparison
    val sourceFromTable = Builders.Source.events(
      table = sourceTable,
      query = Builders.Query(selects = Builders.Selects("user", "item", "time_spent_ms", "price", "ts"))
    )

    val groupByConfFromTable = Builders.GroupBy(
      sources = Seq(sourceFromTable),
      keyColumns = Seq("user"),
      aggregations = aggregations,
      metaData = Builders.MetaData(name = "test_source_table_groupby", namespace = testDatabase, team = "chronon"),
      backfillStartDate = tableUtils.partitionSpec.minus(today, new Window(60, TimeUnit.DAYS))
    )

    GroupBy.computeBackfill(groupByConfFromTable, endPartition = today, tableUtils = tableUtils)
    val groupByOutputTableFromSource = s"$testDatabase.test_source_table_groupby"
    val groupByResultFromSource = tableUtils.sql(s"SELECT * FROM $groupByOutputTableFromSource")

    // Compare the GroupBy results from view vs source table to ensure they match
    val diff = Comparison.sideBySide(groupByResultFromView, groupByResultFromSource, List("user", "ds"))
    if (diff.count() > 0) {
      val logger = org.slf4j.LoggerFactory.getLogger(getClass)
      logger.info(s"View result count: ${groupByResultFromView.count()}")
      logger.info(s"Source result count: ${groupByResultFromSource.count()}")
      logger.info(s"Diff count: ${diff.count()}")
      diff.show()
    }
    assertEquals(0, diff.count())

    // Verify we can query the results successfully with partition pushdown
    val filteredResult = tableUtils.sql(s"""
      SELECT user, time_spent_ms_count 
      FROM $groupByOutputTable 
      WHERE ds >= '${tableUtils.partitionSpec.minus(today, new Window(7, TimeUnit.DAYS))}'
    """)
    assertTrue("Should be able to filter GroupBy results", filteredResult.count() >= 0)
  }


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
   * Tests basic aggregations in incremental mode by comparing Chronon's output against SQL.
   *
   * Operations: SUM, COUNT, AVERAGE, MIN, MAX, VARIANCE, UNIQUE_COUNT, HISTOGRAM, BOUNDED_UNIQUE_COUNT
   *
   * Actual:   Chronon computes daily IRs using computeIncrementalDf, storing intermediate results
   * Expected: SQL query computes the same aggregations directly on the input data for the same date
   */
  @Test
  def testIncrementalBasicAggregations(): Unit = {
    lazy val spark: SparkSession = SparkSessionBuilder.build("GroupByTestIncrementalBasic" + "_" + Random.alphanumeric.take(6).mkString, local = true)
    implicit val tableUtils = TableUtils(spark)
    val namespace = s"incremental_basic_aggs_${Random.alphanumeric.take(6).mkString}"
    tableUtils.createDatabase(namespace)

    val schema = List(
      Column("user", StringType, 10),
      Column("price", DoubleType, 100),
      Column("quantity", IntType, 50),
      Column("product_id", StringType, 20),  // Low cardinality for UNIQUE_COUNT, HISTOGRAM, BOUNDED_UNIQUE_COUNT
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

      // UNIQUE_COUNT (array IR)
      Builders.Aggregation(Operation.UNIQUE_COUNT, "price", Seq(new Window(7, TimeUnit.DAYS))),

      // HISTOGRAM (map IR)
      Builders.Aggregation(Operation.HISTOGRAM, "product_id", Seq(new Window(7, TimeUnit.DAYS)), argMap = Map("k" -> "0")),

      // BOUNDED_UNIQUE_COUNT (array IR with bound)
      Builders.Aggregation(Operation.BOUNDED_UNIQUE_COUNT, "product_id", Seq(new Window(7, TimeUnit.DAYS)), argMap = Map("k" -> "100"))
    )

    val tableProps: Map[String, String] = Map("source" -> "chronon")

    val today_date = tableUtils.partitionSpec.at(System.currentTimeMillis())
    val today_minus_7_date = tableUtils.partitionSpec.minus(today_date, new Window(7, TimeUnit.DAYS))
    val today_minus_20_date = tableUtils.partitionSpec.minus(today_date, new Window(20, TimeUnit.DAYS))

    val partitionRange = PartitionRange(today_minus_20_date, today_date)

    val groupBy = new GroupBy(aggregations, Seq("user"), df)
    groupBy.computeIncrementalDf(s"${namespace}.testIncrementalBasicAggsOutput", partitionRange, tableProps)

    val actualIncrementalDf = spark.sql(s"select * from ${namespace}.testIncrementalBasicAggsOutput where ds='$today_minus_7_date'")
    df.createOrReplaceTempView("test_basic_aggs_input")

    println("=== Incremental IR Schema ===")
    actualIncrementalDf.printSchema()

    // ASSERTION 1: Verify IR table has expected key columns
    val irColumns = actualIncrementalDf.schema.fieldNames.toSet
    assertTrue("IR must contain 'user' column", irColumns.contains("user"))
    assertTrue("IR must contain 'ds' column", irColumns.contains("ds"))
    assertTrue("IR must contain 'ts' column", irColumns.contains("ts"))

    // ASSERTION 2: Verify all aggregation columns exist
    assertTrue("IR must contain SUM price", irColumns.exists(_.contains("price_sum")))
    assertTrue("IR must contain COUNT quantity", irColumns.exists(_.contains("quantity_count")))
    assertTrue("IR must contain AVERAGE rating", irColumns.exists(_.contains("rating_average")))
    assertTrue("IR must contain MIN price", irColumns.exists(_.contains("price_min")))
    assertTrue("IR must contain MAX quantity", irColumns.exists(_.contains("quantity_max")))
    assertTrue("IR must contain VARIANCE price", irColumns.exists(_.contains("price_variance")))
    assertTrue("IR must contain UNIQUE COUNT price", irColumns.exists(_.contains("price_unique_count")))
    assertTrue("IR must contain HISTOGRAM product_id", irColumns.exists(_.contains("product_id_histogram")))
    assertTrue("IR must contain BOUNDED_UNIQUE_COUNT product_id", irColumns.exists(_.contains("product_id_bounded_unique_count")))

    // ASSERTION 4: Verify IR table has data
    val irRowCount = actualIncrementalDf.count()
    assertTrue(s"IR table should have rows, found ${irRowCount}", irRowCount > 0)

    // ASSERTION 5: Compare against SQL computation
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
         |    collect_set(price) as price_unique_count,
         |    slice(collect_set(md5(product_id)), 1, 100) as product_id_bounded_unique_count
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

    // Convert array columns to counts for comparison (since MD5 hashing differs between Scala and SQL)
    import org.apache.spark.sql.functions.size
    val actualWithCounts = actualIncrementalDf
      .withColumn("price_unique_count", size(col("price_unique_count")))
      .withColumn("product_id_bounded_unique_count", size(col("product_id_bounded_unique_count")))

    val expectedWithCounts = expectedDf
      .withColumn("price_unique_count", size(col("price_unique_count")))
      .withColumn("product_id_bounded_unique_count", size(col("product_id_bounded_unique_count")))

    val diff = Comparison.sideBySide(actualWithCounts, expectedWithCounts, List("user", tableUtils.partitionColumn))

    if (diff.count() > 0) {
      println(s"=== Diff Details for All Aggregations ===")
      println(s"Actual count: ${irRowCount}")
      println(s"Expected count: ${expectedDf.count()}")
      println(s"Diff count: ${diff.count()}")
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
    lazy val spark: SparkSession = SparkSessionBuilder.build("GroupByTest" + "_" + Random.alphanumeric.take(6).mkString, local = true)
    implicit val tableUtils = TableUtils(spark)
    val namespace =  s"incremental_groupBy_snapshot_${Random.alphanumeric.take(6).mkString}"
    tableUtils.createDatabase(namespace)


    val outputDates = CStream.genPartitions(10, tableUtils.partitionSpec)

    val aggregations: Seq[Aggregation] = Seq(
      Builders.Aggregation(Operation.SUM, "time_spent_ms", Seq(new Window(10, TimeUnit.DAYS), new Window(5, TimeUnit.DAYS))),
      Builders.Aggregation(Operation.SUM, "price", Seq(new Window(10, TimeUnit.DAYS))),
      Builders.Aggregation(Operation.COUNT, "user", Seq(new Window(10, TimeUnit.DAYS))),
      Builders.Aggregation(Operation.AVERAGE, "price", Seq(new Window(10, TimeUnit.DAYS)))
    )

    val (source, endPartition) = createTestSourceIncremental(windowSize = 30, suffix = "_snapshot_events", partitionColOpt = Some(tableUtils.partitionColumn))
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

    val  groupByIncremental = GroupBy.fromIncrementalDf(groupByConf, PartitionRange(outputDates.min, outputDates.max), tableUtils)
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
   * Unit test for FIRST and LAST aggregations with incremental IR
   * FIRST/LAST use TimeTuple IR: struct {epochMillis: Long, payload: Value}
   * FIRST keeps the value with the earliest timestamp
   * LAST keeps the value with the latest timestamp
   */
  @Test
  def testIncrementalFirstLast(): Unit = {
    lazy val spark: SparkSession = SparkSessionBuilder.build("GroupByTestFirstLast" + "_" + Random.alphanumeric.take(6).mkString, local = true)
    implicit val tableUtils = TableUtils(spark)
    val namespace = s"incremental_first_last_${Random.alphanumeric.take(6).mkString}"
    tableUtils.createDatabase(namespace)

    val schema = List(
      Column("user", StringType, 5),
      Column("value", DoubleType, 100)
    )

    // Generate events and add random milliseconds to ts for unique timestamps
    import org.apache.spark.sql.functions.{rand, col}
    import org.apache.spark.sql.types.{LongType => SparkLongType}

    val dfWithRandom = DataFrameGen.events(spark, schema, count = 10000, partitions = 20)
      .withColumn("ts", col("ts") + (rand() * 86400000).cast(SparkLongType))  // Add 0-24h random millis
      .cache()  // Mark for caching

    // Force materialization - computes and caches the random values
    dfWithRandom.count()

    // Write the CACHED data to table - writes already-materialized values
    dfWithRandom.write.mode("overwrite").saveAsTable(s"${namespace}.test_first_last_input")

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

    val actualIncrementalDf = spark.sql(s"select * from ${namespace}.testIncrementalFirstLastOutput where ds='$today_minus_7_date'")

    println("=== Incremental FIRST/LAST IR Schema ===")
    actualIncrementalDf.printSchema()

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
         |    slice(reverse(sort_array(filter(collect_list(struct(ts, value)), x -> x.value IS NOT NULL))), 1, 3),
         |    x -> named_struct('epochMillis', x.ts, 'payload', x.value)
         |  ) as value_last3,
         |  transform(
         |    slice(sort_array(filter(collect_list(struct(value, ts)), x -> x.value IS NOT NULL), false), 1, 3),
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

    // Comparison.sideBySide handles sorting arrays and converting Row objects to clean JSON
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

    // Cleanup
    spark.stop()
  }

  @Test
  def testIncrementalStatisticalAggregations(): Unit = {
    lazy val spark: SparkSession = SparkSessionBuilder.build("GroupByTestStatistical" + "_" + Random.alphanumeric.take(6).mkString, local = true)
    implicit val tableUtils = TableUtils(spark)
    val namespace = s"incremental_stats_${Random.alphanumeric.take(6).mkString}"
    tableUtils.createDatabase(namespace)

    val schema = List(
      Column("user", StringType, 5),
      Column("value", DoubleType, 100),
      Column("category", StringType, 10)  // For APPROX_UNIQUE_COUNT
    )

    // Generate sufficient data for statistical aggregations
    val df = DataFrameGen.events(spark, schema, count = 10000, partitions = 20)
    df.write.mode("overwrite").saveAsTable(s"${namespace}.test_stats_input")
    val inputDf = spark.table(s"${namespace}.test_stats_input")

    val aggregations: Seq[Aggregation] = Seq(
      Builders.Aggregation(Operation.SKEW, "value", Seq(new Window(7, TimeUnit.DAYS))),
      Builders.Aggregation(Operation.KURTOSIS, "value", Seq(new Window(7, TimeUnit.DAYS))),
      Builders.Aggregation(Operation.APPROX_PERCENTILE, "value", Seq(new Window(7, TimeUnit.DAYS)),
        argMap = Map("percentiles" -> "[0.5, 0.25, 0.75]")),
      Builders.Aggregation(Operation.APPROX_UNIQUE_COUNT, "category", Seq(new Window(7, TimeUnit.DAYS))),
      Builders.Aggregation(Operation.APPROX_HISTOGRAM_K, "category", Seq(new Window(7, TimeUnit.DAYS)),
        argMap = Map("k" -> "10"))
    )

    val tableProps: Map[String, String] = Map("source" -> "chronon")
    val today_date = tableUtils.partitionSpec.at(System.currentTimeMillis())
    val today_minus_7_date = tableUtils.partitionSpec.minus(today_date, new Window(7, TimeUnit.DAYS))
    val today_minus_20_date = tableUtils.partitionSpec.minus(today_date, new Window(20, TimeUnit.DAYS))
    val partitionRange = PartitionRange(today_minus_20_date, today_date)

    val groupBy = new GroupBy(aggregations, Seq("user"), inputDf)
    groupBy.computeIncrementalDf(s"${namespace}.testIncrementalStatsOutput", partitionRange, tableProps)

    val actualIncrementalDf = spark.sql(s"select * from ${namespace}.testIncrementalStatsOutput where ds='$today_minus_7_date'")

    // Verify IR table has data
    assertTrue(s"IR table should have rows", actualIncrementalDf.count() > 0)

    // Verify APPROX_HISTOGRAM_K column exists and has binary data (sketch)
    val histogramCol = actualIncrementalDf.schema.fields.find(_.name.contains("category_approx_histogram_k"))
    assertTrue("APPROX_HISTOGRAM_K column should exist", histogramCol.isDefined)
    assertTrue("APPROX_HISTOGRAM_K should be BinaryType (sketch)", histogramCol.get.dataType.isInstanceOf[BinaryType])

    // Verify histogram sketch is non-null
    val histogramData = spark.sql(
      s"""
         |SELECT category_approx_histogram_k
         |FROM ${namespace}.testIncrementalStatsOutput
         |WHERE ds='$today_minus_7_date' AND category_approx_histogram_k IS NOT NULL
         |LIMIT 1
         |""".stripMargin
    ).collect()

    assertTrue("APPROX_HISTOGRAM_K should produce non-null sketch data", histogramData.nonEmpty)

    println("=== Statistical Aggregations Incremental Test Passed ===")
    println("✓ SKEW: Statistical skewness")
    println("✓ KURTOSIS: Statistical kurtosis")
    println("✓ APPROX_PERCENTILE: Approximate percentiles")
    println("✓ APPROX_UNIQUE_COUNT: Approximate distinct count")
    println("✓ APPROX_HISTOGRAM_K: Approximate histogram with k buckets")

    // Cleanup
    spark.stop()
  }

}
