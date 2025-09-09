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

import ai.chronon.aggregator.test.Column
import ai.chronon.api
import ai.chronon.api.Builders.Query
import ai.chronon.api.Extensions.MetadataOps
import ai.chronon.api.{Source, _}
import ai.chronon.spark.Extensions._
import ai.chronon.spark.{Analyzer, Join, SparkSessionBuilder, TableUtils}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, lit, to_json}
import org.junit.Assert.{assertEquals, assertTrue}
import org.junit.Test
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.{never, spy, verify, when}
import org.slf4j.LoggerFactory

import scala.util.Random

class AnalyzerTest {
  @transient lazy val logger = LoggerFactory.getLogger(getClass)
  val dummySpark: SparkSession = SparkSessionBuilder.build("AnalyzerTest", local = true)
  private val dummyTableUtils = TableUtils(dummySpark)

  private val today = dummyTableUtils.partitionSpec.at(System.currentTimeMillis())
  private val oneMonthAgo = dummyTableUtils.partitionSpec.minus(today, new Window(30, TimeUnit.DAYS))
  private val oneYearAgo = dummyTableUtils.partitionSpec.minus(today, new Window(365, TimeUnit.DAYS))

  @Test
  def testJoinAnalyzerSchemaWithValidation(): Unit = {
    val spark: SparkSession = SparkSessionBuilder.build("AnalyzerTest", local = true)
    val tableUtils = TableUtils(spark)
    val namespace = "analyzer_test_ns" + "_" + Random.alphanumeric.take(6).mkString
    tableUtils.createDatabase(namespace)
    val viewsGroupBy = getViewsGroupBy("join_analyzer_test.item_gb",
                                       Operation.AVERAGE,
                                       source = getTestEventSource(namespace),
                                       namespace = namespace)
    val anotherViewsGroupBy = getViewsGroupBy("join_analyzer_test.another_item_gb",
                                              Operation.SUM,
                                              source = getTestEventSource(namespace),
                                              namespace = namespace)

    // left side
    val itemQueries = List(Column("item", api.StringType, 100))
    val itemQueriesTable = s"$namespace.item_queries_table"
    DataFrameGen
      .events(spark, itemQueries, 1000, partitions = 100)
      .save(itemQueriesTable)

    val start = tableUtils.partitionSpec.minus(today, new Window(100, TimeUnit.DAYS))

    // externalParts
    val externalPart = Builders.ExternalPart(
      externalSource = Builders.ContextualSource(
        fields = Array(
          StructField("reservation", StringType),
          StructField("rule_name", StringType)
        )
      )
    )

    val joinConf = Builders.Join(
      left = Builders.Source.events(Builders.Query(startPartition = start), table = itemQueriesTable),
      joinParts = Seq(
        Builders.JoinPart(groupBy = viewsGroupBy, prefix = "prefix_one", keyMapping = Map("item" -> "item_id")),
        Builders.JoinPart(groupBy = anotherViewsGroupBy, prefix = "prefix_two", keyMapping = Map("item" -> "item_id"))
      ),
      externalParts = Seq(externalPart),
      metaData =
        Builders.MetaData(name = "test_join_analyzer.item_snapshot_features", namespace = namespace, team = "chronon")
    )

    //run analyzer and validate output schema
    val analyzer = new Analyzer(tableUtils, joinConf, oneMonthAgo, today, enableHitter = true)
    val analyzerResult = analyzer.analyzeJoin(joinConf)
    val analyzerSchema = (analyzerResult.leftSchema ++ analyzerResult.finalOutputSchema)
      .map { case (k, v) => s"${k} => ${v}" }
      .toList
      .sorted
    val join = new Join(joinConf = joinConf, endPartition = oneMonthAgo, tableUtils)
    val computed = join.computeJoin()
    val expectedSchema = computed.schema.fields.map(field => s"${field.name} => ${field.dataType}").sorted
    logger.info("=== expected schema =====")
    logger.info(expectedSchema.mkString("\n"))

    assertTrue(expectedSchema sameElements analyzerSchema)
  }

  @Test(expected = classOf[java.lang.AssertionError])
  def testJoinAnalyzerValidationFailure(): Unit = {
    val spark: SparkSession =
      SparkSessionBuilder.build("AnalyzerTest" + "_" + Random.alphanumeric.take(6).mkString, local = true)
    val tableUtils = TableUtils(spark)
    val namespace = "analyzer_test_ns" + "_" + Random.alphanumeric.take(6).mkString
    tableUtils.createDatabase(namespace)
    val viewsGroupBy = getViewsGroupBy("join_analyzer_test.item_gb",
                                       Operation.AVERAGE,
                                       source = getTestGBSource(namespace),
                                       namespace = namespace)
    val usersGroupBy = getUsersGroupBy("join_analyzer_test.user_gb",
                                       Operation.AVERAGE,
                                       source = getTestGBSource(namespace),
                                       namespace = namespace)

    // left side
    val itemQueries = List(Column("item", api.StringType, 100), Column("guest", api.StringType, 100))
    val itemQueriesTable = s"$namespace.item_queries_with_user_table"
    DataFrameGen
      .events(spark, itemQueries, 1000, partitions = 100)
      .save(itemQueriesTable)

    val start = tableUtils.partitionSpec.minus(today, new Window(10, TimeUnit.DAYS))

    val joinConf = Builders.Join(
      left = Builders.Source.events(Builders.Query(startPartition = start), table = itemQueriesTable),
      joinParts = Seq(
        Builders.JoinPart(groupBy = viewsGroupBy, prefix = "mismatch", keyMapping = Map("item" -> "item_id")),
        Builders.JoinPart(groupBy = usersGroupBy, prefix = "mismatch", keyMapping = Map("guest" -> "user"))
      ),
      metaData =
        Builders.MetaData(name = "test_join_analyzer.item_type_mismatch", namespace = namespace, team = "chronon")
    )

    //run analyzer and validate output schema
    val analyzer = new Analyzer(tableUtils, joinConf, oneMonthAgo, today, enableHitter = true)
    analyzer.analyzeJoin(joinConf, validationAssert = true)
  }

  def joinValidationDataAvailability(hasSufficientDateRange: Boolean) = {
    val spark: SparkSession =
      SparkSessionBuilder.build("AnalyzerTest" + "_" + Random.alphanumeric.take(6).mkString, local = true)
    val tableUtils = TableUtils(spark)
    val namespace = "analyzer_test_ns" + "_" + Random.alphanumeric.take(6).mkString
    tableUtils.createDatabase(namespace)
    // left side
    val itemQueries = List(Column("item", api.StringType, 100), Column("guest", api.StringType, 100))
    val itemQueriesTable = s"$namespace.item_queries_with_user_table"
    DataFrameGen
      .events(spark, itemQueries, 500, partitions = 100)
      .save(itemQueriesTable)

    val start = tableUtils.partitionSpec.minus(today, new Window(90, TimeUnit.DAYS))

    val windowDays = if (hasSufficientDateRange) 3 else 365
    def buildGroupBy(partitionOpt: Option[String]): GroupBy = {
      Builders.GroupBy(
        sources = Seq(getTestEventSource(namespace, partitionColOpt = partitionOpt)),
        keyColumns = Seq("item_id"),
        aggregations = Seq(
          Builders.Aggregation(windows = Seq(new Window(windowDays, TimeUnit.DAYS)),
                               operation = Operation.AVERAGE,
                               inputColumn = "time_spent_ms")
        ),
        metaData = Builders.MetaData(
          name = "join_analyzer_test.item_data_avail_gb" + partitionOpt.map(s => s"_$s").getOrElse(""),
          namespace = namespace),
        accuracy = Accuracy.SNAPSHOT
      )
    }

    val joinConf = Builders.Join(
      left = Builders.Source.events(Builders.Query(startPartition = start), table = itemQueriesTable),
      joinParts = Seq(
        Builders.JoinPart(groupBy = buildGroupBy(None), prefix = "validation", keyMapping = Map("item" -> "item_id")),
        Builders.JoinPart(groupBy = buildGroupBy(Some("datestr")),
                          prefix = "validation",
                          keyMapping = Map("item" -> "item_id"))
      ),
      metaData = Builders.MetaData(name = "test_join_analyzer.item_validation", namespace = namespace, team = "chronon")
    )

    //run analyzer and validate data availability
    val analyzer = new Analyzer(tableUtils, joinConf, oneMonthAgo, today, enableHitter = true)
    analyzer.analyzeJoin(joinConf, validationAssert = true)
  }

  @Test(expected = classOf[java.lang.AssertionError])
  def testJoinAnalyzerValidationDataAvailabilityFail(): Unit = {
    joinValidationDataAvailability(false)
  }

  @Test
  def testJoinAnalyzerValidationDataAvailability(): Unit = {
    joinValidationDataAvailability(true)
  }

  @Test
  def testJoinAnalyzerValidationDataAvailabilityMultipleSources(): Unit = {
    val spark: SparkSession =
      SparkSessionBuilder.build("AnalyzerTest" + "_" + Random.alphanumeric.take(6).mkString, local = true)
    val tableUtils = TableUtils(spark)
    val namespace = "analyzer_test_ns" + "_" + Random.alphanumeric.take(6).mkString
    tableUtils.createDatabase(namespace)
    val leftSchema = List(Column("item", api.StringType, 100))
    val leftTable = s"$namespace.multiple_sources_left_table"
    val leftData = DataFrameGen.events(spark, leftSchema, 10, partitions = 1)
    leftData.save(leftTable)
    val leftPartitions = tableUtils.partitions(leftTable)
    val leftMinDs = leftPartitions.min
    val leftMaxDs = leftPartitions.max
    logger.info(s"[Multiple sources test] left minDs: $leftMinDs, left maxDs: $leftMaxDs")

    // Create two sources with consecutive partition ranges of 45 days each.
    // For events-events-SNAPSHOT with 90 day window, we need data from ds-91 to ds-1 days
    val firstSourceStartPartition = tableUtils.partitionSpec.shift(leftMinDs, -91)
    val firstSourceEndPartition = tableUtils.partitionSpec.shift(leftMinDs, -46)
    val secondSourceStartPartition = tableUtils.partitionSpec.shift(firstSourceEndPartition, 1)

    val rightSchema = List(
      Column("item", api.StringType, 1000),
      Column("price", api.DoubleType, 5000)
    )

    val rightData = DataFrameGen.events(spark, rightSchema, count = 5000, partitions = 100).drop("ts")
    val rightSourceTable1 = s"$namespace.multiple_sources_right_source1"
    rightData.where(col("ds").between(firstSourceStartPartition, secondSourceStartPartition)).save(rightSourceTable1)
    val rightSourceTable2 = s"$namespace.multiple_sources_right_source2"
    rightData.where(col("ds").between(secondSourceStartPartition, leftMaxDs)).save(rightSourceTable2)

    def buildSource(startPartition: Option[String], endPartition: Option[String], sourceTable: String): api.Source = {
      val query = Builders.Query(selects = Builders.Selects("price"))
      if (startPartition.isDefined) {
        query.setStartPartition(startPartition.get)
      }
      if (endPartition.isDefined) {
        query.setEndPartition(endPartition.get)
      }
      Builders.Source.events(
        query = query,
        table = sourceTable
      )
    }

    val groupBy = Builders.GroupBy(
      sources = Seq(
        buildSource(Some(firstSourceStartPartition), Some(firstSourceEndPartition), rightSourceTable1),
        buildSource(Some(secondSourceStartPartition), None, rightSourceTable2)
      ),
      keyColumns = Seq("item"),
      aggregations = Seq(
        Builders.Aggregation(windows = Seq(new Window(90, TimeUnit.DAYS)), // greater than one year
                             operation = Operation.AVERAGE,
                             inputColumn = "price")
      ),
      metaData = Builders.MetaData(name = "multiple_sources.item_gb", namespace = namespace),
      accuracy = Accuracy.SNAPSHOT
    )

    val joinConf = Builders.Join(
      left = Builders.Source.events(Builders.Query(startPartition = leftMinDs), table = leftTable),
      joinParts = Seq(Builders.JoinPart(groupBy = groupBy)),
      metaData = Builders.MetaData(name = "multiple_sources.join", namespace = namespace)
    )

    val analyzer = new Analyzer(tableUtils, joinConf, leftMinDs, leftMaxDs)
    analyzer.analyzeJoin(joinConf, validationAssert = true)
  }

  @Test
  def testJoinAnalyzerCheckTimestampHasValues(): Unit = {
    val spark: SparkSession =
      SparkSessionBuilder.build("AnalyzerTest" + "_" + Random.alphanumeric.take(6).mkString, local = true)
    val tableUtils = TableUtils(spark)
    val namespace = "analyzer_test_ns" + "_" + Random.alphanumeric.take(6).mkString
    tableUtils.createDatabase(namespace)
    // left side
    // create the event source with values
    getTestGBSourceWithTs(namespace = namespace)

    // join parts
    val joinPart = Builders.GroupBy(
      sources = Seq(getTestGBSourceWithTs(namespace = namespace)),
      keyColumns = Seq("key"),
      aggregations = Seq(
        Builders.Aggregation(operation = Operation.SUM, inputColumn = "col1")
      ),
      metaData = Builders.MetaData(name = "join_analyzer_test.test_1", namespace = namespace),
      accuracy = Accuracy.SNAPSHOT
    )

    val joinConf = Builders.Join(
      left = Builders.Source.events(Builders.Query(startPartition = oneMonthAgo), table = s"$namespace.test_table"),
      joinParts = Seq(
        Builders.JoinPart(groupBy = joinPart, prefix = "validation")
      ),
      metaData = Builders.MetaData(name = "test_join_analyzer.key_validation", namespace = namespace, team = "chronon")
    )

    //run analyzer an ensure ts timestamp values result in analyzer passing
    val analyzer = new Analyzer(tableUtils, joinConf, oneMonthAgo, today, enableHitter = true)
    analyzer.analyzeJoin(joinConf, validationAssert = true)

  }

  @Test(expected = classOf[java.lang.AssertionError])
  def testJoinAnalyzerCheckTimestampOutOfRange(): Unit = {
    val spark: SparkSession =
      SparkSessionBuilder.build("AnalyzerTest" + "_" + Random.alphanumeric.take(6).mkString, local = true)
    val tableUtils = TableUtils(spark)
    val namespace = "analyzer_test_ns" + "_" + Random.alphanumeric.take(6).mkString
    tableUtils.createDatabase(namespace)
    // left side
    // create the event source with values out of range
    getTestGBSourceWithTs("out_of_range", namespace = namespace)

    // join parts
    val joinPart = Builders.GroupBy(
      sources = Seq(getTestGBSourceWithTs("out_of_range", namespace = namespace)),
      keyColumns = Seq("key"),
      aggregations = Seq(
        Builders.Aggregation(operation = Operation.SUM, inputColumn = "col1")
      ),
      metaData = Builders.MetaData(name = "join_analyzer_test.test_1", namespace = namespace),
      accuracy = Accuracy.SNAPSHOT
    )

    val joinConf = Builders.Join(
      left = Builders.Source.events(Builders.Query(startPartition = oneMonthAgo), table = s"$namespace.test_table"),
      joinParts = Seq(
        Builders.JoinPart(groupBy = joinPart, prefix = "validation")
      ),
      metaData = Builders.MetaData(name = "test_join_analyzer.key_validation", namespace = namespace, team = "chronon")
    )

    //run analyzer an ensure ts timestamp values result in analyzer passing
    val analyzer = new Analyzer(tableUtils, joinConf, oneMonthAgo, today, enableHitter = true)
    analyzer.analyzeJoin(joinConf, validationAssert = true)

  }

  @Test(expected = classOf[java.lang.AssertionError])
  def testJoinAnalyzerCheckTimestampAllNulls(): Unit = {
    val spark: SparkSession =
      SparkSessionBuilder.build("AnalyzerTest" + "_" + Random.alphanumeric.take(6).mkString, local = true)
    val tableUtils = TableUtils(spark)
    val namespace = "analyzer_test_ns" + "_" + Random.alphanumeric.take(6).mkString
    tableUtils.createDatabase(namespace)
    // left side
    // create the event source with nulls
    getTestGBSourceWithTs("nulls", namespace = namespace)

    // join parts
    val joinPart = Builders.GroupBy(
      sources = Seq(getTestGBSourceWithTs("nulls", namespace = namespace)),
      keyColumns = Seq("key"),
      aggregations = Seq(
        Builders.Aggregation(operation = Operation.SUM, inputColumn = "col1")
      ),
      metaData = Builders.MetaData(name = "join_analyzer_test.test_1", namespace = namespace),
      accuracy = Accuracy.SNAPSHOT
    )

    val joinConf = Builders.Join(
      left = Builders.Source.events(Builders.Query(startPartition = oneMonthAgo), table = s"$namespace.test_table"),
      joinParts = Seq(
        Builders.JoinPart(groupBy = joinPart, prefix = "validation")
      ),
      metaData = Builders.MetaData(name = "test_join_analyzer.key_validation", namespace = namespace, team = "chronon")
    )

    //run analyzer an ensure ts timestamp values result in analyzer passing
    val analyzer = new Analyzer(tableUtils, joinConf, oneMonthAgo, today, enableHitter = true)
    analyzer.analyzeJoin(joinConf, validationAssert = true)

  }

  @Test(expected = classOf[java.lang.AssertionError])
  def testJoinAnalyzerInvalidTablePermissions(): Unit = {
    val spark: SparkSession =
      SparkSessionBuilder.build("AnalyzerTest" + "_" + Random.alphanumeric.take(6).mkString, local = true)
    val tableUtils = spy(TableUtils(spark))
    when(tableUtils.checkTablePermission(any(), any(), any())).thenReturn(false)
    val namespace = "analyzer_test_ns" + "_" + Random.alphanumeric.take(6).mkString
    tableUtils.createDatabase(namespace)
    // left side
    // create the event source with values
    getTestGBSourceWithTs(namespace = namespace)

    // join parts
    val joinPart = Builders.GroupBy(
      sources = Seq(getTestGBSourceWithTs(namespace = namespace)),
      keyColumns = Seq("key"),
      aggregations = Seq(
        Builders.Aggregation(operation = Operation.SUM, inputColumn = "col1")
      ),
      metaData = Builders.MetaData(name = "join_analyzer_test.test_4", namespace = namespace),
      accuracy = Accuracy.SNAPSHOT
    )

    val joinConf = Builders.Join(
      left = Builders.Source.events(Builders.Query(startPartition = oneMonthAgo), table = s"$namespace.test_table"),
      joinParts = Seq(
        Builders.JoinPart(groupBy = joinPart, prefix = "validation")
      ),
      metaData = Builders.MetaData(name = "test_join_analyzer.key_validation", namespace = namespace, team = "chronon")
    )

    //run analyzer an ensure ts timestamp values result in analyzer passing
    val analyzer = spy(new Analyzer(tableUtils, joinConf, oneMonthAgo, today, enableHitter = true))
    try {
      verify(analyzer, never()).runTimestampChecks(any(), any())
    } catch {
      case e: AssertionError =>
        logger.error("Caught unexpected AssertionError: " + e.getMessage)
        assertTrue("Timestamp checks should not be called with table permission errors", false)
    }
    analyzer.analyzeJoin(joinConf, validationAssert = true)
  }
  @Test
  def testGroupByAnalyzerCheckTimestampHasValues(): Unit = {
    val spark: SparkSession =
      SparkSessionBuilder.build("AnalyzerTest" + "_" + Random.alphanumeric.take(6).mkString, local = true)
    val tableUtils = TableUtils(spark)
    val namespace = "analyzer_test_ns" + "_" + Random.alphanumeric.take(6).mkString
    tableUtils.createDatabase(namespace)
    val tableGroupBy = Builders.GroupBy(
      sources = Seq(getTestGBSourceWithTs(namespace = namespace)),
      keyColumns = Seq("key"),
      aggregations = Seq(
        Builders.Aggregation(operation = Operation.SUM, inputColumn = "col1")
      ),
      metaData = Builders.MetaData(name = "group_by_analyzer_test.test_1", namespace = namespace),
      accuracy = Accuracy.SNAPSHOT
    )

    //run analyzer an ensure ts timestamp values result in analyzer passing
    val analyzer = new Analyzer(tableUtils, tableGroupBy, oneMonthAgo, today)
    analyzer.analyzeGroupBy(tableGroupBy)

  }

  @Test(expected = classOf[java.lang.AssertionError])
  def testGroupByAnalyzerCheckTimestampAllNulls(): Unit = {
    val spark: SparkSession =
      SparkSessionBuilder.build("AnalyzerTest" + "_" + Random.alphanumeric.take(6).mkString, local = true)
    val tableUtils = TableUtils(spark)
    val namespace = "analyzer_test_ns" + "_" + Random.alphanumeric.take(6).mkString
    tableUtils.createDatabase(namespace)
    val tableGroupBy = Builders.GroupBy(
      sources = Seq(getTestGBSourceWithTs("nulls", namespace = namespace)),
      keyColumns = Seq("key"),
      aggregations = Seq(
        Builders.Aggregation(operation = Operation.SUM, inputColumn = "col2")
      ),
      metaData = Builders.MetaData(name = "group_by_analyzer_test.test_2", namespace = namespace),
      accuracy = Accuracy.TEMPORAL
    )

    //run analyzer and trigger assertion error when timestamps are all NULL
    val analyzer = new Analyzer(tableUtils, tableGroupBy, oneMonthAgo, today)
    analyzer.analyzeGroupBy(tableGroupBy)
  }

  @Test
  def testGroupByAnalyzerInvalidTablePermissions(): Unit = {
    val spark: SparkSession =
      SparkSessionBuilder.build("AnalyzerTest" + "_" + Random.alphanumeric.take(6).mkString, local = true)
    val tableUtils = spy(TableUtils(spark))
    when(tableUtils.checkTablePermission(any(), any(), any())).thenReturn(false)
    val namespace = "analyzer_test_ns" + "_" + Random.alphanumeric.take(6).mkString
    tableUtils.createDatabase(namespace)
    val tableGroupBy = Builders.GroupBy(
      sources = Seq(getTestGBSourceWithTs(namespace = namespace)),
      keyColumns = Seq("key"),
      aggregations = Seq(
        Builders.Aggregation(operation = Operation.SUM, inputColumn = "col1")
      ),
      metaData = Builders.MetaData(name = "group_by_analyzer_test.test_3", namespace = namespace),
      accuracy = Accuracy.SNAPSHOT
    )

    val analyzer = new Analyzer(tableUtils, tableGroupBy, oneMonthAgo, today)
    val analyzerResult = analyzer.analyzeGroupBy(tableGroupBy)
    assertEquals(1, analyzerResult.noAccessTables.size)
  }

  @Test(expected = classOf[java.lang.AssertionError])
  def testGroupByAnalyzerCheckTimestampOutOfRange(): Unit = {
    val spark: SparkSession =
      SparkSessionBuilder.build("AnalyzerTest" + "_" + Random.alphanumeric.take(6).mkString, local = true)
    val tableUtils = TableUtils(spark)
    val namespace = "analyzer_test_ns" + "_" + Random.alphanumeric.take(6).mkString
    tableUtils.createDatabase(namespace)
    val tableGroupBy = Builders.GroupBy(
      sources = Seq(getTestGBSourceWithTs("out_of_range", namespace = namespace)),
      keyColumns = Seq("key"),
      aggregations = Seq(
        Builders.Aggregation(operation = Operation.SUM, inputColumn = "col2")
      ),
      metaData = Builders.MetaData(name = "group_by_analyzer_test.test_3", namespace = namespace),
      accuracy = Accuracy.TEMPORAL
    )

    //run analyzer and trigger assertion error when timestamps are all NULL
    val analyzer = new Analyzer(tableUtils, tableGroupBy, oneMonthAgo, today)
    analyzer.analyzeGroupBy(tableGroupBy)

  }

  def getTestGBSourceWithTs(option: String = "default", namespace: String): api.Source = {
    val spark: SparkSession =
      SparkSessionBuilder.build("AnalyzerTest" + "_" + Random.alphanumeric.take(6).mkString, local = true)
    val testSchema = List(
      Column("key", api.StringType, 10),
      Column("col1", api.IntType, 10),
      Column("col2", api.IntType, 10)
    )

    val viewsTable = s"$namespace.test_table"
    option match {
      case "default" => {
        DataFrameGen
          .events(spark, testSchema, count = 100, partitions = 20)
          .save(viewsTable)
      }
      case "nulls" => {
        DataFrameGen
          .events(spark, testSchema, count = 100, partitions = 20)
          .withColumn("ts", lit(null).cast("bigint")) // set ts to null to test analyzer
          .save(viewsTable)
      }
      case "out_of_range" => {
        DataFrameGen
          .events(spark, testSchema, count = 100, partitions = 20)
          .withColumn("ts", col("ts") * lit(1000)) // convert to nanoseconds to test analyzer
          .save(viewsTable)
      }
      case _ => {
        throw new IllegalArgumentException(s"$option is not a valid timestamp generation option")
      }
    }

    val out = Builders.Source.events(
      query = Builders.Query(selects = Builders.Selects("col1", "col2"), startPartition = oneYearAgo),
      table = viewsTable
    )

    out

  }

  def getTestGBSource(namespace: String): api.Source = {
    val spark: SparkSession =
      SparkSessionBuilder.build("AnalyzerTest" + "_" + Random.alphanumeric.take(6).mkString, local = true)
    val viewsSchema = List(
      Column("user", api.StringType, 10000),
      Column("item_id", api.IntType, 100), // type mismatch
      Column("time_spent_ms", api.LongType, 5000),
      Column("user_review", api.LongType, 5000)
    )

    val viewsTable = s"$namespace.view_events_gb_table_2"
    DataFrameGen.events(spark, viewsSchema, count = 1000, partitions = 200).drop("ts").save(viewsTable)

    Builders.Source.events(
      query = Builders.Query(selects = Builders.Selects("time_spent_ms", "user_review"), startPartition = oneYearAgo),
      table = viewsTable
    )
  }

  def getTestEventSource(namespace: String,
                         partitionColOpt: Option[String] = None,
                         selects: Option[Map[String, String]] = None): api.Source = {
    val spark: SparkSession =
      SparkSessionBuilder.build("AnalyzerTest" + "_" + Random.alphanumeric.take(6).mkString, local = true)
    val viewsSchema = List(
      Column("user", api.StringType, 10000),
      Column("item_id", api.StringType, 100),
      Column("time_spent_ms", api.LongType, 5000)
    )

    val viewsTable = s"$namespace.view_events_gb_table" + partitionColOpt.map(s => s"_$s").getOrElse("")
    val df = DataFrameGen
      .events(spark, viewsSchema, count = 1000, partitions = 200, partitionColOpt = partitionColOpt)
      .drop("ts")
    partitionColOpt match {
      case Some(partition) => df.save(viewsTable, partitionColumns = Seq(partition))
      case None            => df.save(viewsTable)
    }
    Builders.Source.events(
      query = Builders.Query(selects = selects.getOrElse(Builders.Selects("time_spent_ms")),
                             startPartition = oneYearAgo,
                             partitionColumn = partitionColOpt.orNull),
      table = viewsTable
    )
  }

  def getViewsGroupBy(name: String, operation: Operation, source: api.Source, namespace: String): api.GroupBy = {
    Builders.GroupBy(
      sources = Seq(source),
      keyColumns = Seq("item_id"),
      aggregations = Seq(
        Builders.Aggregation(operation = operation, inputColumn = "time_spent_ms")
      ),
      metaData = Builders.MetaData(name = name, namespace = namespace),
      accuracy = Accuracy.SNAPSHOT
    )
  }

  def getUsersGroupBy(name: String, operation: Operation, source: api.Source, namespace: String): api.GroupBy = {
    Builders.GroupBy(
      sources = Seq(source),
      keyColumns = Seq("user"),
      aggregations = Seq(
        Builders.Aggregation(operation = operation, inputColumn = "user_review")
      ),
      metaData = Builders.MetaData(name = name, namespace = namespace),
      accuracy = Accuracy.SNAPSHOT
    )
  }

  def getComplexGroupBy(name: String, namespace: String): api.GroupBy = {
    // Create a GroupBy that generate features with complex data type for schema testing
    // Should cover STRUCT, LIST, MAP scenarios
    val source = getTestEventSource(
      namespace,
      selects = Some(
        Builders.Selects.exprs(
          "time_spent_ms" -> "time_spent_ms",
          "view_struct" -> "NAMED_STRUCT('item_id', item_id, 'time_spent_ms', time_spent_ms)",
          "view_map" -> "MAP(item_id, time_spent_ms)"
        ))
    )
    Builders.GroupBy(
      sources = Seq(source),
      keyColumns = Seq("user"),
      aggregations = Seq(
        Builders.Aggregation(operation = Operation.AVERAGE,
                             inputColumn = "time_spent_ms",
                             windows = Seq(new Window(7, TimeUnit.DAYS))),
        Builders.Aggregation(operation = Operation.SUM,
                             inputColumn = "time_spent_ms",
                             windows = Seq(new Window(7, TimeUnit.DAYS))),
        Builders.Aggregation(operation = Operation.LAST,
                             inputColumn = "time_spent_ms",
                             windows = Seq(new Window(7, TimeUnit.DAYS))),
        Builders.Aggregation(operation = Operation.LAST_K, argMap = Map("k" -> "100"), inputColumn = "view_struct"),
        Builders.Aggregation(operation = Operation.LAST_K, argMap = Map("k" -> "100"), inputColumn = "view_map")
      ),
      derivations = Seq(
        Builders.Derivation.star(),
        Builders.Derivation(
          name = "time_spent_ms_avg_last_7d_diff",
          expression = "time_spent_ms_last_7d - time_spent_ms_average_7d"
        )
      ),
      metaData = Builders.MetaData(name = name, namespace = namespace),
      accuracy = Accuracy.SNAPSHOT
    )
  }

  @Test
  def testJoinAnalyzerSchemaExport(): Unit = {
    val spark: SparkSession = SparkSessionBuilder.build("AnalyzerTest", local = true)
    val tableUtils = TableUtils(spark)
    val namespace = "analyzer_test_ns" + "_" + Random.alphanumeric.take(6).mkString
    tableUtils.createDatabase(namespace)

    // GroupBy
    val complexGroupBy = getComplexGroupBy("analyzer_test.complex_gb", namespace)

    // Left side
    val itemQueries = List(Column("item", api.StringType, 100), Column("user_id", api.StringType, 100))
    val itemQueriesTable = s"$namespace.item_queries_table"
    DataFrameGen
      .events(spark, itemQueries, 1000, partitions = 100)
      .save(itemQueriesTable)
    val start = tableUtils.partitionSpec.minus(today, new Window(100, TimeUnit.DAYS))

    // externalParts
    val externalPart = Builders.ExternalPart(
      externalSource = Builders.ContextualSource(
        fields = Array(
          StructField("user_tenure", LongType)
        )
      )
    )

    // join
    val joinConf = Builders.Join(
      left = Builders.Source.events(Builders.Query(startPartition = start), table = itemQueriesTable),
      joinParts = Seq(
        Builders.JoinPart(groupBy = complexGroupBy, prefix = "prefix", keyMapping = Map("user_id" -> "user"))
      ),
      externalParts = Seq(externalPart),
      derivations = Seq(
        Builders.Derivation.star(),
        Builders
          .Derivation(
            expression = "prefix_analyzer_test_complex_gb_time_spent_ms_avg_last_7d_diff / ext_contextual_user_tenure",
            name = "analyzer_test_time_spent_ms_sum_7d_diff_per_tenure"
          )
      ),
      metaData = Builders.MetaData(name = "analyzer_test.complex_join", namespace = namespace, team = "chronon")
    )

    // run analyzer and validate output schema
    val analyzer = new Analyzer(tableUtils,
                                joinConf,
                                oneMonthAgo,
                                today,
                                enableHitter = false,
                                skipTimestampCheck = true,
                                validateTablePermission = false)
    val analyzerResult = analyzer.runAnalyzeJoin(joinConf, exportSchema = true)

    // expected schema
    val expectedLeftSchema = Seq(
      ("item", api.StringType),
      ("user_id", api.StringType),
      ("ts", api.LongType),
      ("ds", api.StringType)
    )
    val structTypeName = "View_struct_last100ElementStruct"
    val expectedRightSchema = Seq(
      ("prefix_analyzer_test_complex_gb_time_spent_ms_average_7d", api.DoubleType),
      ("prefix_analyzer_test_complex_gb_time_spent_ms_sum_7d", api.LongType),
      ("prefix_analyzer_test_complex_gb_time_spent_ms_last_7d", api.LongType),
      ("prefix_analyzer_test_complex_gb_view_struct_last100",
       api.ListType(
         api.StructType(structTypeName,
                        Array(StructField("item_id", api.StringType), StructField("time_spent_ms", api.LongType))))),
      ("prefix_analyzer_test_complex_gb_view_map_last100", api.ListType(api.MapType(api.StringType, api.LongType))),
      ("prefix_analyzer_test_complex_gb_time_spent_ms_avg_last_7d_diff", api.DoubleType),
      ("ext_contextual_user_tenure", api.LongType)
    )

    val expectedKeySchema = Seq(
      ("user_id", api.StringType)
    )
    val expectedOutputSchema = Seq(
      ("prefix_analyzer_test_complex_gb_time_spent_ms_average_7d", api.DoubleType),
      ("prefix_analyzer_test_complex_gb_time_spent_ms_sum_7d", api.LongType),
      ("prefix_analyzer_test_complex_gb_time_spent_ms_last_7d", api.LongType),
      ("prefix_analyzer_test_complex_gb_view_struct_last100",
       api.ListType(
         api.StructType(structTypeName,
                        Array(StructField("item_id", api.StringType), StructField("time_spent_ms", api.LongType))))),
      ("prefix_analyzer_test_complex_gb_view_map_last100", api.ListType(api.MapType(api.StringType, api.LongType))),
      ("prefix_analyzer_test_complex_gb_time_spent_ms_avg_last_7d_diff", api.DoubleType),
      ("ext_contextual_user_tenure", api.LongType),
      ("analyzer_test_time_spent_ms_sum_7d_diff_per_tenure", api.DoubleType)
    )

    // Assertions on analyzer result
    assertEquals(expectedLeftSchema, analyzerResult.leftSchema)
    assertEquals(expectedRightSchema, analyzerResult.rightSchema)
    assertEquals(expectedKeySchema, analyzerResult.keySchema)
    assertEquals(expectedOutputSchema, analyzerResult.finalOutputSchema)

    // Validate exported schema
    val schema = tableUtils.sparkSession
      .table(joinConf.metaData.schemaTable)
      .select(to_json(col("key_schema")).as("key_schema"), to_json(col("value_schema")).as("value_schema"))
      .head
    val keySchema = schema.getString(0)
    val valueSchema = schema.getString(1)

    val keySchemaExpected = """[{"name":"user_id","data_type":"string"}]"""
    val valueSchemaExpected =
      """[
        |{"name":"prefix_analyzer_test_complex_gb_time_spent_ms_average_7d","data_type":"double"},
        |{"name":"prefix_analyzer_test_complex_gb_time_spent_ms_sum_7d","data_type":"bigint"},
        |{"name":"prefix_analyzer_test_complex_gb_time_spent_ms_last_7d","data_type":"bigint"},
        |{"name":"prefix_analyzer_test_complex_gb_view_struct_last100","data_type":"array<struct<item_id:string,time_spent_ms:bigint>>"},
        |{"name":"prefix_analyzer_test_complex_gb_view_map_last100","data_type":"array<map<string,bigint>>"},
        |{"name":"prefix_analyzer_test_complex_gb_time_spent_ms_avg_last_7d_diff","data_type":"double"},
        |{"name":"ext_contextual_user_tenure","data_type":"bigint"},
        |{"name":"analyzer_test_time_spent_ms_sum_7d_diff_per_tenure","data_type":"double"}
        |]""".stripMargin.replaceAll("\\s+", "")

    assertEquals(keySchemaExpected, keySchema)
    assertEquals(valueSchemaExpected, valueSchema)
  }

  @Test
  def testAvroSchemaValidationWithSupportedTypes(): Unit = {
    val spark: SparkSession =
      SparkSessionBuilder.build("AnalyzerTest" + "_" + Random.alphanumeric.take(6).mkString, local = true)

    val tableUtilsWithValidation = new TableUtils(spark) {
      override val chrononAvroSchemaValidation: Boolean = true
    }

    val namespace = "analyzer_test_ns" + "_" + Random.alphanumeric.take(6).mkString
    tableUtilsWithValidation.createDatabase(namespace)

    // Create test data with only Avro-supported types
    import spark.implicits._
    val currentTime = System.currentTimeMillis()
    val dateString = "2025-09-01"
    val testData = Seq(
      ("key1", 100, 1000L, "value1", true, currentTime, dateString),
      ("key2", 200, 2000L, "value2", false, currentTime, dateString),
      ("key1", 150, 1500L, "value3", true, currentTime, dateString)
    ).toDF("key", "int_col", "long_col", "string_col", "boolean_col", "ts", "ds")

    val tableName = "supported_types_table"
    val supportedTypesTable = s"$namespace.$tableName"

    // Create partitioned table by ds column
    testData.write
      .mode("overwrite")
      .partitionBy("ds")
      .saveAsTable(supportedTypesTable)

    // Create Source using Builders.Source.events
    val testSource = Builders.Source.events(
      query = Builders.Query(
        selects = Builders.Selects("key", "int_col", "long_col", "string_col", "boolean_col"),
        startPartition = dateString
      ),
      table = supportedTypesTable
    )

    val tableGroupBy = Builders.GroupBy(
      sources = Seq(testSource),
      keyColumns = Seq("key"),
      aggregations = Seq(
        Builders.Aggregation(operation = Operation.SUM, inputColumn = "int_col"),
        Builders.Aggregation(operation = Operation.MAX, inputColumn = "long_col")
      ),
      metaData = Builders.MetaData(name = "group_by_analyzer_test_supported", namespace = namespace),
      accuracy = Accuracy.TEMPORAL
    )

    // Should succeed without throwing - all types are Avro compatible
    val analyzer = new Analyzer(tableUtilsWithValidation, tableGroupBy, oneMonthAgo, today)
    new Analyzer(
      tableUtilsWithValidation,
      tableGroupBy,
      "2025-09-01",
      today,
      enableHitter = false,
      skipTimestampCheck = true,
      validateTablePermission = false)
    analyzer.analyzeGroupBy(tableGroupBy)
  }

  @Test(expected = classOf[RuntimeException])
  def testAvroSchemaValidationWithUnsupportedTypes(): Unit = {
    val spark: SparkSession =
      SparkSessionBuilder.build("AnalyzerTest" + "_" + Random.alphanumeric.take(6).mkString, local = true)

    val tableUtilsWithValidation = new TableUtils(spark) {
      override val chrononAvroSchemaValidation: Boolean = true
    }

    val namespace = "analyzer_test_ns" + "_" + Random.alphanumeric.take(6).mkString
    tableUtilsWithValidation.createDatabase(namespace)

    // Create test data with BinaryType data
    import spark.implicits._
    val currentTime = System.currentTimeMillis()
    val dateString = "2025-09-01"
    val testData = Seq(
      ("key1", "binary_data_1".getBytes("UTF-8"), currentTime, dateString),
      ("key2", "binary_data_2".getBytes("UTF-8"), currentTime, dateString),
      ("key1", "binary_data_3".getBytes("UTF-8"), currentTime, dateString)
    ).toDF("key", "binary_col", "ts", "ds")

    val tableName = "binary_types_table"
    val binaryTypesTable = s"$namespace.$tableName"

    // Create partitioned table by ds column
    testData.write
      .mode("overwrite")
      .partitionBy("ds")
      .saveAsTable(binaryTypesTable)

    // Create Source using Builders.Source.events
    val testSource = Builders.Source.events(
      query = Builders.Query(
        selects = Builders.Selects("key", "binary_col"),
        startPartition = dateString
      ),
      table = binaryTypesTable
    )

    val tableGroupBy = Builders.GroupBy(
      sources = Seq(testSource),
      keyColumns = Seq("key"),
      aggregations = Seq(
        Builders.Aggregation(operation = Operation.UNIQUE_COUNT, inputColumn = "binary_col")
      ),
      metaData = Builders.MetaData(name = "group_by_analyzer_test_binary", namespace = namespace),
      accuracy = Accuracy.TEMPORAL
    )

    // Should throw RuntimeException due to BinaryType in schema
    val analyzer = new Analyzer(
      tableUtilsWithValidation,
      tableGroupBy,
      "2025-09-01",
      today,
      enableHitter = false,
      skipTimestampCheck = true,
      validateTablePermission = false)
    analyzer.analyzeGroupBy(tableGroupBy)
  }

  @Test
  def testAvroSchemaValidationDisabled(): Unit = {
    val spark: SparkSession =
      SparkSessionBuilder.build("AnalyzerTest" + "_" + Random.alphanumeric.take(6).mkString, local = true)

    val tableUtils = TableUtils(spark)
    val namespace = "analyzer_test_ns" + "_" + Random.alphanumeric.take(6).mkString
    tableUtils.createDatabase(namespace)

    // Create test data with BinaryType data
    import spark.implicits._
    val currentTime = System.currentTimeMillis()
    val dateString = "2025-09-01"
    val testData = Seq(
      ("key1", "binary_data_1".getBytes("UTF-8"), currentTime, dateString),
      ("key2", "binary_data_2".getBytes("UTF-8"), currentTime, dateString)
    ).toDF("key", "binary_col", "ts", "ds")

    val tableName = "binary_disabled_table"
    val binaryDisabledTable = s"$namespace.$tableName"

    // Create partitioned table by ds column
    testData.write
      .mode("overwrite")
      .partitionBy("ds")
      .saveAsTable(binaryDisabledTable)

    // Create Source using Builders.Source.events
    val testSource = Builders.Source.events(
      query = Builders.Query(
        selects = Builders.Selects("key", "binary_col"),
        startPartition = dateString
      ),
      table = binaryDisabledTable
    )

    val tableGroupBy = Builders.GroupBy(
      sources = Seq(testSource),
      keyColumns = Seq("key"),
      aggregations = Seq(
        Builders.Aggregation(operation = Operation.UNIQUE_COUNT, inputColumn = "binary_col")
      ),
      metaData = Builders.MetaData(name = "group_by_analyzer_test_disabled", namespace = namespace),
      accuracy = Accuracy.TEMPORAL
    )

    // Should succeed because validation is disabled
    val analyzer = new Analyzer(tableUtils, tableGroupBy, "2025-09-01", today)
    analyzer.analyzeGroupBy(tableGroupBy)
  }
}
