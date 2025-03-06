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
import ai.chronon.api._
import ai.chronon.spark.Extensions._
import ai.chronon.spark.{Analyzer, Join, SparkSessionBuilder, TableUtils}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, lit}
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
    val viewsGroupBy = getViewsGroupBy("join_analyzer_test.item_gb", Operation.AVERAGE, source = getTestEventSource(namespace), namespace = namespace)
    val anotherViewsGroupBy = getViewsGroupBy("join_analyzer_test.another_item_gb", Operation.SUM,source = getTestEventSource(namespace), namespace = namespace)

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
    val analyzerSchema = analyzer.analyzeJoin(joinConf)._1.map { case (k, v) => s"${k} => ${v}" }.toList.sorted
    val join = new Join(joinConf = joinConf, endPartition = oneMonthAgo, tableUtils)
    val computed = join.computeJoin()
    val expectedSchema = computed.schema.fields.map(field => s"${field.name} => ${field.dataType}").sorted
    logger.info("=== expected schema =====")
    logger.info(expectedSchema.mkString("\n"))

    assertTrue(expectedSchema sameElements analyzerSchema)
  }

  @Test(expected = classOf[java.lang.AssertionError])
  def testJoinAnalyzerValidationFailure(): Unit = {
    val spark: SparkSession = SparkSessionBuilder.build("AnalyzerTest" + "_" + Random.alphanumeric.take(6).mkString, local = true)
    val tableUtils = TableUtils(spark)
    val namespace = "analyzer_test_ns" + "_" + Random.alphanumeric.take(6).mkString
    tableUtils.createDatabase(namespace)
    val viewsGroupBy = getViewsGroupBy("join_analyzer_test.item_gb", Operation.AVERAGE, source = getTestGBSource(namespace), namespace = namespace)
    val usersGroupBy = getUsersGroupBy("join_analyzer_test.user_gb", Operation.AVERAGE, source = getTestGBSource(namespace), namespace = namespace)

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

  @Test(expected = classOf[java.lang.AssertionError])
  def testJoinAnalyzerValidationDataAvailability(): Unit = {
    val spark: SparkSession = SparkSessionBuilder.build("AnalyzerTest" + "_" + Random.alphanumeric.take(6).mkString, local = true)
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

    val viewsGroupBy = Builders.GroupBy(
      sources = Seq(getTestEventSource(namespace)),
      keyColumns = Seq("item_id"),
      aggregations = Seq(
        Builders.Aggregation(windows = Seq(new Window(365, TimeUnit.DAYS)), // greater than one year
                             operation = Operation.AVERAGE,
                             inputColumn = "time_spent_ms")
      ),
      metaData = Builders.MetaData(name = "join_analyzer_test.item_data_avail_gb", namespace = namespace),
      accuracy = Accuracy.SNAPSHOT
    )

    val joinConf = Builders.Join(
      left = Builders.Source.events(Builders.Query(startPartition = start), table = itemQueriesTable),
      joinParts = Seq(
        Builders.JoinPart(groupBy = viewsGroupBy, prefix = "validation", keyMapping = Map("item" -> "item_id"))
      ),
      metaData = Builders.MetaData(name = "test_join_analyzer.item_validation", namespace = namespace, team = "chronon")
    )

    //run analyzer and validate data availability
    val analyzer = new Analyzer(tableUtils, joinConf, oneMonthAgo, today, enableHitter = true)
    analyzer.analyzeJoin(joinConf, validationAssert = true)
  }

  @Test
  def testJoinAnalyzerValidationDataAvailabilityMultipleSources(): Unit = {
    val spark: SparkSession = SparkSessionBuilder.build("AnalyzerTest" + "_" + Random.alphanumeric.take(6).mkString, local = true)
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
    val spark: SparkSession = SparkSessionBuilder.build("AnalyzerTest" + "_" + Random.alphanumeric.take(6).mkString, local = true)
    val tableUtils = TableUtils(spark)
    val namespace = "analyzer_test_ns" + "_" + Random.alphanumeric.take(6).mkString
    tableUtils.createDatabase(namespace)
    // left side
    // create the event source with values
    getTestGBSourceWithTs(namespace=namespace)

    // join parts
    val joinPart = Builders.GroupBy(
      sources = Seq(getTestGBSourceWithTs(namespace=namespace)),
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
    val spark: SparkSession = SparkSessionBuilder.build("AnalyzerTest" + "_" + Random.alphanumeric.take(6).mkString, local = true)
    val tableUtils = TableUtils(spark)
    val namespace = "analyzer_test_ns" + "_" + Random.alphanumeric.take(6).mkString
    tableUtils.createDatabase(namespace)
    // left side
    // create the event source with values out of range
    getTestGBSourceWithTs("out_of_range",namespace=namespace)

    // join parts
    val joinPart = Builders.GroupBy(
      sources = Seq(getTestGBSourceWithTs("out_of_range",namespace=namespace)),
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
    val spark: SparkSession = SparkSessionBuilder.build("AnalyzerTest" + "_" + Random.alphanumeric.take(6).mkString, local = true)
    val tableUtils = TableUtils(spark)
    val namespace = "analyzer_test_ns" + "_" + Random.alphanumeric.take(6).mkString
    tableUtils.createDatabase(namespace)
    // left side
    // create the event source with nulls
    getTestGBSourceWithTs("nulls",namespace=namespace)

    // join parts
    val joinPart = Builders.GroupBy(
      sources = Seq(getTestGBSourceWithTs("nulls",namespace=namespace)),
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
    val spark: SparkSession = SparkSessionBuilder.build("AnalyzerTest" + "_" + Random.alphanumeric.take(6).mkString, local = true)
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
    val spark: SparkSession = SparkSessionBuilder.build("AnalyzerTest" + "_" + Random.alphanumeric.take(6).mkString, local = true)
    val tableUtils = TableUtils(spark)
    val namespace = "analyzer_test_ns" + "_" + Random.alphanumeric.take(6).mkString
    tableUtils.createDatabase(namespace)
    val tableGroupBy = Builders.GroupBy(
      sources = Seq(getTestGBSourceWithTs(namespace=namespace)),
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
    val spark: SparkSession = SparkSessionBuilder.build("AnalyzerTest" + "_" + Random.alphanumeric.take(6).mkString, local = true)
    val tableUtils = TableUtils(spark)
    val namespace = "analyzer_test_ns" + "_" + Random.alphanumeric.take(6).mkString
    tableUtils.createDatabase(namespace)
    val tableGroupBy = Builders.GroupBy(
      sources = Seq(getTestGBSourceWithTs("nulls",namespace=namespace)),
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
    val spark: SparkSession = SparkSessionBuilder.build("AnalyzerTest" + "_" + Random.alphanumeric.take(6).mkString, local = true)
    val tableUtils = spy(TableUtils(spark))
    when(tableUtils.checkTablePermission(any(), any(), any())).thenReturn(false)
    val namespace = "analyzer_test_ns" + "_" + Random.alphanumeric.take(6).mkString
    tableUtils.createDatabase(namespace)
    val tableGroupBy = Builders.GroupBy(
      sources = Seq(getTestGBSourceWithTs(namespace=namespace)),
      keyColumns = Seq("key"),
      aggregations = Seq(
        Builders.Aggregation(operation = Operation.SUM, inputColumn = "col1")
      ),
      metaData = Builders.MetaData(name = "group_by_analyzer_test.test_3", namespace = namespace),
      accuracy = Accuracy.SNAPSHOT
    )

    val analyzer = new Analyzer(tableUtils, tableGroupBy, oneMonthAgo, today)
    val (_, _, noAccessTables) = analyzer.analyzeGroupBy(tableGroupBy, validateTablePermission = true)
    assertEquals(1, noAccessTables.size)
  }

  @Test(expected = classOf[java.lang.AssertionError])
  def testGroupByAnalyzerCheckTimestampOutOfRange(): Unit = {
    val spark: SparkSession = SparkSessionBuilder.build("AnalyzerTest" + "_" + Random.alphanumeric.take(6).mkString, local = true)
    val tableUtils = TableUtils(spark)
    val namespace = "analyzer_test_ns" + "_" + Random.alphanumeric.take(6).mkString
    tableUtils.createDatabase(namespace)
    val tableGroupBy = Builders.GroupBy(
      sources = Seq(getTestGBSourceWithTs("out_of_range",namespace=namespace)),
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
    val spark: SparkSession = SparkSessionBuilder.build("AnalyzerTest" + "_" + Random.alphanumeric.take(6).mkString, local = true)
    val testSchema = List(
      Column("key", api.StringType, 10),
      Column("col1", api.IntType, 10),
      Column("col2", api.IntType, 10)
    )

    val viewsTable = s"$namespace.test_table"
    option match {
      case "default" => {
        DataFrameGen.events(spark, testSchema, count = 100, partitions = 20)
          .save(viewsTable)
      }
      case "nulls" => {
        DataFrameGen.events(spark, testSchema, count = 100, partitions = 20)
          .withColumn("ts", lit(null).cast("bigint")) // set ts to null to test analyzer
          .save(viewsTable)
      }
      case "out_of_range" => {
        DataFrameGen.events(spark, testSchema, count = 100, partitions = 20)
          .withColumn("ts", col("ts")*lit(1000)) // convert to nanoseconds to test analyzer
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
    val spark: SparkSession = SparkSessionBuilder.build("AnalyzerTest" + "_" + Random.alphanumeric.take(6).mkString, local = true)
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

  def getTestEventSource(namespace: String): api.Source = {
    val spark: SparkSession = SparkSessionBuilder.build("AnalyzerTest" + "_" + Random.alphanumeric.take(6).mkString, local = true)
    val viewsSchema = List(
      Column("user", api.StringType, 10000),
      Column("item_id", api.StringType, 100),
      Column("time_spent_ms", api.LongType, 5000)
    )

    val viewsTable = s"$namespace.view_events_gb_table"
    DataFrameGen.events(spark, viewsSchema, count = 1000, partitions = 200).drop("ts").save(viewsTable)

    Builders.Source.events(
      query = Builders.Query(selects = Builders.Selects("time_spent_ms"), startPartition = oneYearAgo),
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

}
