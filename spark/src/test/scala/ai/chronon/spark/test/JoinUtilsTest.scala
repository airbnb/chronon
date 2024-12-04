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
import ai.chronon.api.{Builders, Constants}
import ai.chronon.spark.JoinUtils.{contains_any, set_add}
import ai.chronon.spark.{GroupBy, JoinUtils, PartitionRange, SparkSessionBuilder, TableUtils}
import ai.chronon.spark.Extensions._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.junit.Assert._
import org.junit.Test

import scala.collection.mutable
import scala.util.{Random, Try}

class JoinUtilsTest {
  @Test
  def testUDFSetAdd(): Unit = {
    val spark: SparkSession =
      SparkSessionBuilder.build("JoinUtilsTest" + "_" + Random.alphanumeric.take(6).mkString, local = true)
    val data = Seq(
      Row(Seq("a", "b", "c"), "a"),
      Row(Seq("a", "b", "c"), "d"),
      Row(Seq("a", "b", "c"), null),
      Row(null, "a"),
      Row(null, null)
    )

    val schema: StructType = new StructType()
      .add("set", ArrayType(StringType))
      .add("item", StringType)
    val rdd: RDD[Row] = spark.sparkContext.parallelize(data)
    val df: DataFrame = spark.createDataFrame(rdd, schema)

    val actual = df
      .select(set_add(col("set"), col("item")).as("new_set"))
      .collect()
      .map(_.getAs[mutable.WrappedArray[String]](0))

    val expected = Array(
      Seq("a", "b", "c"),
      Seq("a", "b", "c", "d"),
      Seq("a", "b", "c"),
      Seq("a"),
      null
    )

    expected.zip(actual).map {
      case (e, a) => e == a
    }
  }

  @Test
  def testUDFContainsAny(): Unit = {
    val spark: SparkSession =
      SparkSessionBuilder.build("JoinUtilsTest" + "_" + Random.alphanumeric.take(6).mkString, local = true)
    val data = Seq(
      Row(Seq("a", "b", "c"), Seq("a")),
      Row(Seq("a", "b", "c"), Seq("a", "b")),
      Row(Seq("a", "b", "c"), Seq("d")),
      Row(Seq("a", "b", "c"), null),
      Row(null, Seq("a")),
      Row(null, null)
    )

    val schema: StructType = new StructType()
      .add("array", ArrayType(StringType))
      .add("query", ArrayType(StringType))
    val rdd: RDD[Row] = spark.sparkContext.parallelize(data)
    val df: DataFrame = spark.createDataFrame(rdd, schema)

    val actual = df
      .select(contains_any(col("array"), col("query")).as("result"))
      .collect()
      .map(_.getAs[Any](0))

    val expected = Array(
      true, true, false, null, false, null
    )

    expected.zip(actual).map {
      case (e, a) => e == a
    }
  }

  private def testJoinScenario(leftSchema: StructType,
                               rightSchema: StructType,
                               keys: Seq[String],
                               isFailure: Boolean): Try[DataFrame] = {
    val spark: SparkSession =
      SparkSessionBuilder.build("JoinUtilsTest" + "_" + Random.alphanumeric.take(6).mkString, local = true)
    val df = Try(
      JoinUtils.coalescedJoin(
        // using empty dataframe is sufficient to test spark query planning
        spark.createDataFrame(spark.sparkContext.parallelize(Seq[Row]()), leftSchema),
        spark.createDataFrame(spark.sparkContext.parallelize(Seq[Row]()), rightSchema),
        keys
      ))
    if (isFailure) {
      assertTrue(df.isFailure)
    } else {
      assertTrue(df.isSuccess)
    }
    df
  }

  @Test
  def testCoalescedJoinMismatchedKeyColumns(): Unit = {
    // mismatch data type on join keys
    testJoinScenario(
      new StructType()
        .add("key", LongType)
        .add("col1", LongType),
      new StructType()
        .add("key", StringType)
        .add("col2", LongType),
      Seq("key"),
      isFailure = true
    )
  }

  @Test
  def testCoalescedJoinMismatchedSharedColumns(): Unit = {
    // mismatch data type on shared columns
    testJoinScenario(
      new StructType()
        .add("key", LongType)
        .add("col1", LongType),
      new StructType()
        .add("key", LongType)
        .add("col1", StringType),
      Seq("key"),
      isFailure = true
    )
  }

  @Test
  def testCoalescedJoinMissingKeys(): Unit = {
    // missing some keys
    testJoinScenario(
      new StructType()
        .add("key1", LongType)
        .add("key2", LongType)
        .add("col1", LongType),
      new StructType()
        .add("key1", LongType)
        .add("col2", LongType),
      Seq("key1", "key2"),
      isFailure = true
    )
  }

  @Test
  def testCoalescedJoinNoSharedColumns(): Unit = {
    // test no shared columns
    val df = testJoinScenario(
      new StructType()
        .add("key", LongType)
        .add("col1", LongType),
      new StructType()
        .add("key", LongType)
        .add("col2", StringType),
      Seq("key"),
      isFailure = false
    )
    assertEquals(3, df.get.columns.length)
  }

  @Test
  def testCoalescedJoinSharedColumns(): Unit = {
    // test shared columns
    val df = testJoinScenario(
      new StructType()
        .add("key", LongType)
        .add("col1", LongType)
        .add("col2", StringType),
      new StructType()
        .add("key", LongType)
        .add("col1", LongType)
        .add("col3", DoubleType),
      Seq("key"),
      isFailure = false
    )
    assertEquals(4, df.get.columns.length)
  }

  @Test
  def testCoalescedJoinOneSidedLeft(): Unit = {
    // test when left side only has keys
    val df = testJoinScenario(
      new StructType()
        .add("key", LongType),
      new StructType()
        .add("key", LongType)
        .add("col1", LongType)
        .add("col2", DoubleType),
      Seq("key"),
      isFailure = false
    )
    assertEquals(3, df.get.columns.length)
  }

  @Test
  def testCoalescedJoinOneSidedRight(): Unit = {
    // test when right side only has keys
    val df = testJoinScenario(
      new StructType()
        .add("key", LongType)
        .add("col1", LongType)
        .add("col2", DoubleType),
      new StructType()
        .add("key", LongType),
      Seq("key"),
      isFailure = false
    )
    assertEquals(3, df.get.columns.length)
  }

  @Test
  def testCreateJoinView(): Unit = {
    val spark: SparkSession =
      SparkSessionBuilder.build("JoinUtilsTest" + "_" + Random.alphanumeric.take(6).mkString, local = true)
    val tableUtils = TableUtils(spark)
    val namespace = "joinUtil" + "_" + Random.alphanumeric.take(6).mkString
    tableUtils.createDatabase(namespace)
    val finalViewName = s"${namespace}.testCreateView"
    val leftTableName = s"${namespace}.testFeatureTable"
    val rightTableName = s"${namespace}.testLabelTable"
    TestUtils.createSampleFeatureTableDf(spark).write.saveAsTable(leftTableName)
    TestUtils.createSampleLabelTableDf(spark).write.saveAsTable(rightTableName)
    val keys = Array("listing_id", tableUtils.partitionColumn)

    JoinUtils.createOrReplaceView(finalViewName,
                                  leftTableName,
                                  rightTableName,
                                  keys,
                                  tableUtils,
                                  viewProperties = Map("featureTable" -> leftTableName, "labelTable" -> rightTableName))

    val view = tableUtils.sql(s"select * from $finalViewName")
    view.show()
    assertEquals(6, view.count())
    assertEquals(null,
                 view
                   .where(view("ds") === "2022-10-01" && view("listing_id") === "5")
                   .select("label_room_type")
                   .first()
                   .get(0))
    assertEquals("SUPER_HOST",
                 view
                   .where(view("ds") === "2022-10-07" && view("listing_id") === "1")
                   .select("label_host_type")
                   .first()
                   .get(0))

    val properties = tableUtils.getTableProperties(finalViewName)
    assertTrue(properties.isDefined)
    assertEquals(properties.get.get("featureTable"), Some(leftTableName))
    assertEquals(properties.get.get("labelTable"), Some(rightTableName))
  }

  @Test
  def testCreateLatestLabelView(): Unit = {
    val spark: SparkSession =
      SparkSessionBuilder.build("JoinUtilsTest" + "_" + Random.alphanumeric.take(6).mkString, local = true)
    val tableUtils = TableUtils(spark)
    val namespace = "joinUtil" + "_" + Random.alphanumeric.take(6).mkString
    tableUtils.createDatabase(namespace)
    val finalViewName = s"${namespace}.testFinalView"
    val leftTableName = s"${namespace}.testFeatureTable2"
    val rightTableName = s"${namespace}.testLabelTable2"
    TestUtils.createSampleFeatureTableDf(spark).write.saveAsTable(leftTableName)
    tableUtils.insertPartitions(TestUtils.createSampleLabelTableDf(spark),
                                rightTableName,
                                partitionColumns = Seq(tableUtils.partitionColumn, Constants.LabelPartitionColumn))
    val keys = Array("listing_id", tableUtils.partitionColumn)

    JoinUtils.createOrReplaceView(
      finalViewName,
      leftTableName,
      rightTableName,
      keys,
      tableUtils,
      viewProperties = Map(Constants.LabelViewPropertyFeatureTable -> leftTableName,
                           Constants.LabelViewPropertyKeyLabelTable -> rightTableName)
    )
    val view = tableUtils.sql(s"select * from $finalViewName")
    view.show()
    assertEquals(6, view.count())

    //verity latest label view
    val latestLabelView = s"${namespace}.testLatestLabel"
    JoinUtils.createLatestLabelView(latestLabelView,
                                    finalViewName,
                                    tableUtils,
                                    propertiesOverride = Map("newProperties" -> "value"))
    val latest = tableUtils.sql(s"select * from $latestLabelView")
    latest.show()
    assertEquals(2, latest.count())
    assertEquals(0, latest.filter(latest("listing_id") === "3").count())
    assertEquals("2022-11-22", latest.where(latest("ds") === "2022-10-07").select("label_ds").first().get(0))
    // label_ds should be unique per ds + listing
    val removeDup = latest.dropDuplicates(Seq("label_ds", "ds"))
    assertEquals(removeDup.count(), latest.count())

    val properties = tableUtils.getTableProperties(latestLabelView)
    assertTrue(properties.isDefined)
    assertEquals(properties.get.get(Constants.LabelViewPropertyFeatureTable), Some(leftTableName))
    assertEquals(properties.get.get("newProperties"), Some("value"))
  }

  @Test
  def testFilterColumns(): Unit = {
    val spark: SparkSession =
      SparkSessionBuilder.build("JoinUtilsTest" + "_" + Random.alphanumeric.take(6).mkString, local = true)
    val tableUtils = TableUtils(spark)
    val testDf = createSampleTable(spark = spark, tableUtils = tableUtils)
    val filter = Array("listing", "ds", "feature_review")
    val filteredDf = JoinUtils.filterColumns(testDf, filter)
    assertTrue(filteredDf.schema.fieldNames.sorted sameElements filter.sorted)
  }

  @Test
  def testGetRangesToFill(): Unit = {
    val spark: SparkSession =
      SparkSessionBuilder.build("JoinUtilsTest" + "_" + Random.alphanumeric.take(6).mkString, local = true)
    val tableUtils = TableUtils(spark)
    val namespace = "joinUtil" + "_" + Random.alphanumeric.take(6).mkString
    tableUtils.createDatabase(namespace)
    // left table
    val itemQueries = List(Column("item", api.StringType, 100))
    val itemQueriesTable = s"${namespace}.item_queries_table"
    DataFrameGen
      .events(spark, itemQueries, 1000, partitions = 100)
      .save(itemQueriesTable)

    val startPartition = "2023-04-15"
    val endPartition = "2023-08-01"
    val leftSource = Builders.Source.events(Builders.Query(startPartition = startPartition), table = itemQueriesTable)
    val range = JoinUtils.getRangesToFill(leftSource, tableUtils, endPartition)
    assertEquals(range, PartitionRange(startPartition, endPartition)(tableUtils))
  }

  @Test
  def testGetRangesToFillWithOverride(): Unit = {
    val spark: SparkSession =
      SparkSessionBuilder.build("JoinUtilsTest" + "_" + Random.alphanumeric.take(6).mkString, local = true)
    val tableUtils = TableUtils(spark)
    val namespace = "joinUtil" + "_" + Random.alphanumeric.take(6).mkString
    tableUtils.createDatabase(namespace)
    // left table
    val itemQueries = List(Column("item", api.StringType, 100))
    val itemQueriesTable = s"${namespace}.queries_table"
    DataFrameGen
      .events(spark, itemQueries, 1000, partitions = 50)
      .save(itemQueriesTable)

    val startPartition = "2023-04-15"
    val startPartitionOverride = "2023-08-01"
    val endPartition = "2023-08-08"
    val leftSource = Builders.Source.events(Builders.Query(startPartition = startPartition), table = itemQueriesTable)
    val range = JoinUtils.getRangesToFill(leftSource, tableUtils, endPartition, Some(startPartitionOverride))
    assertEquals(range, PartitionRange(startPartitionOverride, endPartition)(tableUtils))
  }

  @Test
  def testGetRangesToFillWithEndPartition(): Unit = {
    val spark: SparkSession =
      SparkSessionBuilder.build("JoinUtilsTest" + "_" + Random.alphanumeric.take(6).mkString, local = true)
    val tableUtils = TableUtils(spark)
    val namespace = "joinUtil" + "_" + Random.alphanumeric.take(6).mkString
    tableUtils.createDatabase(namespace)
    // left table
    val itemQueries = List(Column("item", api.StringType, 100))
    val itemQueriesTable = s"${namespace}.queries_table"
    DataFrameGen
      .events(spark, itemQueries, 1000, partitions = 50)
      .save(itemQueriesTable)

    val startQueryDs = "2023-04-15"
    val startBackfillDs = "2023-08-01"
    val endBackfillDs = "2023-08-08"
    val endQueryDs = "2023-08-15"
    val leftSource = Builders.Source
      .events(Builders.Query(startPartition = startQueryDs, endPartition = endQueryDs), table = itemQueriesTable)
    val range = JoinUtils.getRangesToFill(leftSource, tableUtils, endBackfillDs, Some(startBackfillDs))
    assertEquals(range, PartitionRange(startBackfillDs, endBackfillDs)(tableUtils))
  }

  import ai.chronon.api.{LongType, StringType, StructField, StructType}

  def createSampleTable(tableName: String = "testSampleTable",
                        spark: SparkSession,
                        tableUtils: TableUtils): DataFrame = {
    val schema = StructType(
      tableName,
      Array(
        StructField("listing", LongType),
        StructField("feature_review", LongType),
        StructField("feature_locale", StringType),
        StructField("ds", StringType),
        StructField("ts", StringType)
      )
    )
    val rows = List(
      Row(1L, 20L, "US", "2022-10-01", "2022-10-01 10:00:00"),
      Row(2L, 38L, "US", "2022-10-02", "2022-10-02 11:00:00"),
      Row(3L, 19L, "CA", "2022-10-01", "2022-10-01 08:00:00")
    )
    TestUtils.makeDf(spark, schema, rows)
  }

  @Test
  def testInjectKeyFiter(): Unit = {
    val spark: SparkSession =
      SparkSessionBuilder.build("JoinUtilsTest" + "_" + Random.alphanumeric.take(6).mkString, local = true)
    // Create spark sample dataframe
    val seq = Seq(
      ("A", 1, "2024-01-01"),
      ("B's C", 2, "2024-01-01") // edge case with ' in the string
    )
    val df = spark.createDataFrame(seq).toDF("k", "v", "ds")
    val namespace = "join_util_test" + "_" + Random.alphanumeric.take(6).mkString
    val table = namespace + ".sample_table"
    val tableUtils = TableUtils(spark)
    tableUtils.createDatabase(namespace)
    df.save(table, partitionColumns = Seq("ds"))
    val joinPart = Builders.JoinPart(
      Builders.GroupBy(
        sources = List(
          Builders.Source.events(table = table, query = Builders.Query(selects = Map("k" -> "k", "v" -> "v")))
        ),
        keyColumns = List("k"),
        metaData = Builders.MetaData(name = "join_util_test.test_inject_key_filter")
      )
    )

    val leftSeq = Seq(("B's C", "2024-01-02"))
    val leftDf = spark.createDataFrame(leftSeq).toDF("k", "ds")
    val newJoinPart = JoinUtils.injectKeyFilter(leftDf, joinPart)
    val gb = GroupBy.from(newJoinPart.groupBy,
                          PartitionRange("2024-01-01", "2024-01-01")(tableUtils),
                          tableUtils,
                          computeDependency = false)
    assertEquals(1, gb.inputDf.count())
  }
}
