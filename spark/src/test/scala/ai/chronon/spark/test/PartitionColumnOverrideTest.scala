package ai.chronon.spark.test

import ai.chronon.aggregator.test.Column
import ai.chronon.api
import ai.chronon.api.{Accuracy, Aggregation, Builders, Operation, StringType, TimeUnit, Window}
import ai.chronon.api.Extensions._
import ai.chronon.spark.Extensions._
import ai.chronon.spark._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.junit.Assert._
import org.junit.{Before, Test}

class PartitionColumnOverrideTest {

  val spark: SparkSession = SparkSessionBuilder.build("PartitionColumnOverrideTest", local = true)
  private val tableUtils = TableUtils(spark)

  private val today = tableUtils.partitionSpec.at(System.currentTimeMillis())
  private val yesterday = tableUtils.partitionSpec.before(today)
  private val monthAgo = tableUtils.partitionSpec.minus(today, new Window(30, TimeUnit.DAYS))
  private val yearAgo = tableUtils.partitionSpec.minus(today, new Window(365, TimeUnit.DAYS))
  private val dayAndMonthBefore = tableUtils.partitionSpec.before(monthAgo)

  private val namespace = "test_namespace_partition_column_override_test"
  private val viewsTable = s"$namespace.view_events"
  private val itemQueriesTable = s"$namespace.item_queries"
  spark.sql(s"CREATE DATABASE IF NOT EXISTS $namespace")

  @Before
  def before(): Unit = {
    spark.sql(s"DROP TABLE IF EXISTS $viewsTable")
    spark.sql(s"DROP TABLE IF EXISTS $itemQueriesTable")
  }

  private def getEventsTemporalJoin(usePartitionOverrideForLeft: Boolean, usePartitionOverrideForRight: Boolean, nameSuffix: String = ""): api.Join = {
    // left side
    val itemQueries = List(Column("item", api.StringType, 100))
    val itemQueriesDf = {
      val rawDf = DataFrameGen.events(spark, itemQueries, 10000, partitions = 100)
      if(usePartitionOverrideForLeft) rawDf.withColumn("ds", regexp_replace(col("ds"), "-", ""))
      else rawDf
    }

    // duplicate the events
    itemQueriesDf.union(itemQueriesDf).save(itemQueriesTable) //.union(itemQueriesDf)

    val start = tableUtils.partitionSpec.minus(today, new Window(100, TimeUnit.DAYS))
    val suffix = if (nameSuffix.isEmpty) "" else s"_$nameSuffix"
    Builders.Join(
      left = {
        if(usePartitionOverrideForLeft) Builders.Source.events(Builders.Query(startPartition = start, selects = Map("ds" -> "from_unixtime(unix_timestamp(ds, 'yyyyMMdd'), 'yyyy-MM-dd')", "item" -> "item")), table = itemQueriesTable)
        else Builders.Source.events(Builders.Query(startPartition = start), table = itemQueriesTable)
      },
      joinParts = Seq(Builders.JoinPart(groupBy = getEventsTemporalViewsGroupBy(nameSuffix, usePartitionOverrideForRight), prefix = "user")),
      metaData =
        Builders.MetaData(name = s"test.item_temporal_features${suffix}", namespace = namespace, team = "item_team")
    )

  }

  private def getEventsTemporalViewsGroupBy(suffix: String, usePartitionOverride: Boolean): api.GroupBy = {
    val viewsSchema = List(
      Column("user", api.StringType, 10000),
      Column("item", api.StringType, 100),
      Column("time_spent_ms", api.LongType, 5000)
    )

    val viewsTable = s"$namespace.view_$suffix"
    val df = {
      val dfRaw = DataFrameGen.events(spark, viewsSchema, count = 10000, partitions = 200)
      if(usePartitionOverride) dfRaw.withColumn("ds", regexp_replace(col("ds"), "-", ""))
      else dfRaw
    }

    val viewsSource = Builders.Source.events(
      table = viewsTable,
      query = {
        if(usePartitionOverride) Builders.Query(selects = Map("time_spent_ms" -> "time_spent_ms", "ds" -> "from_unixtime(unix_timestamp(ds, 'yyyyMMdd'), 'yyyy-MM-dd')", "item" -> "item"), startPartition = yearAgo)
        else Builders.Query(startPartition = yearAgo)
      },
    )

    spark.sql(s"DROP TABLE IF EXISTS $viewsTable")
    val partitionColumns = Seq("ds")
    df.save(viewsTable, Map("tblProp1" -> "1"), partitionColumns = partitionColumns)

    Builders.GroupBy(
      sources = Seq(viewsSource),
      keyColumns = Seq("item"),
      aggregations = Seq(
        Builders.Aggregation(operation = Operation.AVERAGE, inputColumn = "time_spent_ms"),
        Builders.Aggregation(operation = Operation.MIN, inputColumn = "ts"),
        Builders.Aggregation(operation = Operation.MAX, inputColumn = "ts")
      ),
      metaData = Builders.MetaData(name = "unit_test.item_views", namespace = namespace, team = "item_team"),
      accuracy = Accuracy.TEMPORAL
    )
  }

  private def buildExpectedResultSql(usePartitionColumnOverrideOnLeft: Boolean, usePartitionColumnOverrideOnRight: Boolean, start: String, viewsTable: String): String = {
    val lhsPartitionColumn: String = if(usePartitionColumnOverrideOnLeft) "from_unixtime(unix_timestamp(ds, 'yyyyMMdd'), 'yyyy-MM-dd')" else "ds"
    val rhsPartitionColumn: String = if(usePartitionColumnOverrideOnRight) s"from_unixtime(unix_timestamp($viewsTable.ds, 'yyyyMMdd'), 'yyyy-MM-dd')" else s"$viewsTable.ds"
    s"""
       |WITH
       |   queries AS (SELECT item, ts, $lhsPartitionColumn as ds from $itemQueriesTable where $lhsPartitionColumn >= '$start' and $lhsPartitionColumn <= '$dayAndMonthBefore')
       | SELECT queries.item, queries.ts, queries.ds, part.user_unit_test_item_views_ts_min, part.user_unit_test_item_views_ts_max, part.user_unit_test_item_views_time_spent_ms_average
       | FROM (SELECT queries.item,
       |        queries.ts,
       |        queries.ds,
       |        MIN(IF(queries.ts > $viewsTable.ts, $viewsTable.ts, null)) as user_unit_test_item_views_ts_min,
       |        MAX(IF(queries.ts > $viewsTable.ts, $viewsTable.ts, null)) as user_unit_test_item_views_ts_max,
       |        AVG(IF(queries.ts > $viewsTable.ts, time_spent_ms, null)) as user_unit_test_item_views_time_spent_ms_average
       |     FROM queries left outer join $viewsTable
       |     ON queries.item = $viewsTable.item
       |     WHERE $viewsTable.item IS NOT NULL AND $rhsPartitionColumn >= '$yearAgo' AND $rhsPartitionColumn <= '$dayAndMonthBefore'
       |     GROUP BY queries.item, queries.ts, queries.ds) as part
       | JOIN queries
       | ON queries.item <=> part.item AND queries.ts <=> part.ts AND queries.ds <=> part.ds
       |""".stripMargin
  }

  @Test
  def testEventsTemporalWithPartitionColumnOverrideOnLeftAndRightParts(): Unit = {
    val nameSuffix: String = "temporal_events_with_partition_column_overrides_on_left_and_right"
    val joinConf = getEventsTemporalJoin(usePartitionOverrideForLeft = true, usePartitionOverrideForRight = true, nameSuffix)

    val viewsTable = s"$namespace.view_$nameSuffix"
    val start = tableUtils.partitionSpec.minus(today, new Window(100, TimeUnit.DAYS))


    val join = new Join(joinConf = joinConf, endPartition = dayAndMonthBefore, tableUtils)
    val computed = join.computeJoin(Some(100))
    computed.show()
    val expected = tableUtils.sql(buildExpectedResultSql(usePartitionColumnOverrideOnLeft = true, usePartitionColumnOverrideOnRight = true, start = start, viewsTable = viewsTable))
    expected.show()

    val diff = Comparison.sideBySide(computed, expected, List("item", "ts", "ds"))
    val queriesBare =
      tableUtils.sql(s"SELECT item, ts, from_unixtime(unix_timestamp(ds, 'yyyyMMdd'), 'yyyy-MM-dd') as ds from $itemQueriesTable where from_unixtime(unix_timestamp(ds, 'yyyyMMdd'), 'yyyy-MM-dd') >= '$start' and from_unixtime(unix_timestamp(ds, 'yyyyMMdd'), 'yyyy-MM-dd') <= '$dayAndMonthBefore'")
    assertEquals(queriesBare.count(), computed.count())
    if (diff.count() > 0) {
      println(s"Diff count: ${diff.count()}")
      println(s"diff result rows")
      diff
        .replaceWithReadableTime(Seq("ts", "a_user_unit_test_item_views_ts_max", "b_user_unit_test_item_views_ts_max"),
          dropOriginal = true)
        .show()
    }
    assertEquals(diff.count(), 0)
  }

  @Test
  def testEventsTemporalWithPartitionColumnOverrideOnLeftAndNotRight(): Unit = {
    val nameSuffix: String = "temporal_events_with_partition_column_overrides_on_left_and_NOT_right"
    val joinConf = getEventsTemporalJoin(usePartitionOverrideForLeft = true, usePartitionOverrideForRight = false, nameSuffix)

    val viewsTable = s"$namespace.view_$nameSuffix"
    val start = tableUtils.partitionSpec.minus(today, new Window(100, TimeUnit.DAYS))


    val join = new Join(joinConf = joinConf, endPartition = dayAndMonthBefore, tableUtils)
    val computed = join.computeJoin(Some(100))
    computed.show()
    val expected = tableUtils.sql(buildExpectedResultSql(usePartitionColumnOverrideOnLeft = true, usePartitionColumnOverrideOnRight = false, start = start, viewsTable = viewsTable))
    expected.show()

    val diff = Comparison.sideBySide(computed, expected, List("item", "ts", "ds"))
    val queriesBare =
      tableUtils.sql(s"SELECT item, ts, from_unixtime(unix_timestamp(ds, 'yyyyMMdd'), 'yyyy-MM-dd') as ds from $itemQueriesTable where from_unixtime(unix_timestamp(ds, 'yyyyMMdd'), 'yyyy-MM-dd') >= '$start' and from_unixtime(unix_timestamp(ds, 'yyyyMMdd'), 'yyyy-MM-dd') <= '$dayAndMonthBefore'")
    assertEquals(queriesBare.count(), computed.count())
    if (diff.count() > 0) {
      println(s"Diff count: ${diff.count()}")
      println(s"diff result rows")
      diff
        .replaceWithReadableTime(Seq("ts", "a_user_unit_test_item_views_ts_max", "b_user_unit_test_item_views_ts_max"),
          dropOriginal = true)
        .show()
    }
    assertEquals(diff.count(), 0)
  }

  @Test
  def testEventsTemporalWithPartitionColumnOverrideOnRightAndNotLeft(): Unit = {
    val nameSuffix: String = "temporal_events_with_partition_column_overrides_on_right_and_NOT_left"
    val joinConf = getEventsTemporalJoin(usePartitionOverrideForLeft = false, usePartitionOverrideForRight = true, nameSuffix)

    val viewsTable = s"$namespace.view_$nameSuffix"
    val start = tableUtils.partitionSpec.minus(today, new Window(100, TimeUnit.DAYS))


    val join = new Join(joinConf = joinConf, endPartition = dayAndMonthBefore, tableUtils)
    val computed = join.computeJoin(Some(100))
    computed.show()
    val expected = tableUtils.sql(buildExpectedResultSql(usePartitionColumnOverrideOnLeft = false, usePartitionColumnOverrideOnRight = true, start = start, viewsTable = viewsTable))
    expected.show()

    val diff = Comparison.sideBySide(computed, expected, List("item", "ts", "ds"))
    val queriesBare =
      tableUtils.sql(s"SELECT item, ts, ds from $itemQueriesTable where ds >= '$start' and ds <= '$dayAndMonthBefore'")
    assertEquals(queriesBare.count(), computed.count())
    if (diff.count() > 0) {
      println(s"Diff count: ${diff.count()}")
      println(s"diff result rows")
      diff
        .replaceWithReadableTime(Seq("ts", "a_user_unit_test_item_views_ts_max", "b_user_unit_test_item_views_ts_max"),
          dropOriginal = true)
        .show()
    }
    assertEquals(diff.count(), 0)
  }

  @Test
  def testGroupByUploadWithPartitionColumnOverride(): Unit = {
    val eventsTableNoDash = s"$namespace.events_no_dash"
    val eventsTableWithDash = s"$namespace.events_with_dash"
    val eventSchema = List(
      Column("user", StringType, 10),
      Column("list_event", StringType, 100)
    )
    val eventDf = DataFrameGen.events(spark, eventSchema, count = 1000, partitions = 18)
    eventDf.save(s"$eventsTableWithDash")
    eventDf.withColumn("ds", regexp_replace(col("ds"), "-", "")).save(s"$eventsTableNoDash")

    val aggregations: Seq[Aggregation] = Seq(
      Builders.Aggregation(Operation.COUNT, "list_event", Seq(WindowUtils.Unbounded))
    )

    val keys = Seq("user").toArray

    val groupByConfWithDash =
      Builders.GroupBy(
        sources = Seq(Builders.Source.events(Builders.Query(), table = eventsTableWithDash)),
        keyColumns = keys,
        aggregations = aggregations,
        metaData = Builders.MetaData(namespace = namespace, name = "with_dash"),
        accuracy = Accuracy.TEMPORAL
      )


    val groupByConfWithPartitionColumnOverride =
      Builders.GroupBy(
        sources = Seq(Builders.Source.events(Builders.Query(selects = Map("ds" -> "from_unixtime(unix_timestamp(ds, 'yyyyMMdd'), 'yyyy-MM-dd')", "user" -> "user", "list_event" -> "list_event")), table = eventsTableNoDash)),
        keyColumns = keys,
        aggregations = aggregations,
        metaData = Builders.MetaData(namespace = namespace, name = "no_dash"),
        accuracy = Accuracy.TEMPORAL
      )

    GroupByUpload.run(groupByConfWithDash, endDs = yesterday)
    GroupByUpload.run(groupByConfWithPartitionColumnOverride, endDs = yesterday)

    // Value bytes will be different due to underlying metadata not being equal to each other
    // Therefore best we can do is make sure the counts match and that we computed something for each key

    val expected = spark.sql(s"SELECT * FROM $namespace.with_dash_upload").select("key_bytes", "ds")
    val computed = spark.sql(s"SELECT * FROM $namespace.no_dash_upload").select("key_bytes", "ds")
    assertEquals(expected.count(), computed.count())

    val diff = Comparison.sideBySide(computed, expected, List("key_bytes", "ds"))
    if (diff.count() > 0) {
      println("Difference detected in number of keys produced")
      println(s"Diff count: ${diff.count()}")
    }
    assertEquals(diff.count(), 0)
  }

}