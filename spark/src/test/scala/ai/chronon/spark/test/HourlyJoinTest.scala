package ai.chronon.spark.test

import ai.chronon.aggregator.test.Column
import ai.chronon.api
import ai.chronon.api.{Accuracy, Builders, Constants, GroupBy, Join, LongType, Operation, Source, StringType, TimeUnit, Window}
import ai.chronon.api.Extensions._
import ai.chronon.spark.Extensions._
import ai.chronon.spark.GroupBy.renderDataSourceQuery
import ai.chronon.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{StructField, StructType, IntegerType => SparkIntegerType, LongType => SparkLongType, StringType => SparkStringType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.junit.Assert._
import org.junit.{Before, Test}

import scala.collection.JavaConverters._

class HourlyJoinTest {
  val spark: SparkSession = SparkSessionBuilder.build("HourlyJoinTest", local = true)

  private val hourlyTableUtils = TestHourlyTableUtils(spark)

  private val today = hourlyTableUtils.partitionSpec.at(System.currentTimeMillis())
  private val monthAgo = hourlyTableUtils.partitionSpec.minus(today, new Window(30, TimeUnit.DAYS))
  private val yearAgo = hourlyTableUtils.partitionSpec.minus(today, new Window(365, TimeUnit.DAYS))
  private val dayAndMonthBefore = hourlyTableUtils.partitionSpec.before(monthAgo)

  private val namespace = "test_namespace_hourlyjointest"
  private val viewsTable = s"$namespace.view_events"
  val itemQueriesTable = s"$namespace.item_queries"

  val fmt = hourlyTableUtils.partitionSpec.format
  val snapshotEventEventSql =
    s"""
       |WITH
       |  events as (SELECT sess_length, item, ds FROM $viewsTable),
       |  queries as (SELECT item, ts, ds FROM $itemQueriesTable)
       |SELECT
       |  queries.item,
       |  queries.ts,
       |  queries.ds,
       |  SUM(IF(queries.ds > events.ds AND TO_TIMESTAMP(queries.ds, '$fmt') - INTERVAL 6 HOUR <= TO_TIMESTAMP(events.ds, '$fmt'), events.sess_length, null)) as viewgroupby_sess_length_sum_6h,
       |  SUM(IF(queries.ds > events.ds AND TO_TIMESTAMP(queries.ds, '$fmt') - INTERVAL 1 DAY <= TO_TIMESTAMP(events.ds, '$fmt'), events.sess_length, null)) as viewgroupby_sess_length_sum_1d,
       |  SUM(IF(queries.ds > events.ds AND TO_TIMESTAMP(queries.ds, '$fmt') - INTERVAL 2 DAY <= TO_TIMESTAMP(events.ds, '$fmt'), events.sess_length, null)) as viewgroupby_sess_length_sum_2d
       |FROM
       |  queries left outer join events
       |ON
       |  queries.item = events.item
       |GROUP BY
       |  queries.item, queries.ts, queries.ds
       |
       |""".stripMargin

  spark.sql(s"CREATE DATABASE IF NOT EXISTS $namespace")

  def writeItemQueryDf(rows: Seq[Seq[Any]]): Unit = {
    val itemQuerySparkSchema = StructType(
      List(
        StructField("item", SparkStringType, nullable = true),
        StructField("ts", SparkLongType, nullable = true),
        StructField("ds", SparkStringType, nullable = true)
      )
    )
    val itemQueryRdd: RDD[Row] = spark.sparkContext.parallelize(
      rows.map(Row(_: _*))
    )
    val itemQueryDf = spark.createDataFrame(
      itemQueryRdd,
      itemQuerySparkSchema
    )
    itemQueryDf.save(itemQueriesTable)
    println("itemquerydf")
    itemQueryDf.show()
  }

  def writeViewDf(rows: Seq[Seq[Any]]): Unit = {
    val viewSparkSchema = StructType(
      List(
        StructField("item", SparkStringType, nullable = true),
        StructField("sess_length", SparkIntegerType, nullable = true),
        StructField("ts", SparkLongType, nullable = true),
        StructField("ds", SparkStringType, nullable = true)
      )
    )
    val viewRdd: RDD[Row] = spark.sparkContext.parallelize(
      rows.map(Row(_: _*))
    )
    val viewDf = spark.createDataFrame(
      viewRdd,
      viewSparkSchema
    )
    viewDf.save(viewsTable)
    println("viewdf")
    viewDf.show()
  }

  def makeViewEventSource(sourceStartPartition: String, timeColumn: Option[String], lag: Long = 0L): Source = Builders.Source.events(
    query = Builders.Query(
      selects = Builders.Selects("sess_length", "item"),
      startPartition = sourceStartPartition,
      timeColumn = timeColumn.orNull
    ),
    table = viewsTable,
    lag = lag
  )

  def makeItemQueryEventSource(sourceStartPartition: String): Source = Builders.Source.events(
    Builders.Query(startPartition = sourceStartPartition),
    table = itemQueriesTable
  )

  def makeViewGroupBy(accuracy: Accuracy,
                      source: Source
                     ): GroupBy =
    Builders.GroupBy(
      metaData = Builders.MetaData(
        namespace = namespace,
        name = "viewgroupby"
      ),
      sources = List(source),
      keyColumns = List("item"),
      aggregations = List(
        Builders.Aggregation(
          operation = Operation.SUM,
          inputColumn = "sess_length",
          windows = List(
            new Window(6, TimeUnit.HOURS),
            new Window(1, TimeUnit.DAYS),
            new Window(2, TimeUnit.DAYS),
          )
        )
      ),
      accuracy = accuracy
    )

  // LHS: Queries with an `item`
  // RHS: aggregations on sess_length by key `item`
  def makeQueryViewJoin(
                        querySource: Source,
                        groupBy: GroupBy
                       ): Join =
    Builders.Join(
      metaData = Builders.MetaData(
        name = "viewjoin",
        namespace = namespace
      ),
      left = querySource,
      joinParts = Seq(
        Builders.JoinPart(
          groupBy = groupBy
        )
      )
    )

  def compareToSql(join: Join,
                   joinSqlQuery: String,
  ): Unit = {
    val _: DataFrame = new ai.chronon.spark.Join(
      join,
      endPartition = today,
      tableUtils = hourlyTableUtils
    ).computeJoin()

    println("viewjoin join table")
    val joinDf = spark.sql(s"select * from $namespace.viewjoin")
    joinDf.show(numRows = 400)

    val sqlJoinDf = hourlyTableUtils.sql(
      joinSqlQuery
    )
    println("expected sql table")
    sqlJoinDf.show(numRows = 400)

    val diff = Comparison.sideBySide(joinDf, sqlJoinDf, List("ds", "ts", "item"))
    if (diff.count() > 0) {
      println(s"Diff count: ${diff.count()}")
      println(s"diff result rows")
      diff.show()
    }
    assertEquals(diff.count(), 0)
  }

  @Before
  def before(): Unit = {
    spark.sql(s"DROP TABLE IF EXISTS $viewsTable")
    spark.sql(s"DROP TABLE IF EXISTS $itemQueriesTable")
  }

  @Test
  def testSourceQueryRenderHourly(): Unit = {
    writeViewDf(
      Seq(
        Seq("A", 8, 1704222000000L, "2024010219") // Tuesday, January 2, 2024 7:00:00 PM UTC
      )
    )

    val groupBy = makeViewGroupBy(Accuracy.SNAPSHOT, makeViewEventSource("2023123100", None))
    val renderedIncremental = renderDataSourceQuery(
      groupBy,
      groupBy.sources.asScala.head,
      Seq("item"),
      PartitionRange("2024010100", "2024010112")(hourlyTableUtils),
      hourlyTableUtils,
      None,
      Accuracy.SNAPSHOT
    )
    // Ensure that date logic is correct and using desired partitions
    println(renderedIncremental)
    assert(renderedIncremental.contains("(ds >= '2023123100') AND (ds <= '2024010112')"))
    assert(renderedIncremental.contains(s"((UNIX_TIMESTAMP(ds, '$fmt') * 1000) + ${hourlyTableUtils.partitionSpec.spanMillis} - 1) as `ts`"))
  }

  @Test
  def testEventsEventsSnapshotHourlySmall(): Unit = {
    writeItemQueryDf(
      Seq(
        Seq("A", 1704069360000L, "2024010100"), // Monday, January 1, 2024 12:36:00 AM UTC
        Seq("A", 1704075180000L, "2024010102"), // Monday, January 1, 2024 2:13:00 AM UTC
        Seq("A", 1704145200000L, "2024010121"), // Monday, January 1, 2024 9:40:00 PM UTC
        Seq("A", 1704156720000L, "2024010200"), // Tuesday, January 2, 2024 12:52:00 AM UTC
        Seq("A", 1704163680000L, "2024010202"), // Tuesday, January 2, 2024 2:48:00 AM UTC
        Seq("A", 1704237300000L, "2024010223") // Tuesday, January 2, 2024 11:15:00 PM UTC
      )
    )

    writeViewDf(
      Seq(
        Seq("A", 1, 1704067260000L, "2024010100"), // Monday, January 1, 2024 12:01:00 AM UTC
        Seq("A", 3, 1704070800000L, "2024010101"), // Monday, January 1, 2024 1:00:00 AM UTC
        Seq("A", 2, 1704134700000L, "2024010118"), // Monday, January 1, 2024 6:45:00 PM UTC
        Seq("A", 7, 1704153720000L, "2024010200"), // Tuesday, January 2, 2024 12:02:00 AM
        Seq("A", 4, 1704157200000L, "2024010201"), // Tuesday, January 2, 2024 1:00:00 AM UTC
        Seq("A", 8, 1704222000000L, "2024010219") // Tuesday, January 2, 2024 7:00:00 PM UTC
      )
    )

    val queryViewJoin = makeQueryViewJoin(
      makeItemQueryEventSource("2024010100"),
      makeViewGroupBy(
        Accuracy.SNAPSHOT,
        makeViewEventSource("2023123100", Some("ts"))
      )
    )

    compareToSql(
      queryViewJoin,
      joinSqlQuery = snapshotEventEventSql
    )
  }


  /**
   * This exercises logic in GroupBy.renderDataSourceQuery that infers the timestamp
   * from the partition string if no timestamp column is present.
   */
  @Test
  def testEventsEventsSnapshotHourlySmallNoTs(): Unit = {
    writeItemQueryDf(
      Seq(
        Seq("A", 1704069360000L, "2024010100"), // Monday, January 1, 2024 12:36:00 AM UTC
        Seq("A", 1704075180000L, "2024010102"), // Monday, January 1, 2024 2:13:00 AM UTC
        Seq("A", 1704145200000L, "2024010121"), // Monday, January 1, 2024 9:40:00 PM UTC
        Seq("A", 1704156720000L, "2024010200"), // Tuesday, January 2, 2024 12:52:00 AM UTC
        Seq("A", 1704163680000L, "2024010202"), // Tuesday, January 2, 2024 2:48:00 AM UTC
        Seq("A", 1704237300000L, "2024010223") // Tuesday, January 2, 2024 11:15:00 PM UTC
      )
    )

    writeViewDf(
      Seq(
        Seq("A", 1, 1704067260000L, "2024010100"), // Monday, January 1, 2024 12:01:00 AM UTC
        Seq("A", 3, 1704070800000L, "2024010101"), // Monday, January 1, 2024 1:00:00 AM UTC
        Seq("A", 2, 1704134700000L, "2024010118"), // Monday, January 1, 2024 6:45:00 PM UTC
        Seq("A", 7, 1704153720000L, "2024010200"), // Tuesday, January 2, 2024 12:02:00 AM
        Seq("A", 4, 1704157200000L, "2024010201"), // Tuesday, January 2, 2024 1:00:00 AM UTC
        Seq("A", 8, 1704222000000L, "2024010219") // Tuesday, January 2, 2024 7:00:00 PM UTC
      )
    )

    compareToSql(
      makeQueryViewJoin(
        makeItemQueryEventSource("2024010100"),
        makeViewGroupBy(
          Accuracy.SNAPSHOT,
          makeViewEventSource("2023123100", None)
        )
      ),
      joinSqlQuery = snapshotEventEventSql
    )
  }


  @Test
  def testEventsEventsSnapshotHourly(): Unit = {
    val numDays = 4
    val sourceStartPartition = hourlyTableUtils.partitionSpec.minus(today, new Window(numDays, TimeUnit.DAYS))

    val viewsSchema = List(
      Column("sess_length", api.IntType, 5),
      Column("item", api.StringType, 5),
    )
    // Watch out: partitions is used to determine the total NUMBER OF DAYS
    // produced randomly, even in hourly mode. See DataGen.scala#L139
    // TODO: fix this without piping the tableutils all the way in
    DataFrameGen.events(spark, viewsSchema, count = 5000, partitions = numDays,
      optTableUtils = Some(hourlyTableUtils)
    )
      .drop("ts") // Exercising time mapping
      .save(viewsTable)
    println("viewdf")
    hourlyTableUtils.sql(s"SELECT * FROM $viewsTable").show()

    val itemQuerySchema = List(Column("item", api.StringType, 5))
    DataFrameGen.events(spark, itemQuerySchema, count = 5000, partitions = numDays,
      optTableUtils = Some(hourlyTableUtils)
    )
      .save(itemQueriesTable)
    println("itemqueriesdf")
    hourlyTableUtils.sql(s"SELECT * FROM $itemQueriesTable").show()

    compareToSql(
      makeQueryViewJoin(
        makeItemQueryEventSource("2024010100"),
        makeViewGroupBy(
          Accuracy.SNAPSHOT,
          makeViewEventSource("2023123100", None)
        )
      ),
      joinSqlQuery = snapshotEventEventSql
    )
  }

  def temporalEventEventSql(lag: Long = 0L): String =
    s"""
       |WITH
       |  events as (SELECT sess_length, item, ts + $lag as ts, ds FROM $viewsTable),
       |  queries as (SELECT item, ts, ds FROM $itemQueriesTable)
       |SELECT
       |  queries.item,
       |  queries.ts,
       |  queries.ds,
       |  SUM(IF(queries.ts > events.ts AND (FLOOR(queries.ts / ${WindowUtils.FiveMinutes}) * ${WindowUtils.FiveMinutes}) - ${WindowUtils.Hour.millis * 6} <= events.ts, events.sess_length, null)) as viewgroupby_sess_length_sum_6h,
       |  SUM(IF(queries.ts > events.ts AND (FLOOR(queries.ts / ${WindowUtils.Hour.millis}) * ${WindowUtils.Hour.millis}) - ${WindowUtils.Day.millis} <= events.ts, events.sess_length, null)) as viewgroupby_sess_length_sum_1d,
       |  SUM(IF(queries.ts > events.ts AND (FLOOR(queries.ts / ${WindowUtils.Hour.millis}) * ${WindowUtils.Hour.millis}) - ${WindowUtils.Day.millis * 2} <= events.ts, events.sess_length, null)) as viewgroupby_sess_length_sum_2d
       |FROM
       |  queries left outer join events
       |ON
       |  queries.item = events.item
       |GROUP BY
       |  queries.item, queries.ts, queries.ds
       |
       |""".stripMargin

  /**
   This test specifically exercises realtime windows- queries that happen within
   the same 1-hr partition should return different results if events have occurred
   in between said queries.
   */
  @Test
  def testEventsEventsTemporalHourlySmall(): Unit = {
    writeItemQueryDf(
      Seq(
        Seq("A", 1704237240000L, "2024010223"), // Tuesday, January 2, 2024 11:14:00 PM UTC
        Seq("A", 1704237300000L, "2024010223"), // Tuesday, January 2, 2024 11:15:00 PM UTC
        Seq("A", 1704237600000L, "2024010223"), // Tuesday, January 2, 2024 11:20:00 PM UTC
        Seq("A", 1704238200000L, "2024010223"), // Tuesday, January 2, 2024 11:30:00 PM UTC
        Seq("A", 1704239100000L, "2024010223"), // Tuesday, January 2, 2024 11:45:00 PM UTC
      )
    )

    writeViewDf(
      Seq(
        // This first event row exercises sawtooth windows- will be present in first row of query but not second
        Seq("A", 100, 1704215400000L, "2024010217"), // Tuesday, January 2, 2024 5:10:00 PM UTC
        Seq("A", 1, 1704234600000L, "2024010222"), // Tuesday, January 2, 2024 10:30:00 PM UTC
        Seq("A", 3, 1704236700000L, "2024010223"), // Tuesday, January 2, 2024 11:05:00 PM UTC
        Seq("A", 4, 1704237000000L, "2024010223"), // Tuesday, January 2, 2024 11:10:00 PM UTC
        Seq("A", 1, 1704237600000L, "2024010223"), // Tuesday, January 2, 2024 11:20:00 PM UTC
        Seq("A", 3, 1704237900000L, "2024010223"), // Tuesday, January 2, 2024 11:25:00 PM UTC
        Seq("A", 2, 1704238260000L, "2024010223"), // Tuesday, January 2, 2024 11:31:00 PM UTC
      )
    )

    compareToSql(
      makeQueryViewJoin(
        makeItemQueryEventSource("2024010100"),
        makeViewGroupBy(
          Accuracy.TEMPORAL,
          makeViewEventSource("2023123100", None)
        )
      ),
      joinSqlQuery = temporalEventEventSql()
    )
  }

  @Test
  def testEventsEventsTemporalHourlyWithLagSmall(): Unit = {
    writeItemQueryDf(
      Seq(
        Seq("A", 1704069360000L, "2024010100"), // Monday, January 1, 2024 12:36:00 AM UTC
        Seq("A", 1704075180000L, "2024010102"), // Monday, January 1, 2024 2:13:00 AM UTC
        Seq("A", 1704145200000L, "2024010121"), // Monday, January 1, 2024 9:40:00 PM UTC
        Seq("A", 1704156720000L, "2024010200"), // Tuesday, January 2, 2024 12:52:00 AM UTC
        Seq("A", 1704163680000L, "2024010202"), // Tuesday, January 2, 2024 2:48:00 AM UTC
        Seq("A", 1704237300000L, "2024010223") // Tuesday, January 2, 2024 11:15:00 PM UTC
      )
    )

    writeViewDf(
      Seq(
        Seq("A", 1, 1704067260000L, "2024010100"), // Monday, January 1, 2024 12:01:00 AM UTC
        Seq("A", 3, 1704070800000L, "2024010101"), // Monday, January 1, 2024 1:00:00 AM UTC
        Seq("A", 2, 1704134700000L, "2024010118"), // Monday, January 1, 2024 6:45:00 PM UTC
        Seq("A", 7, 1704153720000L, "2024010200"), // Tuesday, January 2, 2024 12:02:00 AM
        Seq("A", 4, 1704157200000L, "2024010201"), // Tuesday, January 2, 2024 1:00:00 AM UTC
        Seq("A", 8, 1704222000000L, "2024010219") // Tuesday, January 2, 2024 7:00:00 PM UTC
      )
    )

    val lag = 2 * 60 * 60 * 1000 // 2 hrs in ms

    compareToSql(
      makeQueryViewJoin(
        makeItemQueryEventSource("2024010100"),
        makeViewGroupBy(
          Accuracy.TEMPORAL,
          makeViewEventSource("2023123100", Some("ts"), lag = lag)
        )
      ),
      joinSqlQuery = temporalEventEventSql(lag = lag),
    )
  }

  @Test
  def testEntitiesEntitiesHourlyWithLag(): Unit = {
    // untimed/unwindowed entities on right
    // right side
    val weightSchema = List(
      Column("user", api.StringType, 1000),
      Column("country", api.StringType, 100),
      Column("weight", api.DoubleType, 500)
    )
    val weightTable = s"$namespace.weights"
    DataFrameGen.entities(spark, weightSchema, 1000, partitions = 400, optTableUtils = Some(hourlyTableUtils)).save(weightTable)
    val lagHours = 2
    val weightSource = Builders.Source.entities(
      query = Builders.Query(selects = Builders.Selects("weight"),
        startPartition = yearAgo,
        endPartition = dayAndMonthBefore),
      snapshotTable = weightTable,
      lag = 1000 * 60 * 60 * lagHours
    )

    val weightGroupBy = Builders.GroupBy(
      sources = Seq(weightSource),
      keyColumns = Seq("country"),
      aggregations = Seq(Builders.Aggregation(operation = Operation.AVERAGE, inputColumn = "weight")),
      metaData = Builders.MetaData(name = "unit_test.country_weights_lag_unittest", namespace = namespace)
    )

    val heightSchema = List(
      Column("user", api.StringType, 1000),
      Column("country", api.StringType, 100),
      Column("height", api.LongType, 200)
    )
    val heightTable = s"$namespace.heights"
    val heightDf = DataFrameGen.entities(spark, heightSchema, 1000, partitions = 400, optTableUtils = Some(hourlyTableUtils))
    println("heightDf")
    heightDf.show()
    heightDf.save(heightTable)
    val heightSource = Builders.Source.entities(
      query = Builders.Query(selects = Builders.Selects("height"), startPartition = monthAgo),
      snapshotTable = heightTable
    )

    val heightGroupBy = Builders.GroupBy(
      sources = Seq(heightSource),
      keyColumns = Seq("country"),
      aggregations = Seq(Builders.Aggregation(operation = Operation.AVERAGE, inputColumn = "height")),
      metaData = Builders.MetaData(name = "unit_test.country_heights_lag_unittest", namespace = namespace)
    )

    // left side
    val countrySchema = List(Column("country", api.StringType, 100))
    val countryTable = s"$namespace.countries"
    val countryDf = DataFrameGen.entities(spark, countrySchema, 1000, partitions = 400, optTableUtils = Some(hourlyTableUtils))
    println("countryDf")
    countryDf.show()
    countryDf.save(countryTable)

    val start = hourlyTableUtils.partitionSpec.minus(today, new Window(60, TimeUnit.DAYS))
    val end = hourlyTableUtils.partitionSpec.minus(today, new Window(15, TimeUnit.DAYS))
    val joinConf = Builders.Join(
      left = Builders.Source.entities(Builders.Query(startPartition = start), snapshotTable = countryTable),
      joinParts = Seq(Builders.JoinPart(groupBy = weightGroupBy), Builders.JoinPart(groupBy = heightGroupBy)),
      metaData = Builders.MetaData(name = "test.country_features_lag_unittest", namespace = namespace, team = "chronon")
    )

    val runner = new ai.chronon.spark.Join(joinConf, end, hourlyTableUtils)
    val computed = runner.computeJoin(Some(7))
    val expected = hourlyTableUtils.sql(
      s"""
         |WITH
         |   countries AS (SELECT country, ds from $countryTable where ds >= '$start' and ds <= '$end'),
         |   grouped_weights AS (
         |      SELECT country,
         |             ds,
         |             avg(weight) as unit_test_country_weights_lag_unittest_weight_average
         |      FROM $weightTable
         |      WHERE ds >= '$yearAgo' and ds <= '$dayAndMonthBefore'
         |      GROUP BY country, ds),
         |   grouped_heights AS (
         |      SELECT country,
         |             ds,
         |             avg(height) as unit_test_country_heights_lag_unittest_height_average
         |      FROM $heightTable
         |      WHERE ds >= '$monthAgo'
         |      GROUP BY country, ds)
         |   SELECT countries.country,
         |        countries.ds,
         |        grouped_weights.unit_test_country_weights_lag_unittest_weight_average,
         |        grouped_heights.unit_test_country_heights_lag_unittest_height_average
         | FROM countries left outer join grouped_weights
         | ON countries.country = grouped_weights.country
         | AND countries.ds = DATE_FORMAT(TO_TIMESTAMP(grouped_weights.ds, '$fmt') + INTERVAL $lagHours HOURS, '$fmt')
         | left outer join grouped_heights
         | ON countries.ds = grouped_heights.ds
         | AND countries.country = grouped_heights.country
    """.stripMargin)

    println("showing intermediate table")
    hourlyTableUtils.sql(
      """
        |SELECT * FROM test_namespace_hourlyjointest.test_country_features_lag_unittest_unit_test_country_heights_lag_unittest
        |""".stripMargin).show()
    println("showing join result")
    computed.show()
    println("showing query result")
    expected.show()
    println(
      s"Left side count: ${spark.sql(s"SELECT country, ds from $countryTable where ds >= '$start' and ds <= '$end'").count()}")
    println(s"Actual count: ${computed.count()}")
    println(s"Expected count: ${expected.count()}")
    val diff = Comparison.sideBySide(computed, expected, List("country", "ds"))
    if (diff.count() > 0) {
      println(s"Diff count: ${diff.count()}")
      println(s"diff result rows")
      diff.show()
    }
    assertEquals(diff.count(), 0)
  }

  @Test
  def testEventsEntitiesSnapshotHourlySmall(): Unit = {
    // Now View is an entity source e.g. cdm.merchants_core. Every partition contains all
    // views occurring before or during the partition interval.
    // e.g. 2024010100 will contain just the first row,
    // but everything between 2024010101 and
    // 2024010117 will contain the first two rows.
    val entities = Seq(
      Seq("A", 1, 1704067260000L, "2024010100"), // Monday, January 1, 2024 12:01:00 AM UTC
      Seq("A", 3, 1704070800000L, "2024010101"), // Monday, January 1, 2024 1:00:00 AM UTC
      Seq("A", 2, 1704134700000L, "2024010118"), // Monday, January 1, 2024 6:45:00 PM UTC
      Seq("A", 7, 1704153720000L, "2024010200"), // Tuesday, January 2, 2024 12:02:00 AM
      Seq("A", 4, 1704157200000L, "2024010201"), // Tuesday, January 2, 2024 1:00:00 AM UTC
      Seq("A", 8, 1704222000000L, "2024010219") // Tuesday, January 2, 2024 7:00:00 PM UTC
    )

    val entitiesViews: Seq[Seq[Any]] = PartitionRange("2024010100", "2024010223")(hourlyTableUtils)
      .toTimePoints // fill every partition with all the events that happened before or during it
      .flatMap { partitionStartTs: Long =>
        val partitionEndTs = partitionStartTs + hourlyTableUtils.partitionSpec.spanMillis
        entities
          // all views in this partition occurred before the end of the current partition
          .filter(r => r(2).asInstanceOf[Long] < partitionEndTs)
          .map { r =>
            // replace the original partition string with the one for the partition
            // we're currently filling
            val partitionString = hourlyTableUtils.partitionSpec.at(partitionStartTs)
            r.take(3) :+ partitionString
          }
      }

    entitiesViews.foreach(println)
    writeViewDf(
      entitiesViews
    )

    writeItemQueryDf(
      Seq(
        Seq("A", 1704069360000L, "2024010100"), // Monday, January 1, 2024 12:36:00 AM UTC
        Seq("A", 1704075180000L, "2024010102"), // Monday, January 1, 2024 2:13:00 AM UTC
        Seq("A", 1704145200000L, "2024010121"), // Monday, January 1, 2024 9:40:00 PM UTC
        Seq("A", 1704156720000L, "2024010200"), // Tuesday, January 2, 2024 12:52:00 AM UTC
        Seq("A", 1704163680000L, "2024010202"), // Tuesday, January 2, 2024 2:48:00 AM UTC
        Seq("A", 1704237300000L, "2024010223") // Tuesday, January 2, 2024 11:15:00 PM UTC
      )
    )

    val entityJoin = makeQueryViewJoin(
      makeItemQueryEventSource("2024010100"),
      makeViewGroupBy(
        Accuracy.SNAPSHOT,
        Builders.Source.entities(
          query = Builders.Query(
            selects = Builders.Selects("sess_length", "item"),
            startPartition = "2023123100",
            timeColumn = "ts"
          ),
          snapshotTable = viewsTable,
        )
      )
    )


    // The way we've constructed the entity datasets above is equivalent
    // to the evented dataset created in testEventsEventsSnapshotHourlySmall.
    // Manually verifying that the rows match the values computed in that test
    val _: DataFrame = new ai.chronon.spark.Join(
      entityJoin,
      endPartition = today,
      tableUtils = hourlyTableUtils,
    ).computeJoin()

    println("viewjoin join table")
    val joinDf = spark.sql(s"select * from $namespace.viewjoin")
    joinDf.show(numRows = 400)

    val rows = joinDf.collect().sortBy(_.getAs[String]("ds")).map(_.toSeq)
    val expected = Seq(
      Seq("A", 1704069360000L, null, null, null, "2024010100"),
      Seq("A", 1704075180000L, 4, 4, 4, "2024010102"),
      Seq("A", 1704145200000L, 2, 6, 6, "2024010121"),
      Seq("A", 1704156720000L, 2, 6, 6, "2024010200"),
      Seq("A", 1704163680000L, 11, 13, 17, "2024010202"),
      Seq("A", 1704237300000L, 8, 19, 25, "2024010223"),
    )
    expected.zip(rows).foreach {
      case (expected, actual) => assert(expected == actual)
    }

  }

}
