package ai.chronon.spark.test

import ai.chronon.aggregator.test.Column
import ai.chronon.api
import ai.chronon.api.{Accuracy, Builders, Constants, LongType, Operation, StringType, TimeUnit, Window}
import ai.chronon.api.Extensions._
import ai.chronon.spark.Extensions._
import ai.chronon.spark.GroupBy.{renderDataSourceQuery, renderUnpartitionedDataSourceQuery}
import ai.chronon.spark._
import ai.chronon.spark.stats.SummaryJob
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StructField, StructType, StringType => SparkStringType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.junit.Assert._
import org.junit.{Before, Test}

import scala.collection.JavaConverters._
import scala.util.ScalaJavaConversions.ListOps

class JoinTest {

  val spark: SparkSession = SparkSessionBuilder.build("JoinTest", local = true)
  private val tableUtils = TableUtils(spark)

  private val today = tableUtils.partitionSpec.at(System.currentTimeMillis())
  private val monthAgo = tableUtils.partitionSpec.minus(today, new Window(30, TimeUnit.DAYS))
  private val yearAgo = tableUtils.partitionSpec.minus(today, new Window(365, TimeUnit.DAYS))
  private val dayAndMonthBefore = tableUtils.partitionSpec.before(monthAgo)

  private val namespace = "test_namespace_jointest"
  private val viewsTable = s"$namespace.view_events"
  val itemQueriesTable = s"$namespace.item_queries"
  spark.sql(s"CREATE DATABASE IF NOT EXISTS $namespace")

  @Before
  def before(): Unit = {
    spark.sql(s"DROP TABLE IF EXISTS $viewsTable")
    spark.sql(s"DROP TABLE IF EXISTS $itemQueriesTable")
  }

  @Test
  def testEventsEntitiesSnapshot(): Unit = {
    val dollarTransactions = List(
      Column("user", StringType, 100),
      Column("user_name", api.StringType, 100),
      Column("ts", LongType, 200),
      Column("amount_dollars", LongType, 1000)
    )

    val rupeeTransactions = List(
      Column("user", StringType, 100),
      Column("user_name", api.StringType, 100),
      Column("ts", LongType, 200),
      Column("amount_rupees", LongType, 70000)
    )

    val dollarTable = s"$namespace.dollar_transactions"
    val rupeeTable = s"$namespace.rupee_transactions"
    spark.sql(s"DROP TABLE IF EXISTS $dollarTable")
    spark.sql(s"DROP TABLE IF EXISTS $rupeeTable")
    DataFrameGen.entities(spark, dollarTransactions, 30000, partitions = 200).save(dollarTable, Map("tblProp1" -> "1"))
    DataFrameGen.entities(spark, rupeeTransactions, 4500, partitions = 30).save(rupeeTable)

    val dollarSource = Builders.Source.entities(
      query = Builders.Query(
        selects = Builders.Selects("ts", "amount_dollars", "user_name", "user"),
        startPartition = yearAgo,
        endPartition = dayAndMonthBefore,
        setups =
          Seq("create temporary function temp_replace_right_a as 'org.apache.hadoop.hive.ql.udf.UDFRegExpReplace'")
      ),
      snapshotTable = dollarTable
    )

    //println("Rupee Source start partition $month")
    val rupeeSource =
      Builders.Source.entities(
        query = Builders.Query(
          selects = Map("ts" -> "ts", "amount_dollars" -> "CAST(amount_rupees/70 as long)", "user_name" -> "user_name", "user" -> "user"),
          startPartition = monthAgo,
          setups = Seq(
            "create temporary function temp_replace_right_b as 'org.apache.hadoop.hive.ql.udf.UDFRegExpReplace'",
            "create temporary function temp_replace_right_c as 'org.apache.hadoop.hive.ql.udf.UDFRegExpReplace'",
            "create temporary function temp_replace_right_c as 'org.apache.hadoop.hive.ql.udf.UDFRegExpReplace'"
          )
        ),
        snapshotTable = rupeeTable
      )

    val groupBy = Builders.GroupBy(
      sources = Seq(dollarSource, rupeeSource),
      keyColumns = Seq("user", "user_name"),
      aggregations = Seq(
        Builders.Aggregation(operation = Operation.SUM,
          inputColumn = "amount_dollars",
          windows = Seq(new Window(30, TimeUnit.DAYS)))),
      metaData = Builders.MetaData(name = "unit_test.user_transactions", namespace = namespace, team = "chronon")
    )
    val queriesSchema = List(
      Column("user_name", api.StringType, 100),
      Column("user", api.StringType, 100)
    )

    val queryTable = s"$namespace.queries"
    DataFrameGen
      .events(spark, queriesSchema, 3000, partitions = 180)
      .save(queryTable)

    val start = tableUtils.partitionSpec.minus(today, new Window(60, TimeUnit.DAYS))
    val end = tableUtils.partitionSpec.minus(today, new Window(30, TimeUnit.DAYS))
    val joinConf = Builders.Join(
      left = Builders.Source.events(
        query = Builders.Query(
          startPartition = start,
          setups = Seq(
            "create temporary function temp_replace_left as 'org.apache.hadoop.hive.ql.udf.UDFRegExpReplace'",
            "create temporary function temp_replace_right_c as 'org.apache.hadoop.hive.ql.udf.UDFRegExpReplace'"
          )
        ),
        table = queryTable
      ),
      joinParts = Seq(Builders.JoinPart(groupBy = groupBy, keyMapping = Map("user_name" -> "user", "user" -> "user_name"))),
      metaData = Builders.MetaData(name = "test.user_transaction_features", namespace = namespace, team = "chronon")
    )

    val runner1 = new Join(joinConf, tableUtils.partitionSpec.minus(today, new Window(40, TimeUnit.DAYS)), tableUtils)
    runner1.computeJoin()
    val dropStart = tableUtils.partitionSpec.minus(today, new Window(55, TimeUnit.DAYS))
    val dropEnd = tableUtils.partitionSpec.minus(today, new Window(45, TimeUnit.DAYS))
    tableUtils.dropPartitionRange(
      s"$namespace.test_user_transaction_features",
      dropStart,
      dropEnd
    )
    println(tableUtils.partitions(s"$namespace.test_user_transaction_features"))

    joinConf.joinParts.toScala
      .map(jp => joinConf.partOutputTable(jp))
      .foreach(tableUtils.dropPartitionRange(_, dropStart, dropEnd))

    def resetUDFs(): Unit = {
      Seq("temp_replace_left", "temp_replace_right_a", "temp_replace_right_b", "temp_replace_right_c")
        .foreach(function => tableUtils.sql(s"DROP TEMPORARY FUNCTION IF EXISTS $function"))
    }

    resetUDFs()
    val runner2 = new Join(joinConf, end, tableUtils)
    val computed = runner2.computeJoin(Some(3))
    println(s"join start = $start")

    val expectedQuery = s"""
                           |WITH
                           |   queries AS (
                           |     SELECT user_name,
                           |         user,
                           |         ts,
                           |         ds
                           |     from $queryTable
                           |     where user_name IS NOT null
                           |         AND user IS NOT NULL
                           |         AND ts IS NOT NULL
                           |         AND ds IS NOT NULL
                           |         AND ds >= '$start'
                           |         AND ds <= '$end'),
                           |   grouped_transactions AS (
                           |      SELECT user,
                           |             user_name,
                           |             ds,
                           |             SUM(IF(transactions.ts  >= (unix_timestamp(transactions.ds, 'yyyy-MM-dd') - (86400*(30-1))) * 1000, amount_dollars, null)) AS unit_test_user_transactions_amount_dollars_sum_30d,
                           |             SUM(amount_dollars) AS amount_dollars_sum
                           |      FROM
                           |         (SELECT user, user_name, ts, ds, CAST(amount_rupees/70 as long) as amount_dollars from $rupeeTable
                           |          WHERE ds >= '$monthAgo'
                           |          UNION
                           |          SELECT user, user_name, ts, ds, amount_dollars from $dollarTable
                           |          WHERE ds >= '$yearAgo' and ds <= '$dayAndMonthBefore') as transactions
                           |      WHERE unix_timestamp(ds, 'yyyy-MM-dd')*1000 + 86400*1000> ts
                           |        AND user IS NOT NULL
                           |        AND user_name IS NOT NULL
                           |        AND ds IS NOT NULL
                           |      GROUP BY user, user_name, ds)
                           | SELECT queries.user_name,
                           |        queries.user,
                           |        queries.ts,
                           |        queries.ds,
                           |        grouped_transactions.unit_test_user_transactions_amount_dollars_sum_30d
                           | FROM queries left outer join grouped_transactions
                           | ON queries.user_name = grouped_transactions.user
                           | AND queries.user = grouped_transactions.user_name
                           | AND from_unixtime(queries.ts/1000, 'yyyy-MM-dd') = date_add(grouped_transactions.ds, 1)
                           | WHERE queries.user_name IS NOT NULL AND queries.user IS NOT NULL
                           |""".stripMargin
    val expected = spark.sql(expectedQuery)
    val queries = tableUtils.sql(
      s"SELECT user_name, user, ts, ds from $queryTable where user IS NOT NULL AND user_name IS NOT null AND ts IS NOT NULL AND ds IS NOT NULL AND ds >= '$start' AND ds <= '$end'")
    val diff = Comparison.sideBySide(computed, expected, List("user_name", "user", "ts", "ds"))

    if (diff.count() > 0) {
      println(s"Actual count: ${computed.count()}")
      println(s"Expected count: ${expected.count()}")
      println(s"Diff count: ${diff.count()}")
      println(s"Queries count: ${queries.count()}")
      println(s"diff result rows")
      diff.show()
    }
    assertEquals(0, diff.count())

    // test join part table hole detection:
    // in events<>entities<>snapshot case, join part table partitions and left partitions are offset by 1
    //
    // drop only end-1 from join output table, and only end-2 from join part table. trying to trick the job into
    // thinking that end-1 is already computed for the join part, since end-1 already exists in join part table
    val endMinus1 = tableUtils.partitionSpec.minus(end, new Window(1, TimeUnit.DAYS))
    val endMinus2 = tableUtils.partitionSpec.minus(end, new Window(2, TimeUnit.DAYS))

    tableUtils.dropPartitionRange(s"$namespace.test_user_transaction_features", endMinus1, endMinus1)
    println(tableUtils.partitions(s"$namespace.test_user_transaction_features"))

    joinConf.joinParts.asScala
      .map(jp => joinConf.partOutputTable(jp))
      .foreach(tableUtils.dropPartitionRange(_, endMinus2, endMinus2))

    resetUDFs()
    val runner3 = new Join(joinConf, end, tableUtils)

    val expected2 = spark.sql(expectedQuery)
    val computed2 = runner3.computeJoin(Some(3))
    val diff2 = Comparison.sideBySide(computed2, expected2, List("user_name", "user", "ts", "ds"))

    if (diff2.count() > 0) {
      println(s"Actual count: ${computed2.count()}")
      println(s"Expected count: ${expected2.count()}")
      println(s"Diff count: ${diff2.count()}")
      println(s"Queries count: ${queries.count()}")
      println(s"diff result rows")
      diff2.show()
    }
    assertEquals(0, diff2.count())
  }

  @Test
  def testEntitiesEntities(): Unit = {
    // untimed/unwindowed entities on right
    // right side
    val weightSchema = List(
      Column("user", api.StringType, 1000),
      Column("country", api.StringType, 100),
      Column("weight", api.DoubleType, 500)
    )
    val weightTable = s"$namespace.weights"
    DataFrameGen.entities(spark, weightSchema, 1000, partitions = 400).save(weightTable)

    val weightSource = Builders.Source.entities(
      query = Builders.Query(selects = Builders.Selects("weight"),
        startPartition = yearAgo,
        endPartition = dayAndMonthBefore),
      snapshotTable = weightTable
    )

    val weightGroupBy = Builders.GroupBy(
      sources = Seq(weightSource),
      keyColumns = Seq("country"),
      aggregations = Seq(Builders.Aggregation(operation = Operation.AVERAGE, inputColumn = "weight")),
      metaData = Builders.MetaData(name = "unit_test.country_weights", namespace = namespace)
    )

    val heightSchema = List(
      Column("user", api.StringType, 1000),
      Column("country", api.StringType, 100),
      Column("height", api.LongType, 200)
    )
    val heightTable = s"$namespace.heights"
    DataFrameGen.entities(spark, heightSchema, 1000, partitions = 400).save(heightTable)
    val heightSource = Builders.Source.entities(
      query = Builders.Query(selects = Builders.Selects("height"), startPartition = monthAgo),
      snapshotTable = heightTable
    )

    val heightGroupBy = Builders.GroupBy(
      sources = Seq(heightSource),
      keyColumns = Seq("country"),
      aggregations = Seq(Builders.Aggregation(operation = Operation.AVERAGE, inputColumn = "height")),
      metaData = Builders.MetaData(name = "unit_test.country_heights", namespace = namespace)
    )

    // left side
    val countrySchema = List(Column("country", api.StringType, 100))
    val countryTable = s"$namespace.countries"
    DataFrameGen.entities(spark, countrySchema, 1000, partitions = 400).save(countryTable)

    val start = tableUtils.partitionSpec.minus(today, new Window(60, TimeUnit.DAYS))
    val end = tableUtils.partitionSpec.minus(today, new Window(15, TimeUnit.DAYS))
    val joinConf = Builders.Join(
      left = Builders.Source.entities(Builders.Query(startPartition = start), snapshotTable = countryTable),
      joinParts = Seq(Builders.JoinPart(groupBy = weightGroupBy), Builders.JoinPart(groupBy = heightGroupBy)),
      metaData = Builders.MetaData(name = "test.country_features", namespace = namespace, team = "chronon")
    )

    val runner = new Join(joinConf, end, tableUtils)
    val computed = runner.computeJoin(Some(7))
    val expected = tableUtils.sql(s"""
                                     |WITH
                                     |   countries AS (SELECT country, ds from $countryTable where ds >= '$start' and ds <= '$end'),
                                     |   grouped_weights AS (
                                     |      SELECT country,
                                     |             ds,
                                     |             avg(weight) as unit_test_country_weights_weight_average
                                     |      FROM $weightTable
                                     |      WHERE ds >= '$yearAgo' and ds <= '$dayAndMonthBefore'
                                     |      GROUP BY country, ds),
                                     |   grouped_heights AS (
                                     |      SELECT country,
                                     |             ds,
                                     |             avg(height) as unit_test_country_heights_height_average
                                     |      FROM $heightTable
                                     |      WHERE ds >= '$monthAgo'
                                     |      GROUP BY country, ds)
                                     |   SELECT countries.country,
                                     |        countries.ds,
                                     |        grouped_weights.unit_test_country_weights_weight_average,
                                     |        grouped_heights.unit_test_country_heights_height_average
                                     | FROM countries left outer join grouped_weights
                                     | ON countries.country = grouped_weights.country
                                     | AND countries.ds = grouped_weights.ds
                                     | left outer join grouped_heights
                                     | ON countries.ds = grouped_heights.ds
                                     | AND countries.country = grouped_heights.country
    """.stripMargin)

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
    /* the below testing case to cover the scenario when input table and output table
     * have same partitions, in other words, the frontfill is done, the join job
     * should not trigger a backfill and exit the program properly
     */

    // use console to redirect println message to Java IO
    val stream = new java.io.ByteArrayOutputStream()
    Console.withOut(stream) {
      // rerun the same join job
      runner.computeJoin(Some(7))
    }
    val stdOutMsg = stream.toString()
    println(s"std out message =\n $stdOutMsg")
    // make sure that the program exits with target print statements
    assertTrue(stdOutMsg.contains(s"There is no data to compute based on end partition of $end."))
  }

  @Test
  def testEntitiesEntitiesWithLag(): Unit = {
    // untimed/unwindowed entities on right
    // right side
    val weightSchema = List(
      Column("user", api.StringType, 1000),
      Column("country", api.StringType, 100),
      Column("weight", api.DoubleType, 500)
    )
    val weightTable = s"$namespace.weights"
    DataFrameGen.entities(spark, weightSchema, 1000, partitions = 400).save(weightTable)
    val lagDays = 1
    val weightSource = Builders.Source.entities(
      query = Builders.Query(selects = Builders.Selects("weight"),
        startPartition = yearAgo,
        endPartition = dayAndMonthBefore),
      snapshotTable = weightTable,
      lag = 1000 * 24 * 60 * 60 * lagDays // 1 day
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
    DataFrameGen.entities(spark, heightSchema, 1000, partitions = 400).save(heightTable)
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
    DataFrameGen.entities(spark, countrySchema, 1000, partitions = 400).save(countryTable)

    val start = tableUtils.partitionSpec.minus(today, new Window(60, TimeUnit.DAYS))
    val end = tableUtils.partitionSpec.minus(today, new Window(15, TimeUnit.DAYS))
    val joinConf = Builders.Join(
      left = Builders.Source.entities(Builders.Query(startPartition = start), snapshotTable = countryTable),
      joinParts = Seq(Builders.JoinPart(groupBy = weightGroupBy), Builders.JoinPart(groupBy = heightGroupBy)),
      metaData = Builders.MetaData(name = "test.country_features_lag_unittest", namespace = namespace, team = "chronon")
    )

    val runner = new Join(joinConf, end, tableUtils)
    val computed = runner.computeJoin(Some(7))
    val expected = tableUtils.sql(
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
         | AND countries.ds = (date_format(date_add(to_date(grouped_weights.ds, '${tableUtils.partitionSpec.format}'), $lagDays), '${tableUtils.partitionSpec.format}'))
         | left outer join grouped_heights
         | ON countries.ds = grouped_heights.ds
         | AND countries.country = grouped_heights.country
    """.stripMargin)

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
  def testEventsEventsSnapshot(): Unit = {
    val viewsSchema = List(
      Column("user", api.StringType, 10000),
      Column("item", api.StringType, 100),
      Column("time_spent_ms", api.LongType, 5000)
    )

    DataFrameGen.events(spark, viewsSchema, count = 1000, partitions = 200).drop("ts").save(viewsTable)

    val viewsSource = Builders.Source.events(
      query = Builders.Query(selects = Builders.Selects("time_spent_ms"), startPartition = yearAgo),
      table = viewsTable
    )

    val viewsGroupBy = Builders.GroupBy(
      sources = Seq(viewsSource),
      keyColumns = Seq("item"),
      aggregations = Seq(
        Builders.Aggregation(operation = Operation.AVERAGE, inputColumn = "time_spent_ms")
      ),
      metaData = Builders.MetaData(name = "unit_test.item_views", namespace = namespace),
      accuracy = Accuracy.SNAPSHOT
    )

    // left side
    val itemQueries = List(Column("item", api.StringType, 100))
    DataFrameGen
      .events(spark, itemQueries, 1000, partitions = 100)
      .save(itemQueriesTable)

    val start = tableUtils.partitionSpec.minus(today, new Window(100, TimeUnit.DAYS))

    val joinConf = Builders.Join(
      left = Builders.Source.events(Builders.Query(startPartition = start), table = itemQueriesTable),
      joinParts = Seq(Builders.JoinPart(groupBy = viewsGroupBy, prefix = "user")),
      metaData = Builders.MetaData(name = "test.item_snapshot_features_2", namespace = namespace, team = "chronon")
    )

    (new Analyzer(tableUtils, joinConf, monthAgo, today)).run()
    val join = new Join(joinConf = joinConf, endPartition = monthAgo, tableUtils)
    val computed = join.computeJoin()
    computed.show()

    val expected = tableUtils.sql(s"""
                                     |WITH
                                     |   queries AS (SELECT item, ts, ds from $itemQueriesTable where ds >= '$start' and ds <= '$monthAgo')
                                     | SELECT queries.item,
                                     |        queries.ts,
                                     |        queries.ds,
                                     |        AVG(IF(queries.ds > $viewsTable.ds, time_spent_ms, null)) as user_unit_test_item_views_time_spent_ms_average
                                     | FROM queries left outer join $viewsTable
                                     |  ON queries.item = $viewsTable.item
                                     | WHERE ($viewsTable.item IS NOT NULL) AND $viewsTable.ds >= '$yearAgo' AND $viewsTable.ds <= '$dayAndMonthBefore'
                                     | GROUP BY queries.item, queries.ts, queries.ds, from_unixtime(queries.ts/1000, 'yyyy-MM-dd')
                                     |""".stripMargin)
    expected.show()

    val diff = Comparison.sideBySide(computed, expected, List("item", "ts", "ds"))

    if (diff.count() > 0) {
      println(s"Diff count: ${diff.count()}")
      println(s"diff result rows")
      diff.show()
    }
    assertEquals(diff.count(), 0)
  }

  @Test
  def testEventsEventsSnapshotWithLag(): Unit = {
    val viewsSchema = List(
      Column("user", api.StringType, 10000),
      Column("item", api.StringType, 100),
      Column("time_spent_ms", api.LongType, 5000)
    )

    DataFrameGen.events(spark, viewsSchema, count = 1000, partitions = 200).drop("ts").save(viewsTable)

    val viewsSource = Builders.Source.events(
      query = Builders.Query(selects = Builders.Selects("time_spent_ms"), startPartition = yearAgo),
      table = viewsTable,
      lag = 1000 * 24 * 60 * 60 // 1 day
    )

    val viewsGroupBy = Builders.GroupBy(
      sources = Seq(viewsSource),
      keyColumns = Seq("item"),
      aggregations = Seq(
        Builders.Aggregation(operation = Operation.AVERAGE, inputColumn = "time_spent_ms")
      ),
      metaData = Builders.MetaData(name = "unit_test.item_views", namespace = namespace),
      accuracy = Accuracy.SNAPSHOT
    )

    // left side
    val itemQueries = List(Column("item", api.StringType, 100))
    DataFrameGen
      .events(spark, itemQueries, 1000, partitions = 100)
      .save(itemQueriesTable)

    val start = tableUtils.partitionSpec.minus(today, new Window(100, TimeUnit.DAYS))

    val joinConf = Builders.Join(
      left = Builders.Source.events(Builders.Query(startPartition = start), table = itemQueriesTable),
      joinParts = Seq(Builders.JoinPart(groupBy = viewsGroupBy, prefix = "user")),
      metaData = Builders.MetaData(name = "test.item_snapshot_features_2", namespace = namespace, team = "chronon")
    )

    (new Analyzer(tableUtils, joinConf, monthAgo, today)).run()
    val join = new Join(joinConf = joinConf, endPartition = monthAgo, tableUtils)
    val computed = join.computeJoin()
    computed.show()

    println("queries!")
    tableUtils.sql(s"select * from $itemQueriesTable").show()

    println("views!")
    tableUtils.sql(s"select * from $viewsTable").show()
    val expected = tableUtils.sql(
      s"""
         |WITH
         |   queries AS (SELECT item, ts, ds from $itemQueriesTable where ds >= '$start' and ds <= '$monthAgo')
         | SELECT queries.item,
         |        queries.ts,
         |        queries.ds,
         |        AVG(IF(queries.ds > date_format(date_add(to_date($viewsTable.ds, 'yyyy-MM-dd'), 1), 'yyyy-MM-dd'), time_spent_ms, null)) as user_unit_test_item_views_time_spent_ms_average
         | FROM queries left outer join $viewsTable
         |  ON queries.item = $viewsTable.item
         | WHERE ($viewsTable.item IS NOT NULL) AND $viewsTable.ds >= '$yearAgo' AND $viewsTable.ds <= '$dayAndMonthBefore'
         | GROUP BY queries.item, queries.ts, queries.ds, from_unixtime(queries.ts/1000, 'yyyy-MM-dd')
         |""".stripMargin)
    expected.show()
    println("expected count! " + expected.count())
    println("computed: " + computed.count())

    val diff = Comparison.sideBySide(computed, expected, List("item", "ts", "ds"))

    if (diff.count() > 0) {
      println(s"Diff count: ${diff.count()}")
      println(s"diff result rows")
      diff.show()
    }
    assertEquals(diff.count(), 0)
  }

  @Test
  def testEntitiesEventsSnapshotWithUnpartitionedTables(): Unit = {
    // This tests unpartitioned left and right with entities on the left.
    val viewsSchema = List(
      Column("user", api.StringType, 10000),
      Column("item", api.StringType, 100),
      Column("time_spent_ms", api.LongType, 5000)
    )

    DataFrameGen
      .unpartitionedEvents(spark, viewsSchema, count = 1000)
      .save(viewsTable, partitionColumns = Seq())

    val viewsSource = Builders.Source.events(
      query = Builders.Query(selects = Builders.Selects("time_spent_ms"), timeColumn = "ts", startPartition = yearAgo),
      table = viewsTable,
      isCumulative = true
    )

    val viewsGroupBy = Builders.GroupBy(
      sources = Seq(viewsSource),
      keyColumns = Seq("item"),
      aggregations = Seq(
        Builders.Aggregation(operation = Operation.AVERAGE, inputColumn = "time_spent_ms")
      ),
      metaData = Builders.MetaData(name = "unit_test.item_views", namespace = namespace),
      accuracy = Accuracy.SNAPSHOT
    )

    // left side - this side is unpartitioned.
    val itemQueries = List(Column("item", api.StringType, 100))
    DataFrameGen
      .unpartitionedEntities(spark, itemQueries, 1000)
      .distinct()
      .save(itemQueriesTable, partitionColumns = Seq())

    val joinConf = Builders.Join(
      left = Builders.Source.entities(Builders.Query(startPartition = monthAgo), snapshotTable = itemQueriesTable),
      joinParts = Seq(Builders.JoinPart(groupBy = viewsGroupBy, prefix = "user")),
      metaData = Builders.MetaData(name = "test.item_snapshot_features_2", namespace = namespace, team = "chronon")
    )

    val join = new Join(joinConf = joinConf, endPartition = monthAgo, tableUtils)
    val computed = join.computeJoin()
    computed.show()

    val expected = tableUtils.sql(
      s"""
         |WITH
         |   queries AS (SELECT item from $itemQueriesTable),
         |   views as (SELECT *, from_unixtime(ts/1000, 'yyyy-MM-dd') ds from $viewsTable)
         | SELECT queries.item,
         |        '$monthAgo' ds,
         |        AVG(time_spent_ms) as user_unit_test_item_views_time_spent_ms_average
         | FROM queries left outer join views
         |  ON queries.item = views.item
         | WHERE (views.item IS NOT NULL) AND views.ds >= '$yearAgo' AND views.ds <= '$monthAgo'
         | GROUP BY 1,2
         |""".stripMargin)
    expected.show()

    val diff = Comparison.sideBySide(computed, expected, List("item", "ds"))

    if (diff.count() > 0) {
      println(s"Diff count: ${diff.count()}")
      println(s"diff result rows")
      diff.show()
    }
    assertEquals(diff.count(), 0)
  }

  @Test
  def testEventsEventsSnapshotWithCumulativeUnpartitionedTableForRightEvents(): Unit = {
    // This tests an event / event snapshot join where the events for the groupby
    // are unpartitioned and the events for the left are partitioned.
    val viewsSchema = List(
      Column("user", api.StringType, 10000),
      Column("item", api.StringType, 100),
      Column("time_spent_ms", api.LongType, 5000)
    )

    DataFrameGen
      .unpartitionedEvents(spark, viewsSchema, count = 1000)
      .save(viewsTable, partitionColumns = Seq())

    val viewsSource = Builders.Source.events(
      query = Builders.Query(selects = Builders.Selects("time_spent_ms"), timeColumn = "ts", startPartition = yearAgo),
      table = viewsTable,
      isCumulative = true
    )

    val viewsGroupBy = Builders.GroupBy(
      sources = Seq(viewsSource),
      keyColumns = Seq("item"),
      aggregations = Seq(
        Builders.Aggregation(operation = Operation.AVERAGE, inputColumn = "time_spent_ms")
      ),
      metaData = Builders.MetaData(name = "unit_test.item_views", namespace = namespace),
      accuracy = Accuracy.SNAPSHOT
    )

    // left side - this side is partitioned.
    val itemQueries = List(Column("item", api.StringType, 100))
    DataFrameGen
      .events(spark, itemQueries, 1000, partitions = 100)
      .save(itemQueriesTable)

    val start = Constants.Partition.minus(today, new Window(100, TimeUnit.DAYS))

    val joinConf = Builders.Join(
      left = Builders.Source.events(Builders.Query(startPartition = start), table = itemQueriesTable),
      joinParts = Seq(Builders.JoinPart(groupBy = viewsGroupBy, prefix = "user")),
      metaData = Builders.MetaData(name = "test.item_snapshot_features_2", namespace = namespace, team = "chronon")
    )

    val join = new Join(joinConf = joinConf, endPartition = monthAgo, tableUtils)
    val computed = join.computeJoin()
    computed.show()

    val expected = tableUtils.sql(
      s"""
         |WITH
         |   queries AS (SELECT item, ts, ds from $itemQueriesTable where ds >= '$start' and ds <= '$monthAgo'),
         |   views as (SELECT *, from_unixtime(ts/1000, 'yyyy-MM-dd') ds from $viewsTable)
         | SELECT queries.item,
         |        queries.ts,
         |        queries.ds,
         |        AVG(IF(queries.ds > views.ds, time_spent_ms, null)) as user_unit_test_item_views_time_spent_ms_average
         | FROM queries left outer join views
         |  ON queries.item = views.item
         | WHERE (views.item IS NOT NULL) AND views.ds >= '$yearAgo' AND views.ds <= '$dayAndMonthBefore'
         | GROUP BY queries.item, queries.ts, queries.ds, from_unixtime(queries.ts/1000, 'yyyy-MM-dd')
         |""".stripMargin)
    expected.show()

    val diff = Comparison.sideBySide(computed, expected, List("item", "ts", "ds"))
    if (diff.count() > 0) {
      println(s"Diff count: ${diff.count()}")
      println(s"diff result rows")
      diff.show()
    }
    assertEquals(diff.count(), 0)
  }

  @Test
  def testEventsEventsSnapshotWithCumulativeUnpartitionedTableForLeftAndRightEvents(): Unit = {
    // This tests an event / event snapshot join where the events for the groupby
    // and left lookup events are both unpartitioned.
    val viewsSchema = List(
      Column("user", api.StringType, 10000),
      Column("item", api.StringType, 100),
      Column("time_spent_ms", api.LongType, 5000)
    )

    // Have the right side be unpartitioned by dropping the ds column.
    // This has the effect of making the right side cumulative and unpartitioned.
    DataFrameGen
      .events(spark, viewsSchema, count = 1000, partitions = 200)
      .drop("ds")
      .save(viewsTable, partitionColumns = Seq())

    val viewsSource = Builders.Source.events(
      query = Builders.Query(selects = Builders.Selects("time_spent_ms"), timeColumn = "ts", startPartition = yearAgo),
      table = viewsTable,
      isCumulative = true
    )

    val viewsGroupBy = Builders.GroupBy(
      sources = Seq(viewsSource),
      keyColumns = Seq("item"),
      aggregations = Seq(
        Builders.Aggregation(operation = Operation.AVERAGE, inputColumn = "time_spent_ms")
      ),
      metaData = Builders.MetaData(name = "unit_test.item_views", namespace = namespace),
      accuracy = Accuracy.SNAPSHOT
    )

    // left side - this side is also unpartitioned.
    val itemQueries = List(Column("item", api.StringType, 100))
    DataFrameGen
      .unpartitionedEvents(spark, itemQueries, 1000)
      .drop("ds")
      .save(itemQueriesTable, partitionColumns = Seq())

    val start = Constants.Partition.minus(today, new Window(100, TimeUnit.DAYS))

    val joinConf = Builders.Join(
      left = Builders.Source.events(Builders.Query(startPartition = start, timeColumn = "ts"), isCumulative = true, table = itemQueriesTable),
      joinParts = Seq(Builders.JoinPart(groupBy = viewsGroupBy, prefix = "user")),
      metaData = Builders.MetaData(name = "test.item_snapshot_features_2", namespace = namespace, team = "chronon")
    )

    val join = new Join(joinConf = joinConf, endPartition = monthAgo, tableUtils)
    val computed = join.computeJoin()
    computed.show()

    val expected = tableUtils.sql(
      s"""
         |WITH
         |   queries AS (SELECT item, ts, from_unixtime(ts/1000, 'yyyy-MM-dd') ds from $itemQueriesTable where from_unixtime(ts/1000, 'yyyy-MM-dd') >= '$start' and from_unixtime(ts/1000, 'yyyy-MM-dd') <= '$monthAgo'),
         |   views as (SELECT *, from_unixtime(ts/1000, 'yyyy-MM-dd') ds from $viewsTable)
         | SELECT queries.item,
         |        queries.ts,
         |        queries.ds,
         |        AVG(IF(queries.ds > views.ds, time_spent_ms, null)) as user_unit_test_item_views_time_spent_ms_average
         | FROM queries left outer join views
         |  ON queries.item = views.item
         | WHERE (views.item IS NOT NULL) AND views.ds >= '$yearAgo' AND views.ds <= '$dayAndMonthBefore'
         | GROUP BY queries.item, queries.ts, queries.ds, from_unixtime(queries.ts/1000, 'yyyy-MM-dd')
         |""".stripMargin)
    expected.show()

    val diff = Comparison.sideBySide(expected, computed, List("item", "ts", "ds"))
    if (diff.count() > 0) {
      println(s"Diff count: ${diff.count()}")
      println(s"diff result rows")
      diff.show()
    }
    assertEquals(diff.count(), 0)
  }
  @Test
  def testJoinAnalyzerSchema(): Unit = {
    val viewsSource = genTestEventSource

    val viewsGroupBy = Builders.GroupBy(
      sources = Seq(viewsSource),
      keyColumns = Seq("item"),
      aggregations = Seq(
        Builders.Aggregation(operation = Operation.AVERAGE, inputColumn = "time_spent_ms")
      ),
      metaData = Builders.MetaData(name = "join_analyzer_test.item_views", namespace = namespace),
      accuracy = Accuracy.SNAPSHOT
    )
    val anotherViewsGroupBy = Builders.GroupBy(
      sources = Seq(viewsSource),
      keyColumns = Seq("item"),
      aggregations = Seq(
        Builders.Aggregation(operation = Operation.SUM, inputColumn = "time_spent_ms")
      ),
      metaData = Builders.MetaData(name = "join_analyzer_test.new_item_views", namespace = namespace),
      accuracy = Accuracy.SNAPSHOT
    )

    // left side
    val itemQueries = List(Column("item", api.StringType, 100))
    DataFrameGen
      .events(spark, itemQueries, 1000, partitions = 100)
      .save(itemQueriesTable)

    val start = tableUtils.partitionSpec.minus(today, new Window(100, TimeUnit.DAYS))

    val joinConf = Builders.Join(
      left = Builders.Source.events(Builders.Query(startPartition = start), table = itemQueriesTable),
      joinParts = Seq(
        Builders.JoinPart(groupBy = viewsGroupBy, prefix = "prefix_one"),
        Builders.JoinPart(groupBy = anotherViewsGroupBy, prefix = "prefix_two")
      ),
      metaData =
        Builders.MetaData(name = "test_join_analyzer.item_snapshot_features", namespace = namespace, team = "chronon")
    )

    //run analyzer and validate output schema
    val analyzer = new Analyzer(tableUtils, joinConf, monthAgo, today, enableHitter = true)
    val analyzerSchema = analyzer.analyzeJoin(joinConf)._1.map { case (k, v) => s"${k} => ${v}" }.toList.sorted
    val join = new Join(joinConf = joinConf, endPartition = monthAgo, tableUtils)
    val computed = join.computeJoin()
    val expectedSchema = computed.schema.fields.map(field => s"${field.name} => ${field.dataType}").sorted
    println("=== expected schema =====")
    println(expectedSchema.mkString("\n"))
    println("=== analyzer schema =====")
    println(analyzerSchema.mkString("\n"))

    assertTrue(expectedSchema sameElements analyzerSchema)
  }
  @Test
  def testEventsEventsTemporal(): Unit = {
    val joinConf = getEventsEventsTemporal("temporal")

    val viewsTable = s"$namespace.view_temporal"
    val start = tableUtils.partitionSpec.minus(today, new Window(100, TimeUnit.DAYS))
    (new Analyzer(tableUtils, joinConf, monthAgo, today)).run()
    val join = new Join(joinConf = joinConf, endPartition = dayAndMonthBefore, tableUtils)
    val computed = join.computeJoin(Some(100))
    computed.show()
    val expected = tableUtils.sql(s"""
                                     |WITH
                                     |   queries AS (SELECT item, ts, ds from $itemQueriesTable where ds >= '$start' and ds <= '$dayAndMonthBefore')
                                     | SELECT queries.item, queries.ts, queries.ds, part.user_unit_test_item_views_ts_min, part.user_unit_test_item_views_ts_max, part.user_unit_test_item_views_time_spent_ms_average
                                     | FROM (SELECT queries.item,
                                     |        queries.ts,
                                     |        queries.ds,
                                     |        MIN(IF(queries.ts > $viewsTable.ts, $viewsTable.ts, null)) as user_unit_test_item_views_ts_min,
                                     |        MAX(IF(queries.ts > $viewsTable.ts, $viewsTable.ts, null)) as user_unit_test_item_views_ts_max,
                                     |        AVG(IF(queries.ts > $viewsTable.ts, time_spent_ms, null)) as user_unit_test_item_views_time_spent_ms_average
                                     |     FROM queries left outer join $viewsTable
                                     |     ON queries.item = $viewsTable.item
                                     |     WHERE $viewsTable.item IS NOT NULL AND $viewsTable.ds >= '$yearAgo' AND $viewsTable.ds <= '$dayAndMonthBefore'
                                     |     GROUP BY queries.item, queries.ts, queries.ds) as part
                                     | JOIN queries
                                     | ON queries.item <=> part.item AND queries.ts <=> part.ts AND queries.ds <=> part.ds
                                     |""".stripMargin)
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
  def testEventsEventsTemporalWithLag(): Unit = {
    val lag = 1000 * 24 * 60 * 60 // 1 day
    val joinConf = getEventsEventsTemporal("temporal", lag = lag)

    val viewsTable = s"$namespace.view_temporal"
    val start = tableUtils.partitionSpec.minus(today, new Window(100, TimeUnit.DAYS))
    (new Analyzer(tableUtils, joinConf, monthAgo, today)).run()
    val join = new Join(joinConf = joinConf, endPartition = dayAndMonthBefore, tableUtils)
    val computed = join.computeJoin(Some(100))
    computed.show()
    // date_format(date_add(to_date($viewsTable.ds, 'yyyy-MM-dd'), 1), 'yyyy-MM-dd')
    val expected = tableUtils.sql(
      s"""
         |WITH
         |   queries AS (SELECT item, ts, ds from $itemQueriesTable where ds >= '$start' and ds <= '$dayAndMonthBefore')
         | SELECT queries.item, queries.ts, queries.ds, part.user_unit_test_item_views_ts_min, part.user_unit_test_item_views_ts_max, part.user_unit_test_item_views_time_spent_ms_average
         | FROM (SELECT queries.item,
         |        queries.ts,
         |        queries.ds,
         |        MIN(IF(queries.ts > ($viewsTable.ts + $lag), $viewsTable.ts + $lag, null)) as user_unit_test_item_views_ts_min,
         |        MAX(IF(queries.ts > ($viewsTable.ts + $lag), $viewsTable.ts + $lag, null)) as user_unit_test_item_views_ts_max,
         |        AVG(IF(queries.ts > ($viewsTable.ts + $lag), time_spent_ms, null)) as user_unit_test_item_views_time_spent_ms_average
         |     FROM queries left outer join $viewsTable
         |     ON queries.item = $viewsTable.item
         |     WHERE $viewsTable.item IS NOT NULL AND $viewsTable.ds >= '$yearAgo' AND $viewsTable.ds <= '$dayAndMonthBefore'
         |     GROUP BY queries.item, queries.ts, queries.ds) as part
         | JOIN queries
         | ON queries.item <=> part.item AND queries.ts <=> part.ts AND queries.ds <=> part.ds
         |""".stripMargin)
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
  def testSampledEventsEventsTemporal(): Unit = {
    val joinConf = getEventsEventsTemporal("temporalSampled")

    val viewsTable = s"$namespace.view_temporalSampled"
    val numSamples = 200
    val start = tableUtils.partitionSpec.minus(today, new Window(100, TimeUnit.DAYS))
    (new Analyzer(tableUtils, joinConf, monthAgo, today)).run()
    val join = new Join(joinConf = joinConf, endPartition = dayAndMonthBefore, tableUtils)
    val computed = join.computeJoin(Some(100), maybeSampleNumRows = Some(numSamples))
    computed.show()
    assertEquals(numSamples, computed.count())

    val expected = tableUtils.sql(
      s"""
         |WITH
         |   queries AS (SELECT item, ts, ds from $itemQueriesTable where ds >= '$start' and ds <= '$dayAndMonthBefore')
         | SELECT queries.item, queries.ts, queries.ds, part.user_unit_test_item_views_ts_min, part.user_unit_test_item_views_ts_max, part.user_unit_test_item_views_time_spent_ms_average
         | FROM (SELECT queries.item,
         |        queries.ts,
         |        queries.ds,
         |        MIN(IF(queries.ts > $viewsTable.ts, $viewsTable.ts, null)) as user_unit_test_item_views_ts_min,
         |        MAX(IF(queries.ts > $viewsTable.ts, $viewsTable.ts, null)) as user_unit_test_item_views_ts_max,
         |        AVG(IF(queries.ts > $viewsTable.ts, time_spent_ms, null)) as user_unit_test_item_views_time_spent_ms_average
         |     FROM queries left outer join $viewsTable
         |     ON queries.item = $viewsTable.item
         |     WHERE $viewsTable.item IS NOT NULL AND $viewsTable.ds >= '$yearAgo' AND $viewsTable.ds <= '$dayAndMonthBefore'
         |     GROUP BY queries.item, queries.ts, queries.ds) as part
         | JOIN queries
         | ON queries.item <=> part.item AND queries.ts <=> part.ts AND queries.ds <=> part.ds
         |""".stripMargin)
    expected.show()

    // We only compare rows for the numSamples query rows present in both expected and computed,
    // by inner joining on computed.
    val computedRenamed = computed
      .withColumnRenamed("user_unit_test_item_views_ts_max", "computed_user_unit_test_item_views_ts_max")
      .withColumnRenamed("user_unit_test_item_views_ts_min", "computed_user_unit_test_item_views_ts_min")
      .withColumnRenamed("user_unit_test_item_views_time_spent_ms_average", "computed_user_unit_test_item_views_time_spent_ms_average")

    val expectedJoined = expected.join(computedRenamed, List("item", "ts", "ds"))
    val expectedFiltered = expectedJoined.drop(expectedJoined.columns.filter(_.startsWith("computed_")): _*)

    val diff = Comparison.sideBySide(computed, expectedFiltered, List("item", "ts", "ds"))
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
  def testEventsEventsCumulative(): Unit = {
    // Create a cumulative source GroupBy
    val viewsTable = s"$namespace.view_cumulative"
    val viewsGroupBy = getViewsGroupBy(suffix = "cumulative", makeCumulative = true)
    // Copy and modify existing events/events case to use cumulative GroupBy
    val joinConf = getEventsEventsTemporal("cumulative")
    joinConf.setJoinParts(Seq(Builders.JoinPart(groupBy = viewsGroupBy)).asJava)

    // Run job
    println("Item Queries DF: ")
    val q =
      s"""
         |SELECT
         |  `ts`,
         |  `ds`,
         |  `item`,
         |  time_spent_ms as `time_spent_ms`
         |FROM $viewsTable
         |WHERE
         |  ds >= '2021-06-03' AND ds <= '2021-06-03'""".stripMargin
    spark.sql(q).show()
    val start = tableUtils.partitionSpec.minus(today, new Window(100, TimeUnit.DAYS))
    val join = new Join(joinConf = joinConf, endPartition = dayAndMonthBefore, tableUtils)
    val computed = join.computeJoin(Some(100))
    computed.show()

    val expected = tableUtils.sql(s"""
                                     |WITH
                                     |   queries AS (SELECT item, ts, ds from $itemQueriesTable where ds >= '$start' and ds <= '$dayAndMonthBefore')
                                     | SELECT queries.item, queries.ts, queries.ds, part.unit_test_item_views_ts_min, part.unit_test_item_views_ts_max, part.unit_test_item_views_time_spent_ms_average
                                     | FROM (SELECT queries.item,
                                     |        queries.ts,
                                     |        queries.ds,
                                     |        MIN(IF(queries.ts > $viewsTable.ts, $viewsTable.ts, null)) as unit_test_item_views_ts_min,
                                     |        MAX(IF(queries.ts > $viewsTable.ts, $viewsTable.ts, null)) as unit_test_item_views_ts_max,
                                     |        AVG(IF(queries.ts > $viewsTable.ts, time_spent_ms, null)) as unit_test_item_views_time_spent_ms_average
                                     |     FROM queries left outer join $viewsTable
                                     |     ON queries.item = $viewsTable.item
                                     |     WHERE $viewsTable.item IS NOT NULL AND $viewsTable.ds = '$today'
                                     |     GROUP BY queries.item, queries.ts, queries.ds) as part
                                     | JOIN queries
                                     | ON queries.item <=> part.item AND queries.ts <=> part.ts AND queries.ds <=> part.ds
                                     |""".stripMargin)
    expected.show()

    val diff = Comparison.sideBySide(computed, expected, List("item", "ts", "ds"))
    val queriesBare =
      tableUtils.sql(s"SELECT item, ts, ds from $itemQueriesTable where ds >= '$start' and ds <= '$dayAndMonthBefore'")
    assertEquals(queriesBare.count(), computed.count())
    if (diff.count() > 0) {
      println(s"Diff count: ${diff.count()}")
      println(s"diff result rows")
      diff.show()
    }
    assertEquals(diff.count(), 0)
  }

  def getGroupByForIncrementalSourceTest() = {
    val messageData = Seq(
      Row("Hello", "a", "2021-01-01"),
      Row("World", "a", "2021-01-02"),
      Row("Hello!", "b", "2021-01-03")
    )

    val messageSchema: StructType = new StructType()
      .add("message", SparkStringType)
      .add("id", SparkStringType)
      .add("ds", SparkStringType)

    val rdd: RDD[Row] = spark.sparkContext.parallelize(messageData)
    val df: DataFrame = spark.createDataFrame(rdd, messageSchema)

    val table = s"$namespace.messages"

    spark.sql(s"DROP TABLE IF EXISTS $table")

    df.write.partitionBy("ds").saveAsTable(table)

    val source = Builders.Source.events(
      table = table,
      query = Builders.Query(selects = Builders.Selects("message"), startPartition = "2021-01-01")
    )

    Builders.GroupBy(
      sources = Seq(source),
      keyColumns = Seq("id"),
      aggregations = Seq(
        Builders.Aggregation(operation = Operation.MAX, inputColumn = "message")
        // Builders.Aggregation(operation = Operation.APPROX_UNIQUE_COUNT, inputColumn = "ts")
        // sql - APPROX_COUNT_DISTINCT(IF(queries.ts > $viewsTable.ts, time_spent_ms, null)) as user_ts_approx_unique_count
      ),
      metaData = Builders.MetaData(name = "unit_test.messages", namespace = namespace, team = "item_team"),
      accuracy = Accuracy.TEMPORAL
    )

  }

  @Test
  def testSourceQueryRender(): Unit = {
    // Test cumulative
    val viewsGroupByCumulative = getViewsGroupBy(suffix = "render", makeCumulative = true)
    val renderedCumulative = renderDataSourceQuery(
      viewsGroupByCumulative,
      viewsGroupByCumulative.sources.asScala.head,
      Seq("item"),
      PartitionRange("2021-02-23", "2021-05-03")(tableUtils),
      tableUtils,
      None,
      viewsGroupByCumulative.inferredAccuracy
    )
    // Only checking that the date logic is correct in the query
    assert(renderedCumulative.contains(s"(ds >= '${today}') AND (ds <= '${today}')"))

    // Test incremental
    val viewsGroupByIncremental = getGroupByForIncrementalSourceTest()
    val renderedIncremental = renderDataSourceQuery(
      viewsGroupByCumulative,
      viewsGroupByIncremental.sources.asScala.head,
      Seq("item"),
      PartitionRange("2021-01-01", "2021-01-03")(tableUtils),
      tableUtils,
      None,
      viewsGroupByCumulative.inferredAccuracy
    )
    println(renderedIncremental)
    assert(renderedIncremental.contains(s"(ds >= '2021-01-01') AND (ds <= '2021-01-03')"))

    // Test cumulative
    val viewsGroupByUnpartitioned = getViewsGroupBy(suffix = "render", makeCumulative = true, makeUnpartitioned = true)
    val renderedUnpartitioned = renderUnpartitionedDataSourceQuery(
      viewsGroupByUnpartitioned.sources.asScala.head,
      Seq("item"),
      viewsGroupByUnpartitioned.inferredAccuracy
    )
    // Only checking that the date logic is correct in the query
    assertFalse(renderedUnpartitioned.contains("ds"))
  }

  @Test
  def testNoAgg(): Unit = {
    // Left side entities, right side entities no agg
    // Also testing specific select statement (rather than select *)
    val namesSchema = List(
      Column("user", api.StringType, 1000),
      Column("name", api.StringType, 500)
    )
    val namesTable = s"$namespace.names"
    DataFrameGen.entities(spark, namesSchema, 1000, partitions = 400).save(namesTable)

    val namesSource = Builders.Source.entities(
      query =
        Builders.Query(selects = Builders.Selects("name"), startPartition = yearAgo, endPartition = dayAndMonthBefore),
      snapshotTable = namesTable
    )

    val namesGroupBy = Builders.GroupBy(
      sources = Seq(namesSource),
      keyColumns = Seq("user"),
      aggregations = null,
      metaData = Builders.MetaData(name = "unit_test.user_names", team = "chronon")
    )

    DataFrameGen
      .entities(spark, namesSchema, 1000, partitions = 400)
      .groupBy("user", "ds")
      .agg(Map("name" -> "max"))
      .save(namesTable)

    // left side
    val userSchema = List(Column("user", api.StringType, 100))
    val usersTable = s"$namespace.users"
    DataFrameGen.entities(spark, userSchema, 1000, partitions = 400).dropDuplicates().save(usersTable)

    val start = tableUtils.partitionSpec.minus(today, new Window(60, TimeUnit.DAYS))
    val end = tableUtils.partitionSpec.minus(today, new Window(15, TimeUnit.DAYS))
    val joinConf = Builders.Join(
      left = Builders.Source.entities(Builders.Query(selects = Map("user" -> "user"), startPartition = start),
        snapshotTable = usersTable),
      joinParts = Seq(Builders.JoinPart(groupBy = namesGroupBy)),
      metaData = Builders.MetaData(name = "test.user_features", namespace = namespace, team = "chronon")
    )

    val runner = new Join(joinConf, end, tableUtils)
    val computed = runner.computeJoin(Some(7))
    println(s"join start = $start")
    val expected = tableUtils.sql(s"""
                                     |WITH
                                     |   users AS (SELECT user, ds from $usersTable where ds >= '$start' and ds <= '$end'),
                                     |   grouped_names AS (
                                     |      SELECT user,
                                     |             name as unit_test_user_names_name,
                                     |             ds
                                     |      FROM $namesTable
                                     |      WHERE ds >= '$yearAgo' and ds <= '$dayAndMonthBefore')
                                     |   SELECT users.user,
                                     |        grouped_names.unit_test_user_names_name,
                                     |        users.ds
                                     | FROM users left outer join grouped_names
                                     | ON users.user = grouped_names.user
                                     | AND users.ds = grouped_names.ds
    """.stripMargin)

    println("showing join result")
    computed.show()
    println("showing query result")
    expected.show()
    println(
      s"Left side count: ${spark.sql(s"SELECT user, ds from $namesTable where ds >= '$start' and ds <= '$end'").count()}")
    println(s"Actual count: ${computed.count()}")
    println(s"Expected count: ${expected.count()}")
    val diff = Comparison.sideBySide(computed, expected, List("user", "ds"))
    if (diff.count() > 0) {
      println(s"Diff count: ${diff.count()}")
      println(s"diff result rows")
      diff.show()
    }
    assertEquals(diff.count(), 0)
  }

  @Test
  def testVersioning(): Unit = {
    val joinConf = getEventsEventsTemporal("versioning")

    // Run the old join to ensure that tables exist
    val oldJoin = new Join(joinConf = joinConf, endPartition = dayAndMonthBefore, tableUtils)
    oldJoin.computeJoin(Some(100))

    // Make sure that there is no versioning-detected changes at this phase
    val joinPartsToRecomputeNoChange = JoinUtils.tablesToRecompute(joinConf, joinConf.metaData.outputTable, tableUtils)
    assertEquals(joinPartsToRecomputeNoChange.size, 0)

    // First test changing the left side table - this should trigger a full recompute
    val leftChangeJoinConf = joinConf.deepCopy()
    leftChangeJoinConf.getLeft.getEvents.setTable("some_other_table_name")
    val leftChangeJoin = new Join(joinConf = leftChangeJoinConf, endPartition = dayAndMonthBefore, tableUtils)
    val leftChangeRecompute =
      JoinUtils.tablesToRecompute(leftChangeJoinConf, leftChangeJoinConf.metaData.outputTable, tableUtils)
    println(leftChangeRecompute)
    assertEquals(leftChangeRecompute.size, 3)
    val partTable = s"${leftChangeJoinConf.metaData.outputTable}_user_unit_test_item_views"
    assertEquals(leftChangeRecompute,
      Seq(partTable, leftChangeJoinConf.metaData.bootstrapTable, leftChangeJoinConf.metaData.outputTable))

    // Test adding a joinPart
    val addPartJoinConf = joinConf.deepCopy()
    val existingJoinPart = addPartJoinConf.getJoinParts.get(0)
    val newJoinPart = Builders.JoinPart(groupBy = getViewsGroupBy(suffix = "versioning"), prefix = "user_2")
    addPartJoinConf.setJoinParts(Seq(existingJoinPart, newJoinPart).asJava)
    val addPartJoin = new Join(joinConf = addPartJoinConf, endPartition = dayAndMonthBefore, tableUtils)
    val addPartRecompute =
      JoinUtils.tablesToRecompute(addPartJoinConf, addPartJoinConf.metaData.outputTable, tableUtils)
    assertEquals(addPartRecompute.size, 1)
    assertEquals(addPartRecompute, Seq(addPartJoinConf.metaData.outputTable))
    // Compute to ensure that it works and to set the stage for the next assertion
    addPartJoin.computeJoin(Some(100))

    // Test modifying only one of two joinParts
    val rightModJoinConf = addPartJoinConf.deepCopy()
    rightModJoinConf.getJoinParts.get(1).setPrefix("user_3")
    val rightModJoin = new Join(joinConf = rightModJoinConf, endPartition = dayAndMonthBefore, tableUtils)
    val rightModRecompute =
      JoinUtils.tablesToRecompute(rightModJoinConf, rightModJoinConf.metaData.outputTable, tableUtils)
    assertEquals(rightModRecompute.size, 2)
    val rightModPartTable = s"${addPartJoinConf.metaData.outputTable}_user_2_unit_test_item_views"
    assertEquals(rightModRecompute, Seq(rightModPartTable, addPartJoinConf.metaData.outputTable))
    // Modify both
    rightModJoinConf.getJoinParts.get(0).setPrefix("user_4")
    val rightModBothJoin = new Join(joinConf = rightModJoinConf, endPartition = dayAndMonthBefore, tableUtils)
    // Compute to ensure that it works
    val computed = rightModBothJoin.computeJoin(Some(100))

    // Now assert that the actual output is correct after all these runs
    computed.show()
    val itemQueriesTable = joinConf.getLeft.getEvents.table
    val start = joinConf.getLeft.getEvents.getQuery.getStartPartition
    val viewsTable = s"$namespace.view_versioning"

    val expected = tableUtils.sql(s"""
                                     |WITH
                                     |   queries AS (SELECT item, ts, ds from $itemQueriesTable where ds >= '$start' and ds <= '$dayAndMonthBefore')
                                     | SELECT queries.item, queries.ts, queries.ds, part.user_4_unit_test_item_views_ts_min, part.user_4_unit_test_item_views_ts_max, part.user_4_unit_test_item_views_time_spent_ms_average, part.user_3_unit_test_item_views_ts_min, part.user_3_unit_test_item_views_ts_max, part.user_3_unit_test_item_views_time_spent_ms_average
                                     | FROM (SELECT queries.item,
                                     |        queries.ts,
                                     |        queries.ds,
                                     |        MIN(IF(queries.ts > $viewsTable.ts, $viewsTable.ts, null)) as user_4_unit_test_item_views_ts_min,
                                     |        MAX(IF(queries.ts > $viewsTable.ts, $viewsTable.ts, null)) as user_4_unit_test_item_views_ts_max,
                                     |        AVG(IF(queries.ts > $viewsTable.ts, time_spent_ms, null)) as user_4_unit_test_item_views_time_spent_ms_average,
                                     |        MIN(IF(queries.ts > $viewsTable.ts, $viewsTable.ts, null)) as user_3_unit_test_item_views_ts_min,
                                     |        MAX(IF(queries.ts > $viewsTable.ts, $viewsTable.ts, null)) as user_3_unit_test_item_views_ts_max,
                                     |        AVG(IF(queries.ts > $viewsTable.ts, time_spent_ms, null)) as user_3_unit_test_item_views_time_spent_ms_average
                                     |     FROM queries left outer join $viewsTable
                                     |     ON queries.item = $viewsTable.item
                                     |     WHERE $viewsTable.ds >= '$yearAgo' AND $viewsTable.ds <= '$dayAndMonthBefore'
                                     |     GROUP BY queries.item, queries.ts, queries.ds) as part
                                     | JOIN queries
                                     | ON queries.item <=> part.item AND queries.ts <=> part.ts AND queries.ds <=> part.ds
                                     |""".stripMargin)
    expected.show()

    val diff = Comparison.sideBySide(expected, computed, List("item", "ts", "ds"))
    val queriesBare =
      tableUtils.sql(s"SELECT item, ts, ds from $itemQueriesTable where ds >= '$start' and ds <= '$dayAndMonthBefore'")
    assertEquals(queriesBare.count(), computed.count())
    if (diff.count() > 0) {
      println(s"Diff count: ${diff.count()}")
      println(s"diff result rows")
      diff
        .replaceWithReadableTime(
          Seq("ts", "a_user_3_unit_test_item_views_ts_max", "b_user_3_unit_test_item_views_ts_max"),
          dropOriginal = true)
        .show()
    }
    assertEquals(0, diff.count())
  }

  private def getViewsGroupBy(suffix: String, makeCumulative: Boolean = false, makeUnpartitioned: Boolean = false, lag: Long = 0) = {
    val viewsSchema = List(
      Column("user", api.StringType, 10000),
      Column("item", api.StringType, 100),
      Column("time_spent_ms", api.LongType, 5000)
    )

    val viewsTable = s"$namespace.view_$suffix"
    val df = DataFrameGen.events(spark, viewsSchema, count = 10000, partitions = 200)

    val viewsSource = Builders.Source.events(
      table = viewsTable,
      query = Builders.Query(selects = Builders.Selects("time_spent_ms"), startPartition = yearAgo),
      isCumulative = makeCumulative,
      lag = lag
    )

    val dfTemp = if (makeCumulative) {
      // Move all events into latest partition and set isCumulative on thrift object
      df.drop("ds").withColumn("ds", lit(today))
    } else { df }

    val dfToWrite = if (makeUnpartitioned) {
      df.drop("ds")
    } else {
      df
    }

    spark.sql(s"DROP TABLE IF EXISTS $viewsTable")
    val partitionColumns = if (makeUnpartitioned) {
      Seq()
    } else {
      Seq("ds")
    }

    dfToWrite.save(viewsTable, Map("tblProp1" -> "1"), partitionColumns = partitionColumns)

    Builders.GroupBy(
      sources = Seq(viewsSource),
      keyColumns = Seq("item"),
      aggregations = Seq(
        Builders.Aggregation(operation = Operation.AVERAGE, inputColumn = "time_spent_ms"),
        Builders.Aggregation(operation = Operation.MIN, inputColumn = "ts"),
        Builders.Aggregation(operation = Operation.MAX, inputColumn = "ts")
        // Builders.Aggregation(operation = Operation.APPROX_UNIQUE_COUNT, inputColumn = "ts")
        // sql - APPROX_COUNT_DISTINCT(IF(queries.ts > $viewsTable.ts, time_spent_ms, null)) as user_ts_approx_unique_count
      ),
      metaData = Builders.MetaData(name = "unit_test.item_views", namespace = namespace, team = "item_team"),
      accuracy = Accuracy.TEMPORAL
    )
  }

  private def getEventsEventsTemporal(nameSuffix: String = "", lag: Long = 0) = {
    // left side
    val itemQueries = List(Column("item", api.StringType, 100))
    val itemQueriesDf = DataFrameGen
      .events(spark, itemQueries, 10000, partitions = 100)
    // duplicate the events
    itemQueriesDf.union(itemQueriesDf).save(itemQueriesTable) //.union(itemQueriesDf)

    val start = tableUtils.partitionSpec.minus(today, new Window(100, TimeUnit.DAYS))
    val suffix = if (nameSuffix.isEmpty) "" else s"_$nameSuffix"
    Builders.Join(
      left = Builders.Source.events(Builders.Query(startPartition = start), table = itemQueriesTable),
      joinParts = Seq(Builders.JoinPart(groupBy = getViewsGroupBy(nameSuffix, lag = lag), prefix = "user")),
      metaData =
        Builders.MetaData(name = s"test.item_temporal_features${suffix}", namespace = namespace, team = "item_team")
    )

  }

  @Test
  def testEndPartitionJoin(): Unit = {
    val join = getEventsEventsTemporal("end_partition_test")
    val start = join.getLeft.query.startPartition
    val end = tableUtils.partitionSpec.after(start)
    val limitedJoin = Builders.Join(
      left =
        Builders.Source.events(Builders.Query(startPartition = start, endPartition = end), table = join.getLeft.table),
      joinParts = join.getJoinParts.toScala,
      metaData = join.metaData
    )
    assertTrue(end < today)
    val toCompute = new Join(limitedJoin, today, tableUtils)
    toCompute.computeJoin()
    val ds = tableUtils.sql(s"SELECT MAX(ds) FROM ${limitedJoin.metaData.outputTable}")
    assertTrue(ds.first().getString(0) < today)
  }

  @Test
  def testStructJoin(): Unit = {
    val nameSuffix = "_struct_test"
    val itemQueries = List(Column("item", api.StringType, 100))
    val itemQueriesTable = s"$namespace.item_queries_$nameSuffix"
    val itemQueriesDf = DataFrameGen
      .events(spark, itemQueries, 10000, partitions = 100)

    itemQueriesDf.save(s"${itemQueriesTable}_tmp")
    val structLeftDf = tableUtils.sql(
      s"SELECT item, NAMED_STRUCT('item_repeat', item) as item_struct, ts, ds FROM ${itemQueriesTable}_tmp")
    structLeftDf.save(itemQueriesTable)
    val start = tableUtils.partitionSpec.minus(today, new Window(100, TimeUnit.DAYS))

    val viewsSchema = List(
      Column("user", api.StringType, 10000),
      Column("item", api.StringType, 100),
      Column("time_spent_ms", api.LongType, 5000)
    )

    val viewsTable = s"$namespace.view_$nameSuffix"
    val df = DataFrameGen.events(spark, viewsSchema, count = 10000, partitions = 200)

    val viewsSource = Builders.Source.events(
      table = viewsTable,
      query = Builders.Query(selects = Builders.Selects("time_spent_ms", "item_struct"), startPartition = yearAgo)
    )
    spark.sql(s"DROP TABLE IF EXISTS $viewsTable")
    df.save(s"${viewsTable}_tmp", Map("tblProp1" -> "1"))
    val structSource =
      tableUtils.sql(s"SELECT *, NAMED_STRUCT('item_repeat', item) as item_struct FROM ${viewsTable}_tmp")
    structSource.save(viewsTable)
    val gb = Builders.GroupBy(
      sources = Seq(viewsSource),
      keyColumns = Seq("item"),
      aggregations = Seq(
        Builders.Aggregation(operation = Operation.AVERAGE, inputColumn = "time_spent_ms"),
        Builders.Aggregation(operation = Operation.MIN, inputColumn = "ts"),
        Builders.Aggregation(operation = Operation.LAST_K, argMap = Map("k" -> "1"), inputColumn = "item_struct")
        // Builders.Aggregation(operation = Operation.APPROX_UNIQUE_COUNT, inputColumn = "ts")
        // sql - APPROX_COUNT_DISTINCT(IF(queries.ts > $viewsTable.ts, time_spent_ms, null)) as user_ts_approx_unique_count
      ),
      metaData =
        Builders.MetaData(name = s"unit_test.item_views_$nameSuffix", namespace = namespace, team = "item_team"),
      accuracy = Accuracy.TEMPORAL
    )

    val join = Builders.Join(
      left = Builders.Source.events(Builders.Query(startPartition = start), table = itemQueriesTable),
      joinParts = Seq(Builders.JoinPart(groupBy = gb, prefix = "user")),
      metaData =
        Builders.MetaData(name = s"test.item_temporal_features$nameSuffix", namespace = namespace, team = "item_team")
    )
    val toCompute = new Join(join, today, tableUtils)
    toCompute.computeJoin()
    // Add stats
    new SummaryJob(spark, join, today).dailyRun(stepDays = Some(30))
  }

  def genTestEventSource(): api.Source = {
    val viewsSchema = List(
      Column("user", api.StringType, 10000),
      Column("item", api.StringType, 100),
      Column("time_spent_ms", api.LongType, 5000)
    )

    DataFrameGen.events(spark, viewsSchema, count = 1000, partitions = 200).drop("ts").save(viewsTable)

    Builders.Source.events(
      query = Builders.Query(selects = Builders.Selects("time_spent_ms"), startPartition = yearAgo),
      table = viewsTable
    )
  }

  @Test
  def testMigration(): Unit = {

    // Left
    val itemQueriesTable = s"$namespace.item_queries"
    val ds = "2023-01-01"
    val leftStart = tableUtils.partitionSpec.minus(ds, new Window(100, TimeUnit.DAYS))
    val leftSource = Builders.Source.events(Builders.Query(startPartition = leftStart), table = itemQueriesTable)

    // Right
    val viewsTable = s"$namespace.view_events"
    val viewsSource = Builders.Source.events(
      table = viewsTable,
      query = Builders.Query(selects = Builders.Selects("time_spent_ms"),
        startPartition = tableUtils.partitionSpec.minus(ds, new Window(200, TimeUnit.DAYS)))
    )
    val groupBy = Builders.GroupBy(
      sources = Seq(viewsSource),
      keyColumns = Seq("item"),
      aggregations = Seq(
        Builders.Aggregation(operation = Operation.AVERAGE, inputColumn = "time_spent_ms"),
        Builders.Aggregation(operation = Operation.MIN, inputColumn = "ts"),
        Builders.Aggregation(operation = Operation.MAX, inputColumn = "ts")
      ),
      metaData = Builders.MetaData(name = "unit_test.item_views", namespace = namespace, team = "chronon"),
      accuracy = Accuracy.TEMPORAL
    )

    // Join
    val join = Builders.Join(
      left = leftSource,
      joinParts = Seq(Builders.JoinPart(groupBy = groupBy, prefix = "user")),
      metaData = Builders.MetaData(name = s"test.join_migration", namespace = namespace, team = "chronon")
    )

    // test older versions before migration
    // older versions do not have the bootstrap hash, but should not trigger recompute if no bootstrap_parts
    val productionHashV1 = Map(
      "left_source" -> "c+n/pTnymO",
      "test_namespace_jointest.test_join_migration_user_unit_test_item_views" -> "cfi62Ap32e"
    )
    assertEquals(0, join.tablesToDrop(productionHashV1).length)

    // test newer versions
    val productionHashV2 = productionHashV1 ++ Map(
      "test_namespace_jointest.test_join_migration_bootstrap" -> "1B2M2Y8Asg"
    )
    assertEquals(0, join.tablesToDrop(productionHashV2).length)
  }
}