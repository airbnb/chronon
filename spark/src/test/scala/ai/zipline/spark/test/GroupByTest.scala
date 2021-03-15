package ai.zipline.spark.test

import ai.zipline.aggregator.base.{IntType, LongType, StringType}
import ai.zipline.aggregator.test.NaiveAggregator
import ai.zipline.aggregator.windowing.FiveMinuteResolution
import ai.zipline.api.Extensions._
import ai.zipline.api.{GroupBy => _, _}
import ai.zipline.spark.Extensions._
import ai.zipline.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{StructField, StructType, LongType => SparkLongType, StringType => SparkStringType}
import org.apache.spark.sql.{Row, SparkSession}
import org.junit.Assert._
import org.junit.{AfterClass, BeforeClass, Test}

// clean needs to be a static method
object GroupByTest {
  @BeforeClass
  @AfterClass
  def clean(): Unit = {
    SparkSessionBuilder.cleanData()
  }
}

// !!!DO NOT extend Junit.TestCase!!!
// Or the @BeforeClass and @AfterClass annotations fail to run
class GroupByTest {

  lazy val spark: SparkSession = SparkSessionBuilder.build("GroupByTest", local = true)
  private val namespace = "test_namespace_groupby_test"

  private val today = Constants.Partition.at(System.currentTimeMillis())
  private val monthAgo = Constants.Partition.minus(today, new Window(30, TimeUnit.DAYS))
  private val yearAgo = Constants.Partition.minus(today, new Window(365, TimeUnit.DAYS))
  private val dayAndMonthBefore = Constants.Partition.before(monthAgo)

  spark.sql(s"CREATE DATABASE IF NOT EXISTS $namespace")
  private val tableUtils = TableUtils(spark)

  @Test
  def testSnapshotEntities(): Unit = {
    val schema = List(
      DataGen.Column("user", StringType, 10),
      DataGen.Column(Constants.TimeColumn, LongType, 100), // ts = last 100 days
      DataGen.Column("session_length", IntType, 10000)
    )
    val df = DataGen.entities(spark, schema, 100000, 10) // ds = last 10 days
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
        |       AVG(IF(ts  >= (unix_timestamp(ds, 'yyyy-MM-dd') - (86400*10)) * 1000, session_length, null)) AS session_length_average_10d,
        |       AVG(session_length) as session_length_average
        |FROM $viewName
        |WHERE ts < unix_timestamp(ds, 'yyyy-MM-dd') * 1000 
        |GROUP BY user, ds
        |""".stripMargin)

    val diff = Comparison.sideBySide(actualDf, expectedDf, List("user", Constants.PartitionColumn))
    if (diff.count() > 0) {
      diff.show()
      println("diff result rows")
    }
    assertEquals(diff.count(), 0)
  }

  @Test
  def testSnapshotEvents(): Unit = {
    val schema = List(
      DataGen.Column("user", StringType, 10), // ts = last 10 days
      DataGen.Column("session_length", IntType, 2)
    )

    val outputDates = DataGen.genPartitions(10)

    val df = DataGen.events(spark, schema, count = 100000, partitions = 100)
    val viewName = "test_group_by_snapshot_events"
    df.createOrReplaceTempView(viewName)
    val aggregations: Seq[Aggregation] = Seq(
      Builders.Aggregation(Operation.MAX, "ts", Seq(new Window(10, TimeUnit.DAYS), WindowUtils.Unbounded)),
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
                                          |       MAX(IF(ts  >= (unix_timestamp($datesViewName.ds, 'yyyy-MM-dd') - 86400*10) * 1000, ts, null)) AS ts_max_10d,
                                          |       MAX(ts) as ts_max,
                                          |       SUM(IF(ts  >= (unix_timestamp($datesViewName.ds, 'yyyy-MM-dd') - 86400*10) * 1000, session_length, null)) AS session_length_sum_10d
                                          |FROM $viewName CROSS JOIN $datesViewName
                                          |WHERE ts < unix_timestamp($datesViewName.ds, 'yyyy-MM-dd') * 1000 
                                          |group by user, $datesViewName.ds
                                          |""".stripMargin)

    val diff = Comparison.sideBySide(actualDf, expectedDf, List("user", Constants.PartitionColumn))
    if (diff.count() > 0) {
      diff.show()
      println("diff result rows")
    }
    assertEquals(diff.count(), 0)
  }

  @Test
  def testTemporalEvents(): Unit = {
    val eventSchema = List(
      DataGen.Column("user", StringType, 10),
      DataGen.Column("session_length", IntType, 10000)
    )

    val eventDf = DataGen.events(spark, eventSchema, count = 10000, partitions = 180)

    val querySchema = List(DataGen.Column("user", StringType, 10))

    val queryDf = DataGen.events(spark, querySchema, count = 1000, partitions = 180)

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
    val queriesByKey: RDD[(KeyWithHash, Array[Long])] = queryDf.rdd
      .groupBy(keyBuilder)
      .mapValues { rowIter =>
        rowIter.map {
          _.getAs[Long](Constants.TimeColumn)
        }.toArray
      }

    val eventsByKey: RDD[(KeyWithHash, Iterator[ArrayRow])] = eventDf.rdd
      .groupBy(keyBuilder)
      .mapValues { rowIter =>
        rowIter.map(Conversions.toZiplineRow(_, groupBy.tsIndex)).toIterator
      }

    val windows = aggregations.flatMap(_.unpack.map(_.window)).toArray
    val tailHops = windows.map(FiveMinuteResolution.calculateTailHop)
    val naiveAggregator =
      new NaiveAggregator(groupBy.windowAggregator, windows, tailHops)
    val naiveRdd = queriesByKey.leftOuterJoin(eventsByKey).flatMap {
      case (key, (queries: Array[Long], events: Option[Iterator[ArrayRow]])) =>
        val irs = naiveAggregator.aggregate(events.map(_.toSeq).orNull, queries)
        queries.zip(irs).map {
          case (query: Long, ir: Array[Any]) =>
            (key.data :+ query, ir)
        }
    }
    val naiveDf = groupBy.toDataFrame(naiveRdd, Seq((Constants.TimeColumn, SparkLongType)))

    val diff = Comparison.sideBySide(naiveDf, resultDf, List("user", Constants.TimeColumn))
    if (diff.count() > 0) {
      diff.show()
      println("diff result rows")
    }
    assertEquals(diff.count(), 0)
  }

  @Test
  def testComputeGroupBy(): Unit = {
    val viewsSchema = List(
      DataGen.Column("user", StringType, 10000),
      DataGen.Column("item", StringType, 100),
      DataGen.Column("time_spent_ms", LongType, 5000)
    )

    val viewsTable = s"$namespace.view"
    val viewsDf = DataGen.events(spark, viewsSchema, count = 1000, partitions = 200)
    viewsDf.show()
    viewsDf.save(viewsTable)
    val viewsSource = Builders.Source.events(
      query = Builders.Query(selects = Builders.Selects("ts","item","time_spent_ms"), startPartition = yearAgo, wheres = Seq("item is null")),
      table = viewsTable
    )

    val viewsGroupBy = Builders.GroupBy(
      sources = Seq(viewsSource),
      keyColumns = Seq("item"),
      aggregations = Seq(
        Builders.Aggregation(operation = Operation.COUNT, inputColumn = "time_spent_ms"),
        Builders.Aggregation(operation = Operation.MIN, inputColumn = "ts"),
        Builders.Aggregation(operation = Operation.MAX, inputColumn = "ts")
      ),
      metaData = Builders.MetaData(name = "unit_test.item_views", namespace = namespace)
    )
    val endPartition = dayAndMonthBefore
    val computed = GroupBy.computeGroupBy(viewsGroupBy, endPartition = endPartition, tableUtils)
    computed.show(truncate = false)

    val expected = tableUtils.sql(
      s"""
         | SELECT ds,
         |        item,
         |        MIN(ts) as ts_min,
         |        MAX(ts) as ts_max,
         |        COUNT(time_spent_ms) as time_spent_ms_count
         | FROM  $viewsTable
         | WHERE ds >= '$yearAgo' AND ds <= '$endPartition' AND item is null
         | GROUP BY item
         |""".stripMargin)
    val diff = Comparison.sideBySide(computed, expected, List("item", Constants.PartitionColumn))

    if (diff.count() > 0) {
      println(s"Diff count: ${diff.count()}")
      println(s"diff result rows")
      diff
        .replaceWithReadableTime(Seq("a_ts_max", "b_ts_max","a_ts_min", "b_ts_min"), dropOriginal = false)
        .show()
    }
    // todo: the mismatch seems to be from the endTimes.min
    tableUtils.sql(s"select MIN(ts) from $viewsTable where item is null").show(false)
    assertEquals(0, diff.count())
  }
}
