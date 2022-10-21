package ai.chronon.spark.test

import ai.chronon.aggregator.test.Column
import ai.chronon.api
import ai.chronon.api.{Accuracy, Builders, Constants, IntType, LongType, Operation, StringType, StructField, StructType, TimeUnit, Window}
import ai.chronon.spark.Extensions._
import ai.chronon.spark._
import org.apache.spark.sql.{Row, SparkSession}
import org.junit.Test

import scala.util.ScalaVersionSpecificCollectionsConverter

class LabelJoinTest {

  val spark: SparkSession = SparkSessionBuilder.build("JoinTest", local = true)

  private val today = Constants.Partition.at(System.currentTimeMillis())
  private val monthAgo = Constants.Partition.minus(today, new Window(30, TimeUnit.DAYS))
  private val yearAgo = Constants.Partition.minus(today, new Window(365, TimeUnit.DAYS))

  private val namespace = "test_join"
  spark.sql(s"CREATE DATABASE IF NOT EXISTS $namespace")

  private val tableUtils = TableUtils(spark)

  @Test
  def testEventsEntitiesSnapshot(): Unit = {
    val dollarTransactions = List(
      Column("user", StringType, 5),
      Column("ts", LongType, 100),
      Column("amount_dollars", LongType, 20)
    )

    val dollarTable = s"$namespace.dollar_transactions"
    spark.sql(s"DROP TABLE IF EXISTS $dollarTable")
    DataFrameGen.entities(spark, dollarTransactions, 100, partitions = 60).save(dollarTable, Map("tblProp1" -> "1"))

    val dollarSource = Builders.Source.entities(
      query = Builders.Query(
        selects = Builders.Selects("ts", "amount_dollars"),
        startPartition = yearAgo,
        endPartition = today,
        setups =
          Seq("create temporary function temp_replace_right_a as 'org.apache.hadoop.hive.ql.udf.UDFRegExpReplace'")
      ),
      snapshotTable = dollarTable
    )

    val groupBy = Builders.GroupBy(
      sources = Seq(dollarSource),
      keyColumns = Seq("user"),
      aggregations = Seq(
        Builders.Aggregation(operation = Operation.SUM,
          inputColumn = "amount_dollars",
          windows = Seq(new Window(60, TimeUnit.DAYS)))),
      metaData = Builders.MetaData(name = "unit_test.user_transactions", namespace = namespace, team = "chronon")
    )
    val queriesSchema = List(
      Column("user", api.StringType,5)
    )

    val queryTable = s"$namespace.queries"
    DataFrameGen
      .events(spark, queriesSchema, 50, partitions = 20)
      .save(queryTable)

    val start = Constants.Partition.minus(today, new Window(20, TimeUnit.DAYS))
    val end = Constants.Partition.minus(today, new Window(10, TimeUnit.DAYS))
    val joinConf = Builders.Join(
      left = Builders.Source.events(
        query = Builders.Query(
          startPartition = start
        ),
        table = queryTable
      ),
      joinParts = Seq(Builders.JoinPart(groupBy = groupBy)),
      metaData = Builders.MetaData(name = "test.user_transaction_features", namespace = namespace, team = "chronon")
    )

    val runner1 = new Join(joinConf, end, tableUtils)
    val computed = runner1.computeJoin()
    println(s"join start = $start")

    println("--- computed ----")
    println(computed.count())
    computed.show(200)

    val left = spark.sql(
      s"""
         |SELECT user,
         |         ts,
         |         ds
         |     from test_label_join.queries
         |     where user IS NOT null
         |         AND ts IS NOT NULL
         |         AND ds IS NOT NULL
         |         AND ds >= '2022-10-05'
         |         AND ds <= '2022-10-15'
         |""".stripMargin)

    println(left.count())
    left.show(200)

    val right = spark.sql(
      s"""
         |SELECT *
         |     from test_label_join.dollar_transactions
         |     where user IS NOT null
         |         AND ts IS NOT NULL
         |         AND ds IS NOT NULL
         |     order by ds desc
         |""".stripMargin)

    println(right.count())
    right.show(200)
  }

  @Test
  def testLabelJoin(): Unit = {
    val namespace = "label_join"
    spark.sql(s"CREATE DATABASE IF NOT EXISTS $namespace")

    val joinTestSuite = createJoin(namespace)
    val runner = new LabelJoin(joinTestSuite.joinConf, 30,20, tableUtils)
    val computed = runner.computeLabelJoin()
    println(computed.count())
    println(computed.show(100))
  }

  def createJoin(namespace: String): JoinTestSuite = {
    val viewsGroupBy = createViewsGroupBy(namespace, spark)
    val attributesGroupBy = createAttributesGroupBy(namespace, spark)
    val joinConf = Builders.Join(
      left = viewsGroupBy.groupByConf.sources.get(0), //events
      joinParts = Seq(
        Builders.JoinPart(groupBy = attributesGroupBy.groupByConf)
      ),
      metaData = Builders.MetaData(name = "test_join", namespace = namespace, team = "chronon")
    )
    JoinTestSuite(
      joinConf,
      Seq(viewsGroupBy, attributesGroupBy),
      (
        Map("listing" -> 1L.asInstanceOf[AnyRef]),
        Map(
          "unit_test_listing_attributes_dim_bedrooms" -> 4.asInstanceOf[AnyRef],
          "unit_test_listing_attributes_dim_room_type" -> "ENTIRE_HOME"
        )
      )
    )
  }

 // event
  def createViewsGroupBy(namespace: String, spark: SparkSession): GroupByTestSuite = {
    val name = "listing_views"
    val schema = StructType(
      name,
      Array(
        StructField("listing_id", LongType),
        StructField("m_guests", LongType),
        StructField("m_views", LongType),
        StructField("ts", StringType),
        StructField("ds", StringType)
      )
    )
    val rows = List(
      Row(1L, 2L, 20L, "2022-10-01 10:00:00", "2022-10-01"),
      Row(1L, 3L, 30L, "2022-10-02 10:00:00", "2022-10-02"),
      Row(2L, 1L, 10L, "2022-10-01 10:00:00", "2022-10-01"),
      Row(2L, 2L, 20L, "2022-10-02 10:00:00", "2022-10-02")
    )
    val source = Builders.Source.events(
      query = Builders.Query(
        selects = Map(
          "listing" -> "listing_id",
          "m_guests" -> "m_guests",
          "m_views" -> "m_views"
        ),
        timeColumn = "UNIX_TIMESTAMP(ts) * 1000"
      ),
      table = s"${namespace}.${name}",
      topic = null,
      isCumulative = false
    )
    val conf = Builders.GroupBy(
      sources = Seq(source),
      keyColumns = Seq("listing"),
      aggregations = Seq(
        Builders.Aggregation(
          operation = Operation.SUM,
          inputColumn = "m_guests",
          windows = null
        ),
        Builders.Aggregation(
          operation = Operation.SUM,
          inputColumn = "m_views",
          windows = null
        )
      ),
      accuracy = Accuracy.SNAPSHOT,
      metaData = Builders.MetaData(name = s"${name}", namespace = namespace, team = "chronon")
    )
    val df = spark.createDataFrame(
      ScalaVersionSpecificCollectionsConverter.convertScalaListToJava(rows),
      Conversions.fromChrononSchema(schema)
    )
    df.save(s"${namespace}.${name}")
    GroupByTestSuite(
      name,
      conf,
      df
    )
  }

  //entity
  def createAttributesGroupBy(namespace: String, spark: SparkSession): GroupByTestSuite = {
    val name = "listing_attributes"
    val schema = StructType(
      "listing_attributes",
      Array(
        StructField("listing_id", LongType),
        StructField("dim_bedrooms", IntType),
        StructField("dim_room_type", StringType),
        StructField("ds", StringType)
      )
    )
    val rows = List(
      Row(1L, 4, "ENTIRE_HOME", "2022-10-01"),
      Row(1L, 4, "ENTIRE_HOME", "2022-10-02"),
      Row(2L, 1, "PRIVATE_ROOM", "2022-10-01"),
      Row(2L, 1, "PRIVATE_ROOM", "2022-10-02")
    )
    val source = Builders.Source.entities(
      query = Builders.Query(
        selects = Map(
          "listing" -> "listing_id",
          "dim_bedrooms" -> "dim_bedrooms",
          "dim_room_type" -> "dim_room_type"
        )
      ),
      snapshotTable = s"${namespace}.${name}"
    )
    val conf = Builders.GroupBy(
      sources = Seq(source),
      keyColumns = Seq("listing"),
      aggregations = null,
      accuracy = Accuracy.SNAPSHOT,
      metaData = Builders.MetaData(name = s"${name}", namespace = namespace, team = "chronon")
    )
    val df = spark.createDataFrame(
      ScalaVersionSpecificCollectionsConverter.convertScalaListToJava(rows),
      Conversions.fromChrononSchema(schema)
    )
    df.save(s"${namespace}.${name}")
    GroupByTestSuite(
      name,
      conf,
      df
    )
  }
}
