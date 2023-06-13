package ai.chronon.spark.test

import ai.chronon.aggregator.test.Column
import ai.chronon.api
import ai.chronon.api._
import ai.chronon.online.SparkConversions
import ai.chronon.spark.Extensions._
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import scala.util.ScalaJavaConversions.JListOps

object TestUtils {
  def createViewsGroupBy(namespace: String,
                         spark: SparkSession,
                         tableName: String = "listing_views",
                         customRows: List[Row] = List.empty): GroupByTestSuite = {
    val schema = StructType(
      tableName,
      Array(
        StructField("listing_id", LongType),
        StructField("m_views", LongType),
        StructField("ts", StringType),
        StructField("ds", StringType)
      )
    )
    val rows =
      if (customRows.isEmpty)
        List(
          Row(1L, 20L, "2022-10-01 10:00:00", "2022-10-01"),
          Row(2L, 30L, "2022-10-02 10:00:00", "2022-10-02"),
          Row(3L, 10L, "2022-10-01 10:00:00", "2022-10-01"),
          Row(4L, 20L, "2022-10-02 10:00:00", "2022-10-02"),
          Row(5L, 35L, "2022-10-03 10:00:00", "2022-10-03"),
          Row(1L, 15L, "2022-10-03 10:00:00", "2022-10-03")
        )
      else customRows
    val source = Builders.Source.events(
      query = Builders.Query(
        selects = Map(
          "listing" -> "listing_id",
          "m_views" -> "m_views"
        ),
        timeColumn = "UNIX_TIMESTAMP(ts) * 1000"
      ),
      table = s"${namespace}.${tableName}",
      topic = null,
      isCumulative = false
    )
    val conf = Builders.GroupBy(
      sources = Seq(source),
      keyColumns = Seq("listing"),
      aggregations = Seq(
        Builders.Aggregation(
          operation = Operation.SUM,
          inputColumn = "m_views",
          windows = null
        )
      ),
      accuracy = Accuracy.SNAPSHOT,
      metaData = Builders.MetaData(name = s"${tableName}", namespace = namespace, team = "chronon")
    )
    spark.sql(s"DROP TABLE IF EXISTS $tableName")
    val df = makeDf(spark, schema, rows)
    df.save(s"${namespace}.${tableName}")
    GroupByTestSuite(
      tableName,
      conf,
      df
    )
  }

  def createRoomTypeGroupBy(namespace: String,
                            spark: SparkSession,
                            tableName: String = "listing_attributes_room"): GroupByTestSuite = {
    val schema = StructType(
      tableName,
      Array(
        StructField("listing_id", LongType),
        StructField("dim_room_type", StringType),
        StructField("ds", StringType)
      )
    )
    val rows = List(
      Row(1L, "ENTIRE_HOME", "2022-10-30"),
      Row(2L, "ENTIRE_HOME", "2022-10-30"),
      Row(3L, "PRIVATE_ROOM", "2022-10-30"),
      Row(4L, "PRIVATE_ROOM", "2022-10-30"),
      Row(5L, "PRIVATE_ROOM", "2022-10-30"),
      Row(1L, "ENTIRE_HOME_2", "2022-11-11"),
      Row(2L, "ENTIRE_HOME_2", "2022-11-11"),
      Row(3L, "PRIVATE_ROOM_2", "2022-11-11")
    )
    val source = Builders.Source.entities(
      query = Builders.Query(
        selects = Map(
          "listing" -> "listing_id",
          "dim_room_type" -> "dim_room_type"
        )
      ),
      snapshotTable = s"${namespace}.${tableName}"
    )
    val conf = Builders.GroupBy(
      sources = Seq(source),
      keyColumns = Seq("listing"),
      aggregations = null,
      accuracy = Accuracy.SNAPSHOT,
      metaData = Builders.MetaData(name = s"${tableName}", namespace = namespace, team = "chronon")
    )
    spark.sql(s"DROP TABLE IF EXISTS $tableName")
    val df = makeDf(spark, schema, rows)
    df.save(s"${namespace}.${tableName}")
    GroupByTestSuite(
      tableName,
      conf,
      df
    )
  }

  def createReservationGroupBy(namespace: String,
                               spark: SparkSession,
                               tableName: String = "listing_attributes_reservation"): GroupByTestSuite = {
    val schema = StructType(
      tableName,
      Array(
        StructField("listing_id", LongType),
        StructField("dim_reservations", IntType),
        StructField("ds", StringType)
      )
    )
    val rows = List(
      Row(1L, 4, "2022-10-30"),
      Row(2L, 2, "2022-10-30"),
      Row(3L, 6, "2022-10-30"),
      Row(4L, 5, "2022-10-30"),
      Row(5L, 3, "2022-10-30"),
      Row(1L, 5, "2022-11-11"),
      Row(2L, 10, "2022-11-11"),
      Row(3L, 7, "2022-11-11")
    )
    val source = Builders.Source.entities(
      query = Builders.Query(
        selects = Map(
          "listing" -> "listing_id",
          "dim_reservations" -> "dim_reservations"
        )
      ),
      snapshotTable = s"${namespace}.${tableName}"
    )
    val conf = Builders.GroupBy(
      sources = Seq(source),
      keyColumns = Seq("listing"),
      aggregations = null,
      accuracy = Accuracy.SNAPSHOT,
      metaData = Builders.MetaData(name = s"${tableName}", namespace = namespace, team = "chronon")
    )
    val df = spark.createDataFrame(
      rows.toJava,
      SparkConversions.fromChrononSchema(schema)
    )
    df.save(s"${namespace}.${tableName}")
    GroupByTestSuite(
      tableName,
      conf,
      df
    )
  }

  def createAttributesGroupByV2(namespace: String,
                                spark: SparkSession,
                                tableName: String = "listing_attributes"): GroupByTestSuite = {
    val schema = StructType(
      tableName,
      Array(
        StructField("listing_id", LongType),
        StructField("dim_bedrooms", IntType),
        StructField("dim_room_type", StringType),
        StructField("dim_host_type", StringType),
        StructField("ds", StringType)
      )
    )
    val rows = List(
      Row(1L, 4, "ENTIRE_HOME", "SUPER_HOST", "2022-11-01"),
      Row(2L, 4, "ENTIRE_HOME", "SUPER_HOST", "2022-11-01"),
      Row(3L, 1, "PRIVATE_ROOM", "NEW_HOST", "2022-11-01"),
      Row(4L, 1, "PRIVATE_ROOM", "NEW_HOST", "2022-11-01"),
      Row(5L, 1, "PRIVATE_ROOM", "SUPER_HOST", "2022-11-01"),
      Row(1L, 4, "ENTIRE_HOME_2", "SUPER_HOST_2", "2022-10-30")
    )

    val source = Builders.Source.entities(
      query = Builders.Query(
        selects = Map(
          "listing" -> "listing_id",
          "dim_bedrooms" -> "dim_bedrooms",
          "dim_room_type" -> "dim_room_type",
          "dim_host_type" -> "dim_host_type"
        )
      ),
      snapshotTable = s"${namespace}.${tableName}"
    )
    val conf = Builders.GroupBy(
      sources = Seq(source),
      keyColumns = Seq("listing"),
      aggregations = null,
      accuracy = Accuracy.SNAPSHOT,
      metaData = Builders.MetaData(name = s"${tableName}", namespace = namespace, team = "chronon")
    )
    val df = makeDf(spark, schema, rows)
    df.save(s"${namespace}.${tableName}", autoExpand = true)
    GroupByTestSuite(
      tableName,
      conf,
      df
    )
  }

  def createOrUpdateLabelGroupByWithAgg(namespace: String,
                                        spark: SparkSession,
                                        windowSize: Int,
                                        tableName: String = "listing_labels",
                                        customRows: List[Row] = List.empty): GroupByTestSuite = {
    val schema = StructType(
      tableName,
      Array(
        StructField("listing_id", LongType),
        StructField("is_active", IntType),
        StructField("ds", StringType),
        StructField("ts", StringType)
      )
    )
    val rows = if (customRows.isEmpty) {
      List(
        Row(1L, 0, "2022-10-01", "2022-10-01 10:00:00"),
        Row(2L, 0, "2022-10-01", "2022-10-01 10:00:00"),
        Row(3L, 1, "2022-10-01", "2022-10-01 10:00:00"), // not included in agg window
        Row(1L, 1, "2022-10-02", "2022-10-02 10:00:00"),
        Row(2L, 0, "2022-10-02", "2022-10-02 10:00:00"),
        Row(3L, 0, "2022-10-02", "2022-10-02 10:00:00"),
        Row(1L, 0, "2022-10-06", "2022-10-06 11:00:00"),
        Row(2L, 1, "2022-10-06", "2022-10-06 11:00:00"),
        Row(3L, 0, "2022-10-06", "2022-10-06 11:00:00"),
        Row(1L, 2, "2022-10-07", "2022-10-07 11:00:00") // not included in agg window
      )
    } else customRows
    val source = Builders.Source.events(
      query = Builders.Query(
        selects = Map(
          "listing" -> "listing_id",
          "is_active" -> "is_active"
        ),
        timeColumn = "UNIX_TIMESTAMP(ts) * 1000"
      ),
      table = s"${namespace}.${tableName}"
    )
    val conf = Builders.GroupBy(
      sources = Seq(source),
      keyColumns = Seq("listing"),
      aggregations = Seq(
        Builders.Aggregation(
          inputColumn = "is_active",
          operation = Operation.MAX,
          windows = Seq(new Window(windowSize, TimeUnit.DAYS))
        )),
      accuracy = Accuracy.SNAPSHOT,
      metaData = Builders.MetaData(name = s"${tableName}", namespace = namespace, team = "chronon")
    )
    val df = makeDf(spark, schema, rows)
    df.save(s"${namespace}.${tableName}")
    GroupByTestSuite(
      tableName,
      conf,
      df
    )
  }

  def buildListingTable(spark: SparkSession, tableName: String): String = {
    val listingSchema = List(
      Column("listing_id", api.LongType, 50),
      Column("m_views", api.LongType, 50)
    )
    DataFrameGen
      .events(spark, listingSchema, 100, partitions = 10)
      .where(col("listing_id").isNotNull and col("m_views").isNotNull)
      .dropDuplicates("listing_id", "ds")
      .save(tableName)

    tableName
  }

  def buildLabelGroupBy(namespace: String, spark: SparkSession, windowSize: Int, tableName: String): api.GroupBy = {
    val transactions = List(
      Column("listing_id", LongType, 50),
      Column("active_status", LongType, 50)
    )

    spark.sql(s"DROP TABLE IF EXISTS $tableName")
    DataFrameGen
      .events(spark, transactions, 200, partitions = 10)
      .where(col("listing_id").isNotNull)
      .save(tableName)
    val groupBySource = Builders.Source.events(
      query = Builders.Query(
        selects = Builders.Selects("listing_id", "active_status"),
        timeColumn = "ts"
      ),
      table = tableName
    )
    val groupBy = Builders.GroupBy(
      sources = Seq(groupBySource),
      keyColumns = Seq("listing_id"),
      aggregations = Seq(
        Builders.Aggregation(operation = Operation.MAX,
                             inputColumn = "active_status",
                             windows = Seq(new Window(windowSize, TimeUnit.DAYS)))),
      metaData = Builders.MetaData(name = "listing_label_table", namespace = namespace, team = "chronon")
    )

    groupBy
  }

  def createSampleLabelTableDf(spark: SparkSession, tableName: String = "listing_labels"): DataFrame = {
    val schema = StructType(
      tableName,
      Array(
        StructField("listing_id", LongType),
        StructField("room_type", StringType),
        StructField("host_type", StringType),
        StructField("ds", StringType),
        StructField("label_ds", StringType)
      )
    )
    val rows = List(
      Row(1L, "PRIVATE_ROOM", "SUPER_HOST", "2022-10-01", "2022-11-01"),
      Row(2L, "PRIVATE_ROOM", "NEW_HOST", "2022-10-02", "2022-11-01"),
      Row(3L, "ENTIRE_HOME", "SUPER_HOST", "2022-10-03", "2022-11-01"),
      Row(4L, "PRIVATE_ROOM", "SUPER_HOST", "2022-10-04", "2022-11-01"),
      Row(5L, "ENTIRE_HOME", "NEW_HOST", "2022-10-07", "2022-11-01"),
      Row(1L, "PRIVATE ROOM", "SUPER_HOST", "2022-10-07", "2022-11-22")
    )
    makeDf(spark, schema, rows)
  }

  def createSampleFeatureTableDf(spark: SparkSession, tableName: String = "listing_features"): DataFrame = {
    val schema = StructType(
      tableName,
      Array(
        StructField("listing_id", LongType),
        StructField("m_guests", LongType),
        StructField("m_views", LongType),
        StructField("ds", StringType)
      )
    )
    val rows = List(
      Row(1L, 2L, 20L, "2022-10-01"),
      Row(2L, 3L, 30L, "2022-10-01"),
      Row(3L, 1L, 10L, "2022-10-01"),
      Row(4L, 2L, 20L, "2022-10-01"),
      Row(5L, 3L, 35L, "2022-10-01"),
      Row(1L, 5L, 15L, "2022-10-07")
    )
    makeDf(spark, schema, rows)
  }

  def makeDf(spark: SparkSession, schema: StructType, rows: List[Row]): DataFrame = {
    spark.createDataFrame(
      rows.toJava,
      SparkConversions.fromChrononSchema(schema)
    )
  }
}
