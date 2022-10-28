package ai.chronon.spark.test

import ai.chronon.api.{Accuracy, Builders, IntType, LongType, Operation, StringType, StructField, StructType}
import ai.chronon.spark.Conversions
import ai.chronon.spark.Extensions._
import org.apache.spark.sql.{Row, SparkSession}

import scala.util.ScalaVersionSpecificCollectionsConverter

object TestUtils {
  def createViewsGroupBy(namespace: String,
                         spark: SparkSession,
                         tableName: String = "listing_views",
                         rowsOverride: List[Row] = List.empty): GroupByTestSuite = {
    val schema = StructType(
      tableName,
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
      Row(2L, 2L, 20L, "2022-10-02 10:00:00", "2022-10-02"),
      Row(1L, 3L, 35L, "2022-10-03 10:00:00", "2022-10-03"),
      Row(3L, 5L, 15L, "2022-10-03 10:00:00", "2022-10-03")
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
      metaData = Builders.MetaData(name = s"${tableName}", namespace = namespace, team = "chronon")
    )
    val df = spark.createDataFrame(
      ScalaVersionSpecificCollectionsConverter.convertScalaListToJava(rows),
      Conversions.fromChrononSchema(schema)
    )
    df.save(s"${namespace}.${tableName}")
    GroupByTestSuite(
      tableName,
      conf,
      df
    )
  }

  def createAttributesGroupBy(namespace: String,
                              spark: SparkSession,
                              tableName: String = "listing_attributes",
                              rowsOverride: List[Row] = List.empty): GroupByTestSuite = {
    val schema = StructType(
      tableName,
      Array(
        StructField("listing_id", LongType),
        StructField("dim_bedrooms", IntType),
        StructField("dim_room_type", StringType),
        StructField("ds", StringType)
      )
    )
    val rows = List(
      Row(1L, 4, "ENTIRE_HOME", "2022-10-01"),
      Row(1L, 4, "ENTIRE_HOME_2", "2022-10-02"),
      Row(2L, 1, "PRIVATE_ROOM", "2022-10-01"),
      Row(2L, 1, "PRIVATE_ROOM_2", "2022-10-02"),
      Row(3L, 1, "PRIVATE_ROOM_3", "2022-10-03"),
      Row(1L, 4, "ENTIRE_HOME_3", "2022-10-03")
    )
    val source = Builders.Source.entities(
      query = Builders.Query(
        selects = Map(
          "listing" -> "listing_id",
          "dim_bedrooms" -> "dim_bedrooms",
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
    val df = spark.createDataFrame(
      ScalaVersionSpecificCollectionsConverter.convertScalaListToJava(rows),
      Conversions.fromChrononSchema(schema)
    )
    df.save(s"${namespace}.${tableName}")
    GroupByTestSuite(
      tableName,
      conf,
      df
    )
  }
}
