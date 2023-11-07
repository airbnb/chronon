package ai.chronon.spark.test

import ai.chronon.aggregator.test.Column
import ai.chronon.api
import ai.chronon.api.Constants.ChrononMetadataKey
import ai.chronon.api.Extensions.{JoinOps, MetadataOps}
import ai.chronon.api._
import ai.chronon.online.{Api, MetadataStore, SparkConversions}
import ai.chronon.spark.Extensions._
import ai.chronon.spark.TableUtils
import org.apache.spark.sql.functions.{avg, col}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import scala.collection.JavaConverters._
import scala.util.ScalaJavaConversions.JListOps
import scala.util.ScalaVersionSpecificCollectionsConverter

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

  val testPaymentsJoinName: String = "test/payments_join"
  val expectedSchemaForTestPaymentsJoin: Map[String, DataType] =
    Map(
      "unit_test_vendor_ratings_rating_average_2d_by_bucket" -> MapType(StringType, DoubleType),
      "unit_test_vendor_ratings_txn_types_histogram_3d" -> MapType(StringType, IntType),
      "unit_test_user_payments_payment_last" -> LongType,
      "unit_test_vendor_review_review_sum_2d" -> DoubleType,
      "unit_test_user_payments_ts_string_last" -> StringType,
      "unit_test_vendor_ratings_user_last300_2d" -> ListType(StringType),
      "unit_test_vendor_ratings_rating_average_30d_by_bucket" -> MapType(StringType, DoubleType),
      "unit_test_user_payments_payment_count_6h" -> LongType,
      "hist_3d" -> MapType(StringType, IntType),
      "b_unit_test_vendor_credit_credit_sum_2d" -> LongType,
      "unit_test_vendor_ratings_user_last300_30d" -> ListType(StringType),
      "unit_test_user_payments_notes_last5" -> ListType(StringType),
      "unit_test_user_payments_notes_first" -> StringType,
      "unit_test_user_payments_payment_count_14d" -> LongType,
      "unit_test_user_payments_payment_count" -> LongType,
      "payment_variance" -> DoubleType,
      "a_unit_test_vendor_credit_credit_sum_2d" -> LongType,
      "unit_test_user_payments_payment_variance" -> DoubleType,
      "unit_test_vendor_review_review_sum_30d" -> DoubleType,
      "b_unit_test_vendor_credit_credit_sum_30d" -> LongType,
      "unit_test_user_payments_ts_string_first" -> StringType,
      "unit_test_user_balance_avg_balance" -> DoubleType,
      "a_unit_test_vendor_credit_credit_sum_30d" -> LongType
    )

  val vendorRatingsGroupByName: String = "unit_test/vendor_ratings";
  val expectedSchemaForVendorRatingsGroupBy: Map[String, DataType] =
    Map("rating_average_2d_by_bucket" -> MapType(StringType, DoubleType),
      "txn_types_histogram_3d" -> MapType(StringType, IntType),
      "user_last300_2d" -> ListType(StringType),
      "rating_average_30d_by_bucket" -> MapType(StringType, DoubleType),
      "user_last300_30d" -> ListType(StringType)
    )

  def generateRandomData(spark: SparkSession, namespace: String, keyCount: Int = 100, cardinality: Int = 1000, topic: String = "test_topic"): api.Join = {
    spark.sql(s"CREATE DATABASE IF NOT EXISTS $namespace")
    val rowCount = cardinality * keyCount
    val userCol = Column("user", StringType, keyCount)
    val vendorCol = Column("vendor", StringType, keyCount)
    // temporal events
    val paymentCols = Seq(userCol, vendorCol, Column("payment", LongType, 100), Column("notes", StringType, 20))
    val paymentsTable = s"$namespace.payments_table"
    val paymentsDf = DataFrameGen.events(spark, paymentCols, rowCount, 60)
    val tsColString = "ts_string"

    paymentsDf.withTimeBasedColumn(tsColString, format = "yyyy-MM-dd HH:mm:ss").save(paymentsTable)
    val userPaymentsGroupBy = Builders.GroupBy(
      sources = Seq(Builders.Source.events(query = Builders.Query(), table = paymentsTable, topic = topic)),
      keyColumns = Seq("user"),
      aggregations = Seq(
        Builders.Aggregation(operation = Operation.COUNT,
          inputColumn = "payment",
          windows = Seq(new Window(6, TimeUnit.HOURS), new Window(14, TimeUnit.DAYS))),
        Builders.Aggregation(operation = Operation.COUNT, inputColumn = "payment"),
        Builders.Aggregation(operation = Operation.LAST, inputColumn = "payment"),
        Builders.Aggregation(operation = Operation.LAST_K, argMap = Map("k" -> "5"), inputColumn = "notes"),
        Builders.Aggregation(operation = Operation.VARIANCE, inputColumn = "payment"),
        Builders.Aggregation(operation = Operation.FIRST, inputColumn = "notes"),
        Builders.Aggregation(operation = Operation.FIRST, inputColumn = tsColString),
        Builders.Aggregation(operation = Operation.LAST, inputColumn = tsColString)
      ),
      metaData = Builders.MetaData(name = "unit_test/user_payments", namespace = namespace)
    )

    // snapshot events
    val ratingCols =
      Seq(
        userCol,
        vendorCol,
        Column("rating", IntType, 5),
        Column("bucket", StringType, 5),
        Column("sub_rating", ListType(DoubleType), 5),
        Column("txn_types", ListType(StringType), 5)
      )
    val ratingsTable = s"$namespace.ratings_table"
    DataFrameGen.events(spark, ratingCols, rowCount, 180).save(ratingsTable)
    val vendorRatingsGroupBy = Builders.GroupBy(
      sources = Seq(Builders.Source.events(query = Builders.Query(), table = ratingsTable)),
      keyColumns = Seq("vendor"),
      aggregations = Seq(
        Builders.Aggregation(operation = Operation.AVERAGE,
          inputColumn = "rating",
          windows = Seq(new Window(2, TimeUnit.DAYS), new Window(30, TimeUnit.DAYS)),
          buckets = Seq("bucket")),
        Builders.Aggregation(operation = Operation.HISTOGRAM,
          inputColumn = "txn_types",
          windows = Seq(new Window(3, TimeUnit.DAYS))),
        Builders.Aggregation(operation = Operation.LAST_K,
          argMap = Map("k" -> "300"),
          inputColumn = "user",
          windows = Seq(new Window(2, TimeUnit.DAYS), new Window(30, TimeUnit.DAYS)))
      ),
      metaData = Builders.MetaData(name = vendorRatingsGroupByName, namespace = namespace),
      accuracy = Accuracy.SNAPSHOT
    )

    // no-agg
    val userBalanceCols = Seq(userCol, Column("balance", IntType, 5000))
    val balanceTable = s"$namespace.balance_table"
    DataFrameGen
      .entities(spark, userBalanceCols, rowCount, 180)
      .groupBy("user", "ds")
      .agg(avg("balance") as "avg_balance")
      .save(balanceTable)
    val userBalanceGroupBy = Builders.GroupBy(
      sources = Seq(Builders.Source.entities(query = Builders.Query(), snapshotTable = balanceTable)),
      keyColumns = Seq("user"),
      metaData = Builders.MetaData(name = "unit_test/user_balance", namespace = namespace)
    )

    // snapshot-entities
    val userVendorCreditCols =
      Seq(Column("account", StringType, 100),
        vendorCol, // will be renamed
        Column("credit", IntType, 500),
        Column("ts", LongType, 100))
    val creditTable = s"$namespace.credit_table"
    DataFrameGen
      .entities(spark, userVendorCreditCols, rowCount, 100)
      .withColumnRenamed("vendor", "vendor_id")
      .save(creditTable)
    val creditGroupBy = Builders.GroupBy(
      sources = Seq(Builders.Source.entities(query = Builders.Query(), snapshotTable = creditTable)),
      keyColumns = Seq("vendor_id"),
      aggregations = Seq(
        Builders.Aggregation(operation = Operation.SUM,
          inputColumn = "credit",
          windows = Seq(new Window(2, TimeUnit.DAYS), new Window(30, TimeUnit.DAYS)))),
      metaData = Builders.MetaData(name = "unit_test/vendor_credit", namespace = namespace)
    )

    // temporal-entities
    val vendorReviewCols =
      Seq(Column("vendor", StringType, 10), // will be renamed
        Column("review", LongType, 10))
    val snapshotTable = s"$namespace.reviews_table_snapshot"
    val mutationTable = s"$namespace.reviews_table_mutations"
    val mutationTopic = "reviews_mutation_topic"
    val (snapshotDf, mutationsDf) =
      DataFrameGen.mutations(spark, vendorReviewCols, 10000, 35, 0.2, 1, keyColumnName = "vendor")
    snapshotDf.withColumnRenamed("vendor", "vendor_id").save(snapshotTable)
    mutationsDf.withColumnRenamed("vendor", "vendor_id").save(mutationTable)
    val tableUtils = TableUtils(spark)
    val today = tableUtils.partitionSpec.at(System.currentTimeMillis())
    val yesterday = tableUtils.partitionSpec.before(today)
    val reviewGroupBy = Builders.GroupBy(
      sources = Seq(
        Builders.Source
          .entities(
            query = Builders.Query(
              startPartition = tableUtils.partitionSpec.before(yesterday)
            ),
            snapshotTable = snapshotTable,
            mutationTable = mutationTable,
            mutationTopic = mutationTopic
          )),
      keyColumns = Seq("vendor_id"),
      aggregations = Seq(
        Builders.Aggregation(operation = Operation.SUM,
          inputColumn = "review",
          windows = Seq(new Window(2, TimeUnit.DAYS), new Window(30, TimeUnit.DAYS)))),
      metaData = Builders.MetaData(name = "unit_test/vendor_review", namespace = namespace),
      accuracy = Accuracy.TEMPORAL
    )

    // queries
    val queryCols = Seq(userCol, vendorCol)
    val queriesTable = s"$namespace.queries_table"
    val queriesDf = DataFrameGen
      .events(spark, queryCols, rowCount, 4)
      .withColumnRenamed("user", "user_id")
      .withColumnRenamed("vendor", "vendor_id")
    queriesDf.show()
    queriesDf.save(queriesTable)

    val joinConf = Builders.Join(
      left = Builders.Source.events(Builders.Query(startPartition = today), table = queriesTable),
      joinParts = Seq(
        Builders.JoinPart(groupBy = vendorRatingsGroupBy, keyMapping = Map("vendor_id" -> "vendor")),
        Builders.JoinPart(groupBy = userPaymentsGroupBy, keyMapping = Map("user_id" -> "user")),
        Builders.JoinPart(groupBy = userBalanceGroupBy, keyMapping = Map("user_id" -> "user")),
        Builders.JoinPart(groupBy = reviewGroupBy),
        Builders.JoinPart(groupBy = creditGroupBy, prefix = "b"),
        Builders.JoinPart(groupBy = creditGroupBy, prefix = "a")
      ),
      metaData = Builders.MetaData(name = testPaymentsJoinName,
        namespace = namespace,
        team = "chronon",
        consistencySamplePercent = 30),
      derivations = Seq(
        Builders.Derivation("*", "*"),
        Builders.Derivation("hist_3d", "unit_test_vendor_ratings_txn_types_histogram_3d"),
        Builders.Derivation("payment_variance", "unit_test_user_payments_payment_variance/2")
      )
    )
    joinConf
  }

  def getParentJoin(spark: SparkSession, namespace: String, name: String, gbName: String): api.Join = {
    spark.sql(s"CREATE DATABASE IF NOT EXISTS $namespace")
    val topic = "kafka://test_topic/schema=my_schema/host=X/port=Y"
    val listingCol = Column("listing", StringType, 50)
    // price for listing
    val priceCols = Seq(listingCol, Column("price", IntType, 100), Column("notes", StringType, 10))
    val priceTable = s"$namespace.price_table"
    val priceDf = DataFrameGen.events(spark, priceCols, 50, 30).filter(col("listing").isNotNull)
    val tsColString = "ts_string"

    priceDf.withTimeBasedColumn(tsColString, format = "yyyy-MM-dd HH:mm:ss").save(priceTable)
    val priceGroupBy = Builders.GroupBy(
      sources = Seq(Builders.Source.events(query = Builders.Query(), table = priceTable)),
      keyColumns = Seq("listing"),
      aggregations = Seq(
        Builders.Aggregation(operation = Operation.LAST, inputColumn = "price")
      ),
      metaData = Builders.MetaData(name = gbName, namespace = namespace),
      accuracy = Accuracy.TEMPORAL
    )

    // views table
    val userCol = Column("user", StringType, 50)
    val viewsCols = Seq(listingCol, userCol)
    val viewsTable = s"$namespace.views_table"
    val viewsDf = DataFrameGen
      .events(spark, viewsCols, 30, 7).filter(col("user").isNotNull && col("listing").isNotNull)
    viewsDf.show()
    viewsDf.save(viewsTable)

    val joinConf = Builders.Join(
      left = Builders.Source.events(
        Builders.Query(startPartition = "2023-06-01", selects = Builders.Selects("listing", "user")),
        table = viewsTable,
        topic = topic),
      joinParts = Seq(
        Builders.JoinPart(groupBy = priceGroupBy)
      ),
      metaData = Builders.MetaData(name = name, namespace = namespace, team = "chronon", consistencySamplePercent = 30)
    )
    joinConf
  }

  /**
   * Given a join configuration, this will conduct setup (including writing to KV store) so that fetcher can be called with the join.
   * @return - mock API to create fetcher
   */
  def setupFetcherWithJoin(spark: SparkSession, joinConf: api.Join, namespace: String): Api = {
    implicit val tableUtils: TableUtils = TableUtils(spark)

    val endDs:String = tableUtils.partitionSpec.at(System.currentTimeMillis())
    val kvStoreFunc = () => OnlineUtils.buildInMemoryKVStore("FetcherTest")
    val inMemoryKvStore = kvStoreFunc()
    val mockApi = new MockApi(kvStoreFunc, namespace)

    val joinedDf = new ai.chronon.spark.Join(joinConf, endDs, tableUtils).computeJoin()
    val joinTable = s"$namespace.join_test_expected_${joinConf.metaData.cleanName}"
    joinedDf.save(joinTable)

    joinConf.joinParts.asScala.foreach(jp =>
      OnlineUtils.serve(tableUtils, inMemoryKvStore, kvStoreFunc, namespace, endDs, jp.groupBy))

    val metadataStore = new MetadataStore(inMemoryKvStore, timeoutMillis = 10000)
    inMemoryKvStore.create(ChrononMetadataKey)
    metadataStore.putJoinConf(joinConf)

    mockApi
  }

  /**
    * This test group by is trying to get the price of listings a user viewed in the last 7 days. The source
    * of groupby is a Join source which computes the the last accuracy price for a given listing.
    *
    * @return a group by with a join source
    */

  def getTestGBWithJoinSource(joinSource: api.Join, query: api.Query, namespace: String, name: String): api.GroupBy = {
    Builders.GroupBy(
      sources = Seq(Builders.Source.joinSource(joinSource, query)),
      keyColumns = Seq("user"),
      aggregations = Seq(
        Builders.Aggregation(operation = Operation.LAST_K,
                             argMap = Map("k" -> "7"),
                             inputColumn = "parent_gb_price_last",
                             windows = Seq(new Window(7, TimeUnit.DAYS)))
      ),
      metaData = Builders.MetaData(name = name, namespace = namespace),
      accuracy = Accuracy.TEMPORAL
    )
  }
}
