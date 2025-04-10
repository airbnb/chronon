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

import org.slf4j.LoggerFactory
import ai.chronon.aggregator.test.Column
import ai.chronon.aggregator.windowing.TsUtils
import ai.chronon.api
import ai.chronon.api.Constants.ChrononMetadataKey
import ai.chronon.api.Extensions.{JoinOps, MetadataOps}
import ai.chronon.api._
import ai.chronon.online.Fetcher.{Request, Response, StatsRequest}
import ai.chronon.online.{JavaRequest, LoggableResponseBase64, MetadataStore, SparkConversions}
import ai.chronon.spark.Extensions._
import ai.chronon.spark.stats.ConsistencyJob
import ai.chronon.spark.{Join => _, _}
import com.google.gson.GsonBuilder
import junit.framework.TestCase
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.apache.spark.sql.functions.{avg, col, lit}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.junit.Assert.{assertEquals, assertTrue}

import java.lang
import java.util.TimeZone
import java.util.concurrent.Executors
import scala.collection.Seq
import scala.compat.java8.FutureConverters
import scala.concurrent.duration.{Duration, SECONDS}
import scala.concurrent.{Await, ExecutionContext}
import scala.util.Random
import scala.util.ScalaJavaConversions._

class FetcherTest extends TestCase {
  @transient lazy val logger = LoggerFactory.getLogger(getClass)
  val sessionName = "FetcherTest"
  val dummySpark: SparkSession = SparkSessionBuilder.build(sessionName, local = true)
  private val dummyTableUtils = TableUtils(dummySpark)
  private val topic = "test_topic"
  TimeZone.setDefault(TimeZone.getTimeZone("UTC"))
  private val today = dummyTableUtils.partitionSpec.at(System.currentTimeMillis())
  private val yesterday = dummyTableUtils.partitionSpec.before(today)


  /**
    * Generate deterministic data for testing and checkpointing IRs and streaming data.
    */
  def generateMutationData(namespace: String): api.Join = {
    val spark: SparkSession = SparkSessionBuilder.build(sessionName + "_" + Random.alphanumeric.take(6).mkString, local = true)
    val tableUtils = TableUtils(spark)
    tableUtils.createDatabase(namespace)
    def toTs(arg: String): Long = TsUtils.datetimeToTs(arg)
    val eventData = Seq(
      Row(595125622443733822L, toTs("2021-04-10 09:00:00"), "2021-04-10"),
      Row(595125622443733822L, toTs("2021-04-10 23:00:00"), "2021-04-10"), // Query for added event
      Row(595125622443733822L, toTs("2021-04-10 23:45:00"), "2021-04-10"), // Query for mutated event
      Row(1L, toTs("2021-04-10 00:10:00"), "2021-04-10"), // query for added event
      Row(1L, toTs("2021-04-10 03:10:00"), "2021-04-10") // query for mutated event
    )
    val snapshotData = Seq(
      Row(1L, toTs("2021-04-04 00:30:00"), 4, "2021-04-08"),
      Row(1L, toTs("2021-04-04 12:30:00"), 4, "2021-04-08"),
      Row(1L, toTs("2021-04-05 00:30:00"), 4, "2021-04-08"),
      Row(1L, toTs("2021-04-08 02:30:00"), 4, "2021-04-08"),
      Row(595125622443733822L, toTs("2021-04-04 01:40:00"), 3, "2021-04-08"),
      Row(595125622443733822L, toTs("2021-04-05 03:40:00"), 3, "2021-04-08"),
      Row(595125622443733822L, toTs("2021-04-06 03:45:00"), 4, "2021-04-08"),
      // {listing_id, ts, rating, ds}
      Row(1L, toTs("2021-04-04 00:30:00"), 4, "2021-04-09"),
      Row(1L, toTs("2021-04-04 12:30:00"), 4, "2021-04-09"),
      Row(1L, toTs("2021-04-05 00:30:00"), 4, "2021-04-09"),
      Row(1L, toTs("2021-04-08 02:30:00"), 4, "2021-04-09"),
      Row(595125622443733822L, toTs("2021-04-04 01:40:00"), 3, "2021-04-09"),
      Row(595125622443733822L, toTs("2021-04-05 03:40:00"), 3, "2021-04-09"),
      Row(595125622443733822L, toTs("2021-04-06 03:45:00"), 4, "2021-04-09"),
      Row(595125622443733822L, toTs("2021-04-09 05:45:00"), 5, "2021-04-09")
    )
    val mutationData = Seq(
      Row(595125622443733822L, toTs("2021-04-09 05:45:00"), 2, "2021-04-09", toTs("2021-04-09 05:45:00"), false),
      Row(595125622443733822L, toTs("2021-04-09 05:45:00"), 2, "2021-04-09", toTs("2021-04-09 07:00:00"), true),
      Row(595125622443733822L, toTs("2021-04-09 05:45:00"), 5, "2021-04-09", toTs("2021-04-09 07:00:00"), false),
      // {listing_id, ts, rating, ds, mutation_ts, is_before}
      Row(1L, toTs("2021-04-10 00:30:00"), 5, "2021-04-10", toTs("2021-04-10 00:30:00"), false),
      Row(1L, toTs("2021-04-10 00:30:00"), 5, "2021-04-10", toTs("2021-04-10 02:30:00"), true), // mutation delete event
      Row(595125622443733822L, toTs("2021-04-10 10:00:00"), 4, "2021-04-10", toTs("2021-04-10 10:00:00"), false),
      Row(595125622443733822L, toTs("2021-04-10 10:00:00"), 4, "2021-04-10", toTs("2021-04-10 23:30:00"), true),
      Row(595125622443733822L, toTs("2021-04-10 10:00:00"), 3, "2021-04-10", toTs("2021-04-10 23:30:00"), false)
    )
    // Schemas
    val snapshotSchema = StructType(
      "listing_ratings_snapshot_fetcher",
      Array(StructField("listing", LongType),
            StructField("ts", LongType),
            StructField("rating", IntType),
            StructField("ds", StringType))
    )

    // {..., mutation_ts (timestamp of mutation), is_before (previous value or the updated value),...}
    // Change the names to make sure mappings work properly
    val mutationSchema = StructType(
      "listing_ratings_mutations_fetcher",
      snapshotSchema.fields ++ Seq(
        StructField("mutation_time", LongType),
        StructField("is_before_reversal", BooleanType)
      )
    )

    // {..., event (generic event column), ...}
    val eventSchema = StructType("listing_events_fetcher",
                                 Array(
                                   StructField("listing_id", LongType),
                                   StructField("ts", LongType),
                                   StructField("ds", StringType)
                                 ))

    val sourceData: Map[StructType, Seq[Row]] = Map(
      eventSchema -> eventData,
      mutationSchema -> mutationData,
      snapshotSchema -> snapshotData
    )

    sourceData.foreach {
      case (schema, rows) =>
        spark
          .createDataFrame(rows.toJava, SparkConversions.fromChrononSchema(schema))
          .save(s"$namespace.${schema.name}")

    }
    logger.info("saved all data hand written for fetcher test")

    val startPartition = "2021-04-08"
    val endPartition = "2021-04-10"
    val rightSource = Builders.Source.entities(
      query = Builders.Query(
        selects = Map("listing_id" -> "listing", "ts" -> "ts", "rating" -> "CAST(rating/1.0 AS FLOAT)"),
        startPartition = startPartition,
        endPartition = endPartition,
        mutationTimeColumn = "mutation_time",
        reversalColumn = "is_before_reversal"
      ),
      snapshotTable = s"$namespace.${snapshotSchema.name}",
      mutationTable = s"$namespace.${mutationSchema.name}",
      mutationTopic = "blank"
    )

    val leftSource =
      Builders.Source.events(
        query = Builders.Query(
          selects = Builders.Selects("listing_id", "ts"),
          startPartition = startPartition
        ),
        table = s"$namespace.${eventSchema.name}"
      )

    val groupBy = Builders.GroupBy(
      sources = Seq(rightSource),
      keyColumns = Seq("listing_id"),
      aggregations = Seq(
        Builders.Aggregation(
          operation = Operation.SUM,
          inputColumn = "rating",
          windows = null
        ),
        Builders.Aggregation(
          operation = Operation.AVERAGE,
          inputColumn = "rating",
          windows = Seq(new Window(1, TimeUnit.DAYS))
        ),
        Builders.Aggregation(
          operation = Operation.SKEW,
          inputColumn = "rating",
          windows = Seq(new Window(1, TimeUnit.DAYS))
        )
      ),
      accuracy = Accuracy.TEMPORAL,
      metaData = Builders.MetaData(name = "unit_test/fetcher_mutations_gb", namespace = namespace, team = "chronon"),
      derivations=Seq(
        Builders.Derivation(name = "*", expression = "*"),
        Builders.Derivation(name = "rating_average_1d_same", expression = "rating_average_1d")
      )
    )

    val joinConf = Builders.Join(
      left = leftSource,
      joinParts = Seq(Builders.JoinPart(groupBy = groupBy)),
      metaData = Builders.MetaData(name = "unit_test/fetcher_mutations_join", namespace = namespace, team = "chronon")
    )
    joinConf
  }

  def generateRandomData(namespace: String, keyCount: Int = 10, cardinality: Int = 100): api.Join = {
    val spark: SparkSession = SparkSessionBuilder.build(sessionName + "_" + Random.alphanumeric.take(6).mkString, local = true)
    val tableUtils = TableUtils(spark)
    tableUtils.createDatabase(namespace)
    val rowCount = cardinality * keyCount
    val userCol = Column("user", StringType, keyCount)
    val vendorCol = Column("vendor", StringType, keyCount)
    // temporal events
    val paymentCols = Seq(userCol, vendorCol, Column("payment", LongType, 100), Column("notes", StringType, 20))
    val paymentsTable = s"$namespace.payments_table"
    val paymentsDf = DataFrameGen.events(spark, paymentCols, rowCount, 60)
    val tsColString = "ts_string"

    paymentsDf.withTimeBasedColumn(tsColString, format = "yyyy-MM-dd HH:mm:ss").save(paymentsTable)
    // temporal events
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
        Builders.Aggregation(operation = Operation.SKEW,
                             inputColumn = "rating",
                             windows = Seq(new Window(2, TimeUnit.DAYS), new Window(30, TimeUnit.DAYS)),
                             buckets = Seq("bucket")),
        Builders.Aggregation(operation = Operation.HISTOGRAM,
                             inputColumn = "txn_types",
                             windows = Seq(new Window(3, TimeUnit.DAYS))),
        Builders.Aggregation(operation = Operation.APPROX_HISTOGRAM_K,
                             inputColumn = "txn_types",
                             windows = Seq(new Window(3, TimeUnit.DAYS))),
        Builders.Aggregation(operation = Operation.LAST_K,
                             argMap = Map("k" -> "300"),
                             inputColumn = "user",
                             windows = Seq(new Window(2, TimeUnit.DAYS), new Window(30, TimeUnit.DAYS)))
      ),
      metaData = Builders.MetaData(name = "unit_test/vendor_ratings", namespace = namespace),
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
    val creditDerivationGroupBy = Builders.GroupBy(
      sources = Seq(Builders.Source.entities(query = Builders.Query(), snapshotTable = creditTable)),
      keyColumns = Seq("vendor_id"),
      aggregations = Seq(
        Builders.Aggregation(operation = Operation.SUM,
          inputColumn = "credit",
          windows = Seq(new Window(3, TimeUnit.DAYS)))),
      metaData = Builders.MetaData(name = "unit_test/vendor_credit_derivation", namespace = namespace),
      derivations = Seq(
        Builders.Derivation("credit_sum_3d_test_rename", "credit_sum_3d"),
        Builders.Derivation("*", "*")
      )
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
        Builders.JoinPart(groupBy = creditGroupBy, prefix = "a"),
        Builders.JoinPart(groupBy = creditDerivationGroupBy, prefix = "c")
      ),
      metaData = Builders.MetaData(name = "test/payments_join",
                                   namespace = namespace,
                                   team = "chronon",
                                   consistencySamplePercent = 30),
      derivations = Seq(
        Builders.Derivation("*", "*"),
        Builders.Derivation("hist_3d", "unit_test_vendor_ratings_txn_types_histogram_3d"),
        Builders.Derivation("payment_variance", "unit_test_user_payments_payment_variance/2"),
        Builders.Derivation("derived_ds", "from_unixtime(ts/1000, 'yyyy-MM-dd')"),
        Builders.Derivation("direct_ds", "ds")
      )
    )
    joinConf
  }

  def generateEventOnlyData(namespace: String, groupByCustomJson: Option[String] = None): api.Join = {
    val spark: SparkSession = SparkSessionBuilder.build(sessionName + "_" + Random.alphanumeric.take(6).mkString, local = true)
    val tableUtils = TableUtils(spark)
    tableUtils.createDatabase(namespace)
    def toTs(arg: String): Long = TsUtils.datetimeToTs(arg)

    val listingEventData = Seq(
      Row(1L, toTs("2021-04-10 03:10:00"), "2021-04-10"),
      Row(2L, toTs("2021-04-10 03:10:00"), "2021-04-10")
    )
    val ratingEventData = Seq(
      // 1L listing id event data
      Row(1L, toTs("2021-04-08 00:30:00"), 2, "2021-04-08"),
      Row(1L, toTs("2021-04-09 05:35:00"), 4, "2021-04-09"),
      Row(1L, toTs("2021-04-10 02:30:00"), 5, "2021-04-10"),
      Row(1L, toTs("2021-04-10 02:30:00"), 5, "2021-04-10"),
      Row(1L, toTs("2021-04-10 02:30:00"), 8, "2021-04-10"),
      Row(1L, toTs("2021-04-10 02:30:00"), 8, "2021-04-10"),
      // 2L listing id event data
      Row(2L, toTs("2021-04-06 00:30:00"), 10, "2021-04-06"), // excluded from all aggs with start partition 4/7
      Row(2L, toTs("2021-04-06 00:30:00"), 10, "2021-04-06"), // excluded from all aggs with start partition 4/7
      Row(2L, toTs("2021-04-07 00:30:00"), 10, "2021-04-07"), // excluded from avg agg
      Row(2L, toTs("2021-04-07 00:30:00"), 10, "2021-04-07"), // excluded from avg agg
      Row(2L, toTs("2021-04-08 00:30:00"), 2, "2021-04-08"),
      Row(2L, toTs("2021-04-09 05:35:00"), 4, "2021-04-09"),
      Row(2L, toTs("2021-04-10 02:30:00"), 5, "2021-04-10"),
      Row(2L, toTs("2021-04-10 02:30:00"), 5, "2021-04-10"),
      Row(2L, toTs("2021-04-10 02:30:00"), 8, "2021-04-10"),
      Row(2L, toTs("2021-04-10 02:30:00"), 8, "2021-04-10"),
      Row(2L, toTs("2021-04-07 00:30:00"), 10, "2021-04-10") // dated 4/10 but excluded from avg agg based on ts
    )
    // Schemas
    // {..., event (generic event column), ...}
    val listingsSchema = StructType("listing_events_fetcher",
                                    Array(
                                      StructField("listing_id", LongType),
                                      StructField("ts", LongType),
                                      StructField("ds", StringType)
                                    ))

    val ratingsSchema = StructType(
      "listing_ratings_fetcher",
      Array(StructField("listing_id", LongType),
            StructField("ts", LongType),
            StructField("rating", IntType),
            StructField("ds", StringType))
    )

    val sourceData: Map[StructType, Seq[Row]] = Map(
      listingsSchema -> listingEventData,
      ratingsSchema -> ratingEventData
    )

    sourceData.foreach {
      case (schema, rows) =>
        val tableName = s"$namespace.${schema.name}"

        spark.sql(s"DROP TABLE IF EXISTS $tableName")

        spark
          .createDataFrame(rows.toJava, SparkConversions.fromChrononSchema(schema))
          .save(tableName)
    }
    println("saved all data hand written for fetcher test")

    val startPartition = "2021-04-07"
    val endPartition = "2021-04-10"

    val leftSource =
      Builders.Source.events(
        query = Builders.Query(
          selects = Builders.Selects("listing_id", "ts"),
          startPartition = startPartition
        ),
        table = s"$namespace.${listingsSchema.name}"
      )

    val rightSource =
      Builders.Source.events(
        query = Builders.Query(
          selects = Builders.Selects("listing_id", "ts", "rating"),
          startPartition = startPartition
        ),
        table = s"$namespace.${ratingsSchema.name}",
        topic = "fake_topic2"
      )

    val groupBy = Builders.GroupBy(
      sources = Seq(rightSource),
      keyColumns = Seq("listing_id"),
      aggregations = Seq(
        Builders.Aggregation(
          operation = Operation.SUM,
          inputColumn = "rating",
          windows = null
        ),
        Builders.Aggregation(
          operation = Operation.AVERAGE,
          inputColumn = "rating",
          windows = Seq(new Window(2, TimeUnit.DAYS))
        ),
        Builders.Aggregation(
          operation = Operation.APPROX_HISTOGRAM_K,
          inputColumn = "rating",
          windows = Seq(new Window(1, TimeUnit.DAYS))
        )
      ),
      accuracy = Accuracy.TEMPORAL,
      metaData = Builders.MetaData(name = "unit_test/fetcher_tiled_gb",
                                   namespace = namespace,
                                   team = "chronon",
                                   customJson = groupByCustomJson.orNull)
    )

    val joinConf = Builders.Join(
      left = leftSource,
      joinParts = Seq(Builders.JoinPart(groupBy = groupBy)),
      metaData = Builders.MetaData(name = "unit_test/fetcher_tiled_join", namespace = namespace, team = "chronon")
    )
    joinConf
  }

  // Compute a join until endDs and compare the result of fetching the aggregations with the computed join values.
  def compareTemporalFetch(joinConf: api.Join,
                           endDs: String,
                           namespace: String,
                           consistencyCheck: Boolean,
                           dropDsOnWrite: Boolean): Unit = {
    implicit val executionContext: ExecutionContext = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(1))
    val spark: SparkSession = SparkSessionBuilder.build(sessionName + "_" + Random.alphanumeric.take(6).mkString, local = true)
    val tableUtils = TableUtils(spark)
    val kvStoreFunc = () => OnlineUtils.buildInMemoryKVStore("FetcherTest")
    val inMemoryKvStore = kvStoreFunc()
    val mockApi = new MockApi(kvStoreFunc, namespace)

    val joinedDf = new ai.chronon.spark.Join(joinConf, endDs, tableUtils).computeJoin()
    val joinTable = s"$namespace.join_test_expected_${joinConf.metaData.cleanName}"
    joinedDf.save(joinTable)
    val endDsExpected = tableUtils.sql(s"SELECT * FROM $joinTable WHERE ds='$endDs'")

    joinConf.joinParts.toScala.foreach(jp =>
      OnlineUtils.serve(tableUtils,
                        inMemoryKvStore,
                        kvStoreFunc,
                        namespace,
                        endDs,
                        jp.groupBy,
                        dropDsOnWrite = dropDsOnWrite))

    // Extract queries for the EndDs from the computedJoin results and eliminating computed aggregation values
    val endDsEvents = {
      tableUtils.sql(
        s"SELECT * FROM $joinTable WHERE ts >= unix_timestamp('$endDs', '${tableUtils.partitionSpec.format}')")
    }
    val endDsQueries = endDsEvents.drop(endDsEvents.schema.fieldNames.filter(_.contains("unit_test")): _*)
    val keys = joinConf.leftKeyCols
    val keyIndices = keys.map(endDsQueries.schema.fieldIndex)
    val tsIndex = endDsQueries.schema.fieldIndex(Constants.TimeColumn)
    val metadataStore = new MetadataStore(inMemoryKvStore, timeoutMillis = 10000)
    inMemoryKvStore.create(ChrononMetadataKey)
    metadataStore.putJoinConf(joinConf)

    def buildRequests(lagMs: Int = 0): Array[Request] =
      endDsQueries.rdd
        .map { row =>
          val keyMap = keyIndices.indices.map { idx =>
            keys(idx) -> row.get(keyIndices(idx)).asInstanceOf[AnyRef]
          }.toMap
          val ts = row.get(tsIndex).asInstanceOf[Long]
          Request(joinConf.metaData.nameToFilePath, keyMap, Some(ts - lagMs))
        }
        .collect()

    val requests = buildRequests()

    if (consistencyCheck) {
      val lagMs = -100000
      val laggedRequests = buildRequests(lagMs)
      val laggedResponseDf =
        FetcherTestUtil.joinResponses(spark, laggedRequests, mockApi, samplePercent = 5, logToHive = true)._2
      val correctedLaggedResponse = laggedResponseDf
        .withColumn("ts_lagged", laggedResponseDf.col("ts_millis") + lagMs)
        .withColumn("ts_millis", col("ts_lagged"))
        .drop("ts_lagged")
      logger.info("corrected lagged response")
      correctedLaggedResponse.show()
      correctedLaggedResponse.save(mockApi.logTable, partitionColumns = Seq(tableUtils.partitionColumn, "name"))

      // build flattened log table
      SchemaEvolutionUtils.runLogSchemaGroupBy(mockApi, today, today)
      val flattenerJob = new LogFlattenerJob(spark, joinConf, today, mockApi.logTable, mockApi.schemaTable)
      flattenerJob.buildLogTable()

      // build consistency metrics
      val consistencyJob = new ConsistencyJob(spark, joinConf, today)
      val metrics = consistencyJob.buildConsistencyMetrics()
      logger.info(s"ooc metrics: $metrics".stripMargin)
      OnlineUtils.serveConsistency(tableUtils, inMemoryKvStore, today, joinConf)
      val fetcher = mockApi.buildFetcher()
      val consistencyFetch =
        fetcher.fetchConsistencyMetricsTimeseries(StatsRequest(joinConf.metaData.nameToFilePath, None, None))
      val response = Await.result(consistencyFetch, Duration.Inf)
      val gson = new GsonBuilder().setPrettyPrinting().serializeNulls().create()
      logger.info(s"""
          |
          | Fetched Consistency Metrics
          | ${gson.toJson(response.values.get)}
          |""".stripMargin)
    }
    // benchmark
    FetcherTestUtil.joinResponses(spark, requests, mockApi, runCount = 10, useJavaFetcher = true)
    FetcherTestUtil.joinResponses(spark, requests, mockApi, runCount = 10)

    // comparison
    val columns = endDsExpected.schema.fields.map(_.name)
    val responseRows: Seq[Row] =
      FetcherTestUtil.joinResponses(spark, requests, mockApi, useJavaFetcher = true, debug = true)._1.map { res =>
        val all: Map[String, AnyRef] =
          res.request.keys ++
            res.values.get ++
            Map(tableUtils.partitionColumn -> today) ++
            Map(Constants.TimeColumn -> new lang.Long(res.request.atMillis.get))
        val values: Array[Any] = columns.map(all.get(_).orNull)
        SparkConversions
          .toSparkRow(values, StructType.from("record", SparkConversions.toChrononSchema(endDsExpected.schema)))
          .asInstanceOf[GenericRow]
      }

    logger.info(endDsExpected.schema.pretty)

    val keyishColumns = keys.toList ++ List(tableUtils.partitionColumn, Constants.TimeColumn)
    val responseRdd = tableUtils.sparkSession.sparkContext.parallelize(responseRows.toSeq)
    var responseDf = tableUtils.sparkSession.createDataFrame(responseRdd, endDsExpected.schema)
    if (endDs != today) {
      responseDf = responseDf.drop("ds").withColumn("ds", lit(endDs))
    }
    logger.info(s"expected schema: \n ${endDsExpected.schema.pretty} \n expected data frame:")
    endDsExpected.show()
    logger.info(s"response schema: \n ${endDsExpected.schema.pretty} \n response data frame:")
    responseDf.show()

    val diff = Comparison.sideBySide(responseDf, endDsExpected, keyishColumns, aName = "online", bName = "offline")
    assertEquals(endDsQueries.count(), responseDf.count())
    if (diff.count() > 0) {
      logger.info("queries:")
      endDsQueries.show()
      logger.info(s"Total count: ${responseDf.count()}")
      logger.info(s"Diff count: ${diff.count()}")
      logger.info(s"diff result rows:")
      diff
        .withTimeBasedColumn("ts_string", "ts", "yy-MM-dd HH:mm")
        .select("ts_string", diff.schema.fieldNames: _*)
        .show()
    }
    assertEquals(0, diff.count())
  }

  def testTemporalFetchJoinDeterministic(): Unit = {
    val namespace = "deterministic_fetch"
    val joinConf = generateMutationData(namespace)
    compareTemporalFetch(joinConf, "2021-04-10", namespace, consistencyCheck = false, dropDsOnWrite = true)
  }

  def testTemporalFetchJoinDerivation(): Unit = {
    val namespace = "derivation_fetch"
    val joinConf = generateMutationData(namespace)
    val derivations = Seq(Builders.Derivation(name = "*", expression = "*"),
      Builders.Derivation(name = "unit_test_fetcher_mutations_gb_rating_sum_plus", expression = "unit_test_fetcher_mutations_gb_rating_sum + 1"),
      Builders.Derivation(name = "listing_id_renamed", expression = "listing_id")
    )
    joinConf.setDerivations(derivations.toJava)

    compareTemporalFetch(joinConf, "2021-04-10", namespace, consistencyCheck = false, dropDsOnWrite = true)
  }

  def testTemporalFetchJoinDerivationRenameOnly(): Unit = {
    val namespace = "derivation_fetch_rename_only"
    val joinConf = generateMutationData(namespace)
    val derivations = Seq(Builders.Derivation(name = "*", expression = "*"),
      Builders.Derivation(name = "listing_id_renamed", expression = "listing_id")
    )
    joinConf.setDerivations(derivations.toJava)

    compareTemporalFetch(joinConf, "2021-04-10", namespace, consistencyCheck = false, dropDsOnWrite = true)
  }


  def testTemporalFetchJoinGenerated(): Unit = {
    val namespace = "generated_fetch"
    val joinConf = generateRandomData(namespace)
    compareTemporalFetch(joinConf,
                         dummyTableUtils.partitionSpec.at(System.currentTimeMillis()),
                         namespace,
                         consistencyCheck = true,
                         dropDsOnWrite = false)
  }

  def testTemporalTiledFetchJoinDeterministic(): Unit = {
    val namespace = "deterministic_tiled_fetch"
    val joinConf = generateEventOnlyData(namespace, groupByCustomJson = Some("{\"enable_tiling\": true}"))
    compareTemporalFetch(joinConf, "2021-04-10", namespace, consistencyCheck = false, dropDsOnWrite = true)
  }

  // test soft-fail on missing keys
  def testEmptyRequest(): Unit = {
    val spark: SparkSession = SparkSessionBuilder.build(sessionName + "_" + Random.alphanumeric.take(6).mkString, local = true)
    val namespace = "empty_request"
    val joinConf = generateRandomData(namespace, 5, 5)
    implicit val executionContext: ExecutionContext = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(1))
    val kvStoreFunc = () => OnlineUtils.buildInMemoryKVStore("FetcherTest#empty_request")
    val inMemoryKvStore = kvStoreFunc()
    val mockApi = new MockApi(kvStoreFunc, namespace)

    val metadataStore = new MetadataStore(inMemoryKvStore, timeoutMillis = 10000)
    inMemoryKvStore.create(ChrononMetadataKey)
    metadataStore.putJoinConf(joinConf)

    val request = Request(joinConf.metaData.nameToFilePath, Map.empty)
    val (responses, _) = FetcherTestUtil.joinResponses(spark, Array(request), mockApi)
    val responseMap = responses.head.values.get

    logger.info("====== Empty request response map ======")
    logger.info(responseMap.toString)
    // In this case because of empty keys, both attempts to compute derivation will fail
    val derivationExceptionTypes = Seq("derivation_fetch_exception", "derivation_rename_exception")
    assertEquals(joinConf.joinParts.size() + derivationExceptionTypes.size, responseMap.size)
    assertTrue(responseMap.keys.forall(_.endsWith("_exception")))
  }

  def testTemporalFetchGroupByNonExistKey(): Unit = {
    val namespace = "non_exist_key_group_by_fetch"
    val joinConf = generateMutationData(namespace)
    val endDs = "2021-04-10"
    val spark: SparkSession = SparkSessionBuilder.build(sessionName + "_" + Random.alphanumeric.take(6).mkString, local = true)
    val tableUtils = TableUtils(spark)
    val kvStoreFunc = () => OnlineUtils.buildInMemoryKVStore("FetcherTest")
    val inMemoryKvStore = kvStoreFunc()
    val mockApi = new MockApi(kvStoreFunc, namespace)
    @transient lazy val fetcher = mockApi.buildFetcher(debug=false)

    joinConf.joinParts.toScala.foreach(jp =>
      OnlineUtils.serve(tableUtils,
        inMemoryKvStore,
        kvStoreFunc,
        namespace,
        endDs,
        jp.groupBy,
        dropDsOnWrite = true))

    // a random key that doesn't exist
    val nonExistKey = 123L
    val request = Request("unit_test/fetcher_mutations_gb",
      Map("listing_id" -> nonExistKey.asInstanceOf[AnyRef]))
    val response = fetcher.fetchGroupBys(Seq(request))
    val result = Await.result(response, Duration(10, SECONDS))

    // result should be "null" if the key is not found
    val expected: Map[String, AnyRef] = Map("rating_average_1d_same" -> null)
    assertEquals(expected, result.head.values.get)
  }
}

object FetcherTestUtil {
  @transient lazy val logger = LoggerFactory.getLogger(getClass)
  def joinResponses(spark: SparkSession,
                    requests: Array[Request],
                    mockApi: MockApi,
                    useJavaFetcher: Boolean = false,
                    runCount: Int = 1,
                    samplePercent: Double = -1,
                    logToHive: Boolean = false,
                    debug: Boolean = false)(implicit ec: ExecutionContext): (List[Response], DataFrame) = {
    val chunkSize = 100
    @transient lazy val fetcher = mockApi.buildFetcher(debug)
    @transient lazy val javaFetcher = mockApi.buildJavaFetcher()

    def fetchOnce = {
      var latencySum: Long = 0
      var latencyCount = 0
      val blockStart = System.currentTimeMillis()
      val result = requests.iterator
        .grouped(chunkSize)
        .map { oldReqs =>
          // deliberately mis-type a few keys
          val r = oldReqs
            .map(r =>
              r.copy(keys = r.keys.mapValues { v =>
                if (v.isInstanceOf[java.lang.Long]) v.toString else v
              }.toMap))
          val responses = if (useJavaFetcher) {
            // Converting to java request and using the toScalaRequest functionality to test conversion
            val convertedJavaRequests = r.map(new JavaRequest(_)).toJava
            val javaResponse = javaFetcher.fetchJoin(convertedJavaRequests)
            FutureConverters
              .toScala(javaResponse)
              .map(
                _.toScala.map(jres =>
                  Response(
                    Request(jres.request.name, jres.request.keys.toScala.toMap, Option(jres.request.atMillis)),
                    jres.values.toScala.map(_.toScala)
                  )))
          } else {
            fetcher.fetchJoin(r)
          }

          // fix mis-typed keys in the request
          val fixedResponses =
            responses.map(resps => resps.zip(oldReqs).map { case (resp, req) => resp.copy(request = req) })
          System.currentTimeMillis() -> fixedResponses
        }
        .flatMap {
          case (start, future) =>
            val result = Await.result(future, Duration(10000, SECONDS)) // todo: change back to millis
            val latency = System.currentTimeMillis() - start
            latencySum += latency
            latencyCount += 1
            result
        }
        .toList
      val latencyMillis = latencySum.toFloat / latencyCount.toFloat
      val qps = (requests.length * 1000.0) / (System.currentTimeMillis() - blockStart).toFloat
      (latencyMillis, qps, result)
    }

    // to overwhelm the profiler with fetching code path
    // so as to make it prominent in the flamegraph & collect enough stats

    var latencySum = 0.0
    var qpsSum = 0.0
    var loggedValues: Seq[LoggableResponseBase64] = null
    var result: List[Response] = null
    (0 until runCount).foreach { _ =>
      val (latency, qps, resultVal) = fetchOnce
      result = resultVal
      loggedValues = mockApi.flushLoggedValues
      latencySum += latency
      qpsSum += qps
    }
    val fetcherNameString = if (useJavaFetcher) "Java" else "Scala"

    logger.info(s"""
         |Averaging fetching stats for $fetcherNameString Fetcher over ${requests.length} requests $runCount times
         |with batch size: $chunkSize
         |average qps: ${qpsSum / runCount}
         |average latency: ${latencySum / runCount}
         |""".stripMargin)
    val loggedDf = mockApi.loggedValuesToDf(loggedValues, spark)
    if (logToHive) {
      TableUtils(spark).insertPartitions(
        loggedDf,
        mockApi.logTable,
        partitionColumns = Seq("ds", "name")
      )
    }
    if (samplePercent > 0) {
      logger.info(s"logged count: ${loggedDf.count()}")
      loggedDf.show()
    }
    result -> loggedDf
  }
}
