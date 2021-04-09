package ai.zipline.spark.test

import ai.zipline.aggregator.base.{IntType, LongType, StringType}
import ai.zipline.aggregator.test.{CStream, Column, RowsWithSchema}
import ai.zipline.api.{Builders, Constants, Operation, TimeUnit, Window}
import ai.zipline.spark.{GroupBy, PartitionRange, SparkSessionBuilder, TableUtils}
import org.apache.spark.sql.SparkSession
import ai.zipline.spark.Extensions._
import junit.framework.TestCase

class FetcherTest extends TestCase {
  val spark: SparkSession = SparkSessionBuilder.build("FetcherTest", local = true)

  def testTemporalFetch: Unit = {

    val paymentCols =
      Seq(Column("user", StringType, 100), Column("vendor", StringType, 10), Column("payment", LongType, 100))

    val ratingCols =
      Seq(Column("user", StringType, 100), Column("vendor", StringType, 10), Column("rating", IntType, 5))

    val paymentsTable = "payments_table"
    DataFrameGen.events(spark, paymentCols, 100000, 60).save(paymentsTable)
    val ratingsTable = "ratings_table"
    DataFrameGen.events(spark, ratingCols, 100000, 180).save(ratingsTable)

    val queryCols =
      Seq(Column("user_id", StringType, 100), Column("vendor_id", StringType, 10), Column("ts", LongType, 1))

    val vendorRatingsGroupBy = Builders.GroupBy(
      sources = Seq(Builders.Source.events(query = Builders.Query(), table = ratingsTable)),
      keyColumns = Seq("vendor"),
      aggregations = Seq(
        Builders.Aggregation(operation = Operation.AVERAGE,
                             inputColumn = "rating",
                             windows = Seq(new Window(2, TimeUnit.DAYS), new Window(30, TimeUnit.DAYS)))),
      metaData = Builders.MetaData(name = "unit_test.vendor_ratings")
    )

    val userPaymentsGroupBy = Builders.GroupBy(
      sources = Seq(Builders.Source.events(query = Builders.Query(), table = paymentsTable)),
      keyColumns = Seq("user"),
      aggregations = Seq(
        Builders.Aggregation(operation = Operation.APPROX_UNIQUE_COUNT,
                             inputColumn = "payment",
                             windows = Seq(new Window(1, TimeUnit.HOURS), new Window(14, TimeUnit.DAYS)))),
      metaData = Builders.MetaData(name = "unit_test.user_payments")
    )

    val today = Constants.Partition.at(System.currentTimeMillis())
    println(today)
    GroupBy
      .from(userPaymentsGroupBy, PartitionRange(today, today), TableUtils(spark))
      .temporalEventsUpload(today)
      .show()
    // create groupBy data

    // create queries
    // sawtooth aggregate batch data
    // split "streaming" data
    // bulk upload batch data

    // merge queries + streaming data and replay to inmemory store
    // streaming data gets "put", queries do "get"

  }
}
