package ai.zipline.spark.test

import ai.zipline.aggregator.base.{IntType, LongType, StringType}
import ai.zipline.aggregator.test.{CStream, Column, RowsWithSchema}
import ai.zipline.api.{Builders, Operation, TimeUnit, Window}

class FetcherTest {
  def testTemporalFetch: Unit = {

    val paymentCols = Seq(Column("user", StringType, 100),
                          Column("vendor", StringType, 10),
                          Column("ts", LongType, 60),
                          Column("payment", LongType, 100))

    val ratingCols = Seq(Column("user", StringType, 100),
                         Column("vendor", StringType, 10),
                         Column("ts", LongType, 180),
                         Column("rating", IntType, 5))

    val queryCols =
      Seq(Column("user_id", StringType, 100), Column("vendor_id", StringType, 10), Column("ts", LongType, 1))

    val vendorRatingsGroupBy = Builders.GroupBy(
      sources = Seq(Builders.Source.events(query = Builders.Query(), table = "ratings_table")),
      keyColumns = Seq("vendor"),
      aggregations = Seq(
        Builders.Aggregation(operation = Operation.AVERAGE,
                             inputColumn = "rating",
                             windows = Seq(new Window(2, TimeUnit.DAYS), new Window(30, TimeUnit.DAYS)))),
      metaData = Builders.MetaData(name = "unit_test.vendor_ratings")
    )

    val userPaymentsGroupBy = Builders.GroupBy(
      sources = Seq(Builders.Source.events(query = Builders.Query(), table = "payments_table")),
      keyColumns = Seq("user"),
      aggregations = Seq(
        Builders.Aggregation(operation = Operation.SUM,
                             inputColumn = "payment",
                             windows = Seq(new Window(1, TimeUnit.HOURS), new Window(10, TimeUnit.DAYS)))),
      metaData = Builders.MetaData(name = "unit_test.user_payments")
    )

    val RowsWithSchema(rows, schema) = CStream.gen(paymentCols, 100000)
    // create groupBy data

    // create queries
    // sawtooth aggregate batch data
    // split "streaming" data
    // bulk upload batch data

    // merge queries + streaming data and replay to inmemory store
    // streaming data gets "put", queries do "get"

  }
}
