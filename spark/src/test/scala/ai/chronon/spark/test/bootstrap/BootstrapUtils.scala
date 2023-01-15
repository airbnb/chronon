package ai.chronon.spark.test.bootstrap

import ai.chronon.aggregator.test.Column
import ai.chronon.api
import ai.chronon.api._
import ai.chronon.spark.Extensions._
import ai.chronon.spark.test.DataFrameGen
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col

object BootstrapUtils {

  def buildGroupBy(namespace: String, spark: SparkSession): api.GroupBy = {
    val transactions = List(
      Column("user", LongType, 100),
      Column("amount_dollars", LongType, 1000)
    )
    val transactionsTable = s"$namespace.transactions"
    spark.sql(s"DROP TABLE IF EXISTS $transactionsTable")
    DataFrameGen
      .events(spark, transactions, 4500, partitions = 45)
      .where(col("user").isNotNull)
      .save(transactionsTable)
    val groupBySource = Builders.Source.events(
      query = Builders.Query(
        selects = Builders.Selects("amount_dollars"),
        timeColumn = "ts"
      ),
      table = transactionsTable
    )
    val groupBy = Builders.GroupBy(
      sources = Seq(groupBySource),
      keyColumns = Seq("user"),
      aggregations = Seq(
        Builders.Aggregation(operation = Operation.SUM,
                             inputColumn = "amount_dollars",
                             windows = Seq(new Window(30, TimeUnit.DAYS), new Window(15, TimeUnit.DAYS)))),
      accuracy = Accuracy.SNAPSHOT,
      metaData = Builders.MetaData(name = "unit_test.user_transactions", namespace = namespace, team = "chronon")
    )

    groupBy
  }

  def buildQuery(namespace: String, spark: SparkSession): String = {
    val queriesSchema = List(
      Column("user", api.LongType, 100),
      Column("request_id", api.StringType, 100)
    )
    val queryTable = s"$namespace.queries"
    DataFrameGen
      .events(spark, queriesSchema, 200, partitions = 5)
      .where(col("request_id").isNotNull and col("user").isNotNull)
      .dropDuplicates("request_id")
      .save(queryTable)

    queryTable
  }

}
