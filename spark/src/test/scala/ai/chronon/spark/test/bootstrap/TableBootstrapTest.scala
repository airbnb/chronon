package ai.chronon.spark.test.bootstrap

import ai.chronon.aggregator.test.Column
import ai.chronon.api
import ai.chronon.api._
import ai.chronon.spark.Extensions._
import ai.chronon.spark.test.DataFrameGen
import ai.chronon.spark.{Comparison, SparkSessionBuilder, TableUtils}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.junit.Assert.assertEquals
import org.junit.Test

import scala.util.ScalaVersionSpecificCollectionsConverter

class TableBootstrapTest {

  val spark: SparkSession = SparkSessionBuilder.build("BootstrapTest", local = true)
  val namespace = "test_table_bootstrap"
  spark.sql(s"CREATE DATABASE IF NOT EXISTS $namespace")
  private val tableUtils = TableUtils(spark)
  private val today = Constants.Partition.at(System.currentTimeMillis())

  @Test
  def testBootstrap(): Unit = {

    // group by
    val groupBy = Utils.buildGroupBy(namespace, spark)

    // query
    val queryTable = Utils.buildQuery(namespace, spark)

    // bootstrap
    val bootstrapSchema = List(
      Column("request_id", api.StringType, 100),
      Column("unit_test_user_transactions_amount_dollars_sum_30d", api.LongType, 30000)
    )
    val bootstrapTable = s"$namespace.user_transactions_bootstrap"
    val bootstrapDf = DataFrameGen
      .events(spark, bootstrapSchema, 200, partitions = 5)
      // when a bootstrap feature is null, the bloomFilter causes non-deterministic behavior, because the backfilled
      // keys are a superset of required keys.
      .where(col("unit_test_user_transactions_amount_dollars_sum_30d").isNotNull)
      // drop duplicates to ensure determinism during testing
      .dropDuplicates("request_id")

    bootstrapDf.save(bootstrapTable)
    val partitionRange = bootstrapDf.partitionRange

    val bootstrapPart = Builders.BootstrapPart(
      query = Builders.Query(
        selects = Builders.Selects("request_id", "unit_test_user_transactions_amount_dollars_sum_30d"),
        startPartition = partitionRange.start,
        endPartition = partitionRange.end
      ),
      table = bootstrapTable
    )

    // join
    val baseJoin = Builders.Join(
      left = Builders.Source.events(
        table = queryTable,
        query = Builders.Query()
      ),
      joinParts = Seq(Builders.JoinPart(groupBy = groupBy)),
      rowIds = Seq("request_id"),
      metaData = Builders.MetaData(name = "test.user_transaction_features", namespace = namespace, team = "chronon")
    )

    val bootstrapJoin = baseJoin.deepCopy()
    bootstrapJoin.getMetaData.setName("test.user_transaction_features.bootstrap")
    bootstrapJoin
      .setBootstrapParts(
        ScalaVersionSpecificCollectionsConverter.convertScalaSeqToJava(Seq(bootstrapPart))
      )

    // computation
    val runner1 = new ai.chronon.spark.Join(baseJoin, today, tableUtils)
    val runner2 = new ai.chronon.spark.Join(bootstrapJoin, today, tableUtils)

    val baseOutput = runner1.computeJoin()
    val expected = baseOutput
      .join(bootstrapDf,
            baseOutput("request_id") <=> bootstrapDf("request_id") and baseOutput("ds") <=> bootstrapDf("ds"),
            "left")
      .select(
        baseOutput("user"),
        baseOutput("request_id"),
        baseOutput("ts"),
        coalesce(bootstrapDf("unit_test_user_transactions_amount_dollars_sum_30d"),
                 baseOutput("unit_test_user_transactions_amount_dollars_sum_30d"))
          .as("unit_test_user_transactions_amount_dollars_sum_30d"),
        baseOutput("ds")
      )

    val computed = runner2.computeJoin()

    val overwrittenRows = baseOutput
      .join(bootstrapDf,
            baseOutput("request_id") <=> bootstrapDf("request_id") and baseOutput("ds") <=> bootstrapDf("ds"),
            "left")
      .where(not(bootstrapDf("unit_test_user_transactions_amount_dollars_sum_30d") <=> baseOutput(
        "unit_test_user_transactions_amount_dollars_sum_30d")))
      .count()

    println(s"testing bootstrap where ${overwrittenRows} rows are expected to be overwritten")

    val diff = Comparison.sideBySide(computed, expected, List("request_id", "user", "ts", "ds"))
    if (diff.count() > 0) {
      println(s"Actual count: ${computed.count()}")
      println(s"Expected count: ${expected.count()}")
      println(s"Diff count: ${diff.count()}")
      println(s"diff result rows")
      diff.show()
    }

    assertEquals(0, diff.count())
  }
}
