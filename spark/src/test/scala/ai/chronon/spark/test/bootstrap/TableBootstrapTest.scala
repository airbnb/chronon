package ai.chronon.spark.test.bootstrap

import ai.chronon.api._
import ai.chronon.spark.Extensions._
import ai.chronon.spark.{Comparison, SparkSessionBuilder, TableUtils}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
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
    val groupBy = BootstrapUtils.buildGroupBy(namespace, spark)

    // query
    val queryTable = BootstrapUtils.buildQuery(namespace, spark)

    // Define base join which uses standard backfill
    val baseJoin = Builders.Join(
      left = Builders.Source.events(
        table = queryTable,
        query = Builders.Query()
      ),
      joinParts = Seq(Builders.JoinPart(groupBy = groupBy)),
      rowIds = Seq("request_id"),
      metaData = Builders.MetaData(name = "test.user_transaction_features", namespace = namespace, team = "chronon")
    )

    // Runs through standard backfill
    val runner1 = new ai.chronon.spark.Join(baseJoin, today, tableUtils)
    val baseOutput = runner1.computeJoin()

    // Create bootstrap dataset with randomly overwritten data
    def buildBootstrapPart(tableName: String): (BootstrapPart, DataFrame) = {
      val bootstrapTable = s"$namespace.$tableName"
      val bootstrapDf = spark
        .table(queryTable)
        .select(
          col("request_id"),
          (rand() * 30000)
            .cast(org.apache.spark.sql.types.LongType)
            .as("unit_test_user_transactions_amount_dollars_sum_30d"),
          col("ds")
        )
        .sample(0.8)

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

      (bootstrapPart, bootstrapDf)
    }

    // Create two bootstrap parts to verify that bootstrap coalesce respects the ordering of the input bootstrap parts
    val (bootstrapTable1, bootstrapTable2) = ("user_transactions_bootstrap1", "user_transactions_bootstrap2")
    val (bootstrapPart1, bootstrapDf1) = buildBootstrapPart(bootstrapTable1)
    val (bootstrapPart2, bootstrapDf2) = buildBootstrapPart(bootstrapTable2)

    // Create bootstrap join using base join as template
    val bootstrapJoin = baseJoin.deepCopy()
    bootstrapJoin.getMetaData.setName("test.user_transaction_features.bootstrap")
    bootstrapJoin
      .setBootstrapParts(
        ScalaVersionSpecificCollectionsConverter.convertScalaSeqToJava(Seq(bootstrapPart1, bootstrapPart2))
      )

    // Runs through boostrap backfill which combines backfill and bootstrap
    val runner2 = new ai.chronon.spark.Join(bootstrapJoin, today, tableUtils)
    val computed = runner2.computeJoin()

    // Comparison
    val expected = baseOutput
      .join(bootstrapDf1,
            baseOutput("request_id") <=> bootstrapDf1("request_id") and baseOutput("ds") <=> bootstrapDf1("ds"),
            "left")
      .join(bootstrapDf2,
            baseOutput("request_id") <=> bootstrapDf2("request_id") and baseOutput("ds") <=> bootstrapDf2("ds"),
            "left")
      .select(
        baseOutput("user"),
        baseOutput("request_id"),
        baseOutput("ts"),
        coalesce(
          coalesce(bootstrapDf1("unit_test_user_transactions_amount_dollars_sum_30d"),
                   bootstrapDf2("unit_test_user_transactions_amount_dollars_sum_30d")),
          baseOutput("unit_test_user_transactions_amount_dollars_sum_30d")
        ).as("unit_test_user_transactions_amount_dollars_sum_30d"),
        baseOutput("unit_test_user_transactions_amount_dollars_sum_15d"), // not covered by bootstrap
        baseOutput("ds")
      )

    val overlapBaseBootstrap1 = baseOutput.join(bootstrapDf1, Seq("request_id", "ds")).count()
    val overlapBaseBootstrap2 = baseOutput.join(bootstrapDf2, Seq("request_id", "ds")).count()
    val overlapBootstrap12 = bootstrapDf1.join(bootstrapDf2, Seq("request_id", "ds")).count()
    println(s"""Debug information:
         |base count: ${baseOutput.count()}
         |overlap keys between base and bootstrap1 count: ${overlapBaseBootstrap1}
         |overlap keys between base and bootstrap2 count: ${overlapBaseBootstrap2}
         |overlap keys between bootstrap1 and bootstrap2 count: ${overlapBootstrap12}
         |""".stripMargin)

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
