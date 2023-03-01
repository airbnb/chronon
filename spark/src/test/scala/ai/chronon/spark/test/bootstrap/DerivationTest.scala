package ai.chronon.spark.test.bootstrap

import ai.chronon.api._
import ai.chronon.spark.Extensions.DataframeOps
import ai.chronon.spark.{Comparison, SparkSessionBuilder, TableUtils}
import org.apache.spark.sql.functions.{coalesce, col, lit, rand}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.junit.Assert.{assertEquals, assertTrue}
import org.junit.Test

import scala.util.ScalaVersionSpecificCollectionsConverter

class DerivationTest {

  val spark: SparkSession = SparkSessionBuilder.build("DerivationTest", local = true)
  val namespace = "test_derivations"
  spark.sql(s"CREATE DATABASE IF NOT EXISTS $namespace")
  private val tableUtils = TableUtils(spark)
  private val today = Constants.Partition.at(System.currentTimeMillis())

  private def buildBootstrapConf(baseJoin: Join): (Join, DataFrame, DataFrame, DataFrame) = {
    val leftTable = baseJoin.left.getEvents.table

    /* directly bootstrap a derived feature field */
    val diffBootstrapDf = spark
      .table(leftTable)
      .select(
        col("request_id"),
        (rand() * 30000)
          .cast(org.apache.spark.sql.types.LongType)
          .as("user_amount_30d_minus_15d"),
        col("ds")
      )
      .sample(0.8)
    val diffBootstrapTable = s"$namespace.bootstrap_diff"
    diffBootstrapDf.save(diffBootstrapTable)
    val diffBootstrapRange = diffBootstrapDf.partitionRange
    val diffBootstrapPart = Builders.BootstrapPart(
      query = Builders.Query(
        selects = Builders.Selects("request_id", "user_amount_30d_minus_15d"),
        startPartition = diffBootstrapRange.start,
        endPartition = diffBootstrapRange.end
      ),
      table = diffBootstrapTable
    )

    /* bootstrap an external feature field such that it can be used in a downstream derivation */
    val externalBootstrapDf = spark
      .table(leftTable)
      .select(
        col("request_id"),
        (rand() * 30000)
          .cast(org.apache.spark.sql.types.LongType)
          .as("ext_payments_service_user_txn_count_15d"),
        col("ds")
      )
      .sample(0.8)
    val externalBootstrapTable = s"$namespace.bootstrap_external"
    externalBootstrapDf.save(externalBootstrapTable)
    val externalBootstrapRange = externalBootstrapDf.partitionRange
    val externalBootstrapPart = Builders.BootstrapPart(
      query = Builders.Query(
        selects = Builders.Selects("request_id", "ext_payments_service_user_txn_count_15d"),
        startPartition = externalBootstrapRange.start,
        endPartition = externalBootstrapRange.end
      ),
      table = externalBootstrapTable
    )

    /* bootstrap an contextual feature field such that it can be used in a downstream derivation */
    val contextualBoostrapDf = spark
      .table(leftTable)
      .select(
        col("request_id"),
        (rand() * 30000)
          .cast(org.apache.spark.sql.types.LongType)
          .as("user_txn_count_30d"),
        col("ds")
      )
      .sample(0.8)
    val contextualBootstrapTable = s"$namespace.bootstrap_contextual"
    contextualBoostrapDf.save(contextualBootstrapTable)
    val contextualBootstrapRange = contextualBoostrapDf.partitionRange
    val contextualBootstrapPart = Builders.BootstrapPart(
      query = Builders.Query(
        // bootstrap of contextual fields will against the keys but it should be propagated to the values as well
        selects = Builders.Selects("request_id", "user_txn_count_30d"),
        startPartition = contextualBootstrapRange.start,
        endPartition = contextualBootstrapRange.end
      ),
      table = contextualBootstrapTable
    )

    /* construct final boostrap join with 3 bootstrap parts */
    val bootstrapJoin = baseJoin.deepCopy()
    bootstrapJoin.getMetaData.setName("test.derivations_join_w_bootstrap")
    bootstrapJoin
      .setBootstrapParts(
        ScalaVersionSpecificCollectionsConverter.convertScalaSeqToJava(
          Seq(
            diffBootstrapPart,
            externalBootstrapPart,
            contextualBootstrapPart
          ))
      )

    (bootstrapJoin, diffBootstrapDf, externalBootstrapDf, contextualBoostrapDf)
  }

  @Test
  def testBootstrapToDerivations(): Unit = {

    val groupBy = BootstrapUtils.buildGroupBy(namespace, spark)
    val queryTable = BootstrapUtils.buildQuery(namespace, spark)

    val joinConf = Builders.Join(
      left = Builders.Source.events(
        table = queryTable,
        query = Builders.Query()
      ),
      joinParts = Seq(Builders.JoinPart(groupBy = groupBy)),
      rowIds = Seq("request_id"),
      externalParts = Seq(
        Builders.ExternalPart(
          Builders.ExternalSource(
            metadata = Builders.MetaData(name = "payments_service"),
            keySchema = StructType(name = "keys", fields = Array(StructField("user", StringType))),
            valueSchema = StructType(name = "values", fields = Array(StructField("user_txn_count_15d", LongType)))
          )
        ),
        Builders.ExternalPart(
          Builders.ContextualSource(
            fields = Array(StructField("user_txn_count_30d", LongType))
          )
        )
      ),
      derivations = Seq(
        Builders.Derivation(
          name = "*"
        ),
        // derivation based on one external field (rename)
        Builders.Derivation(
          name = "user_txn_count_15d",
          expression = "ext_payments_service_user_txn_count_15d"
        ),
        // derivation based on one group by field (rename)
        Builders.Derivation(
          name = "user_amount_30d",
          expression = "unit_test_user_transactions_amount_dollars_sum_30d"
        ),
        // derivation based on one group by field (rename)
        Builders.Derivation(
          name = "user_amount_15d",
          expression = "unit_test_user_transactions_amount_dollars_sum_15d"
        ),
        // derivation based on two group by fields
        Builders.Derivation(
          name = "user_amount_30d_minus_15d",
          expression =
            "unit_test_user_transactions_amount_dollars_sum_30d - unit_test_user_transactions_amount_dollars_sum_15d"
        ),
        // derivation based on one group by field and one contextual field
        Builders.Derivation(
          name = "user_amount_avg_30d",
          expression = "1.0 * unit_test_user_transactions_amount_dollars_sum_30d / ext_contextual_user_txn_count_30d"
        ),
        // derivation based on one group by field and one external field
        Builders.Derivation(
          name = "user_amount_avg_15d",
          expression =
            "1.0 * unit_test_user_transactions_amount_dollars_sum_15d / ext_payments_service_user_txn_count_15d"
        )
      ),
      metaData = Builders.MetaData(name = "test.derivations_join", namespace = namespace, team = "chronon")
    )

    val runner = new ai.chronon.spark.Join(joinConf, today, tableUtils)
    val outputDf = runner.computeJoin()

    assertTrue(
      outputDf.columns sameElements Array(
        "user",
        "request_id",
        "ts",
        "user_txn_count_30d",
        "user_txn_count_15d",
        "user_amount_30d",
        "user_amount_15d",
        "user_amount_30d_minus_15d",
        "user_amount_avg_30d",
        "user_amount_avg_15d",
        "ds"
      ))

    val (bootstrapJoin, diffBootstrapDf, externalBootstrapDf, contextualBootstrapDf) = buildBootstrapConf(joinConf)

    val runner2 = new ai.chronon.spark.Join(bootstrapJoin, today, tableUtils)
    val computed = runner2.computeJoin()

    // Comparison
    val expected = outputDf
      .join(diffBootstrapDf,
            outputDf("request_id") <=> diffBootstrapDf("request_id") and outputDf("ds") <=> diffBootstrapDf("ds"),
            "left")
      .join(
        externalBootstrapDf,
        outputDf("request_id") <=> externalBootstrapDf("request_id") and outputDf("ds") <=> externalBootstrapDf("ds"),
        "left")
      .join(contextualBootstrapDf,
            outputDf("request_id") <=> contextualBootstrapDf("request_id") and outputDf("ds") <=> contextualBootstrapDf(
              "ds"),
            "left")
      .select(
        outputDf("user"),
        outputDf("request_id"),
        outputDf("ts"),
        contextualBootstrapDf("user_txn_count_30d"),
        externalBootstrapDf("ext_payments_service_user_txn_count_15d").as("user_txn_count_15d"),
        outputDf("user_amount_30d"),
        outputDf("user_amount_15d"),
        coalesce(diffBootstrapDf("user_amount_30d_minus_15d"), outputDf("user_amount_30d_minus_15d"))
          .as("user_amount_30d_minus_15d"),
        (outputDf("user_amount_30d") * lit(1.0) / contextualBootstrapDf("user_txn_count_30d"))
          .as("user_amount_avg_30d"),
        (outputDf("user_amount_15d") * lit(1.0) / externalBootstrapDf("ext_payments_service_user_txn_count_15d"))
          .as("user_amount_avg_15d"),
        outputDf("ds")
      )

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
