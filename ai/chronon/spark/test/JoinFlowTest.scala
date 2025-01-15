package ai.chronon.spark.test;

import ai.chronon.aggregator.test.Column
import ai.chronon.api
import ai.chronon.api._
import ai.chronon.spark._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.junit.Assert._
import org.junit.Test

import scala.util.Random

class JoinFlowTest {

  val spark: SparkSession = SparkSessionBuilder.build("JoinFlowTest", local = true)
  private val tableUtils = TableUtils(spark)

  @Test
  def testBootstrapAndDerivation(): Unit = {
    val namespace = "test_namespace_joinflowtest" + "_" + Random.alphanumeric.take(6).mkString
    tableUtils.createDatabase(namespace)

    // in JoinBackfill Flow mode, left, rights and final steps are triggered by separate Driver mode

    val prefix = "test_bootstrap_and_derivation"

    // left part
    val querySchema = Seq(Column("user", api.LongType, 100))
    val queryTable = s"$namespace.${prefix}_left_table"
    DataFrameGen
      .events(spark, querySchema, 200, partitions = 5)
      .where(col("user").isNotNull)
      .dropDuplicates("user")
      .save(queryTable)
    val querySource = Builders.Source.events(
      table = queryTable,
      query = Builders.Query(Builders.Selects("user"), timeColumn = "ts")
    )

    // right part
    val transactionSchema = Seq(
      Column("user", LongType, 100),
      Column("amount", LongType, 1000)
    )
    val transactionsTable = s"$namespace.${prefix}_transactions"
    DataFrameGen
      .events(spark, transactionSchema, 1200, partitions = 30)
      .where(col("user").isNotNull)
      .save(transactionsTable)

    val joinPart: JoinPart = Builders.JoinPart(groupBy = Builders.GroupBy(
      keyColumns = Seq("user"),
      sources = Seq(
        Builders.Source.events(
          query = Builders.Query(
            selects = Builders.Selects("amount"),
            timeColumn = "ts"
          ),
          table = transactionsTable
        )),
      aggregations = Seq(
        Builders.Aggregation(operation = Operation.SUM,
                             inputColumn = "amount",
                             windows = Seq(new Window(7, TimeUnit.DAYS), new Window(14, TimeUnit.DAYS)))),
      accuracy = Accuracy.SNAPSHOT,
      metaData = Builders.MetaData(name = s"join_test.${prefix}_txn", namespace = namespace, team = "chronon")
    ))

    // bootstrap part
    val bootstrapSourceTable = s"$namespace.${prefix}_bootstrap_source"
    spark
      .table(queryTable)
      .sample(0.5)
      .withColumn(s"${joinPart.fullPrefix}_amount_sum_7d", lit(0).cast("long"))
      .withColumn(s"${joinPart.fullPrefix}_amount_sum_14d", lit(0).cast("long"))
      .save(bootstrapSourceTable)
    val bootstrapPart = Builders.BootstrapPart(
      table = bootstrapSourceTable,
      keyColumns = Seq("user"),
      query = Builders.Query(
        Builders.Selects("user", s"${joinPart.fullPrefix}_amount_sum_7d", s"${joinPart.fullPrefix}_amount_sum_14d"))
    )

    // derivations
    val derivations = Seq(
      Builders.Derivation(
        name = s"${joinPart.fullPrefix}_amount_sum_7d",
        expression = s"COALESCE(${joinPart.fullPrefix}_amount_sum_7d, 0)"
      ),
      Builders.Derivation(
        name = s"${joinPart.fullPrefix}_amount_sum_14d",
        expression = s"COALESCE(${joinPart.fullPrefix}_amount_sum_14d, 0)"
      )
    )

    // join
    val join = Builders.Join(
      left = querySource,
      joinParts = Seq(joinPart),
      bootstrapParts = Seq(bootstrapPart),
      rowIds = Seq("user"),
      derivations = derivations,
      metaData = Builders.MetaData(name = s"unit_test.${prefix}_join", namespace = namespace, team = "chronon")
    )

    val endDs = tableUtils.partitions(queryTable).max

    // compute left
    val joinLeftJob = new Join(join.deepCopy(), endDs, tableUtils)
    joinLeftJob.computeLeft()
    assertTrue(tableUtils.tableExists(join.metaData.bootstrapTable))

    // compute right
    val joinPartJob = new Join(join.deepCopy(), endDs, tableUtils, selectedJoinParts = Some(List(joinPart.fullPrefix)))
    joinPartJob.computeJoinOpt(useBootstrapForLeft = true)
    assertTrue(tableUtils.tableExists(join.partOutputTable(joinPart)))

    // compute final
    val joinFinalJob = new Join(join.deepCopy(), endDs, tableUtils)
    joinFinalJob.computeFinal()
    assertTrue(tableUtils.tableExists(join.metaData.outputTable))
  }
}
