package ai.chronon.spark.test;

import ai.chronon.aggregator.test.Column
import ai.chronon.api
import ai.chronon.api.Extensions._
import ai.chronon.api._
import ai.chronon.spark.Extensions._
import ai.chronon.spark.catalog.TableUtils
import ai.chronon.spark.{Join, _}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.junit.Assert._
import org.junit.Test
import org.slf4j.LoggerFactory

import scala.util.Random

class JoinFlowTest {
  @transient lazy val logger = LoggerFactory.getLogger(getClass)

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

  @Test
  def testExternalPartWithOfflineGroupBy(): Unit = {
    val namespace = "test_namespace_external_part" + "_" + Random.alphanumeric.take(6).mkString
    tableUtils.createDatabase(namespace)

    // in JoinBackfill Flow mode, left, rights and final steps are triggered by separate Driver mode
    val prefix = "test_external_part_offline_gb"

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

    // Create a GroupBy for external part's offlineGroupBy
    val externalDataSchema = Seq(
      Column("user", LongType, 100),
      Column("score", LongType, 1000)
    )
    val externalDataTable = s"$namespace.${prefix}_external_data"
    DataFrameGen
      .events(spark, externalDataSchema, 1000, partitions = 20)
      .where(col("user").isNotNull)
      .save(externalDataTable)

    val externalOfflineGroupBy = Builders.GroupBy(
      keyColumns = Seq("user"),
      sources = Seq(
        Builders.Source.events(
          query = Builders.Query(
            selects = Builders.Selects("score"),
            timeColumn = "ts"
          ),
          table = externalDataTable
        )),
      aggregations = Seq(
        Builders.Aggregation(operation = Operation.AVERAGE,
                             inputColumn = "score",
                             windows = Seq(new Window(7, TimeUnit.DAYS)))),
      accuracy = Accuracy.SNAPSHOT,
      metaData = Builders.MetaData(name = s"join_test.${prefix}_external_gb", namespace = namespace, team = "chronon")
    )

    // Create ExternalPart with offlineGroupBy
    val externalSource = new ExternalSource()
    externalSource.setOfflineGroupBy(externalOfflineGroupBy)
    externalSource.setMetadata(Builders.MetaData(name = s"${prefix}_external_source", namespace = namespace, team = "chronon"))

    val externalPart = new ExternalPart()
    externalPart.setSource(externalSource)
    externalPart.setPrefix("ext")

    // Regular join part
    val regularJoinPart = Builders.JoinPart(groupBy = Builders.GroupBy(
      keyColumns = Seq("user"),
      sources = Seq(
        Builders.Source.events(
          query = Builders.Query(
            selects = Builders.Selects("score"),
            timeColumn = "ts"
          ),
          table = externalDataTable
        )),
      aggregations = Seq(
        Builders.Aggregation(operation = Operation.SUM,
                             inputColumn = "score",
                             windows = Seq(new Window(14, TimeUnit.DAYS)))),
      accuracy = Accuracy.SNAPSHOT,
      metaData = Builders.MetaData(name = s"join_test.${prefix}_regular_gb", namespace = namespace, team = "chronon")
    ))

    // Create join with both regular join part and external part with offlineGroupBy
    val join = Builders.Join(
      left = querySource,
      joinParts = Seq(regularJoinPart),
      externalParts = Seq(externalPart),
      rowIds = Seq("user"),
      metaData = Builders.MetaData(name = s"unit_test.${prefix}_join", namespace = namespace, team = "chronon")
    )

    val endDs = tableUtils.partitions(queryTable).max

    // Verify that getRegularAndExternalJoinParts includes both parts
    val allJoinParts = join.getRegularAndExternalJoinParts
    assertEquals("Should have 2 join parts (1 regular + 1 external)", 2, allJoinParts.size)

    // compute left
    val joinLeftJob = new Join(join.deepCopy(), endDs, tableUtils)
    joinLeftJob.computeLeft()
    assertTrue(tableUtils.tableExists(join.metaData.bootstrapTable))

    // compute regular join part
    val regularPartJob = new Join(join.deepCopy(), endDs, tableUtils, selectedJoinParts = Some(List(regularJoinPart.fullPrefix)))
    regularPartJob.computeJoinOpt(useBootstrapForLeft = true)
    assertTrue(tableUtils.tableExists(join.partOutputTable(regularJoinPart)))

    // compute external part (which has offlineGroupBy)
    // Find the external join part in the combined list
    val externalJoinPart = allJoinParts.find(_.groupBy.metaData.name == externalOfflineGroupBy.metaData.name).get
    val externalPartJob = new Join(join.deepCopy(), endDs, tableUtils, selectedJoinParts = Some(List(externalJoinPart.fullPrefix)))
    externalPartJob.computeJoinOpt(useBootstrapForLeft = true)
    // For external parts with offlineGroupBy, verify the part table exists
    assertTrue(tableUtils.tableExists(join.partOutputTable(externalJoinPart)))

    // compute final - should include both regular and external parts
    val joinFinalJob = new Join(join.deepCopy(), endDs, tableUtils)
    joinFinalJob.computeFinal()
    assertTrue(tableUtils.tableExists(join.metaData.outputTable))

    // Verify the final output contains columns from both regular and external parts
    val finalDf = spark.table(join.metaData.outputTable)
    assertTrue("Should contain regular part columns", finalDf.columns.exists(_.contains(regularJoinPart.fullPrefix)))
    assertTrue("Should contain external part columns", finalDf.columns.exists(_.contains("ext")))

    // Verify that external groupby columns have non-null data
    val externalColumnName = s"${externalJoinPart.fullPrefix}_score_average_7d"
    val regularColumnName = s"${regularJoinPart.fullPrefix}_score_sum_14d"

    assertTrue(s"Final output should contain external column: $externalColumnName",
               finalDf.columns.contains(externalColumnName))
    assertTrue(s"Final output should contain regular column: $regularColumnName",
               finalDf.columns.contains(regularColumnName))

    // Verify non-null values exist in the external and regular columns
    val nonNullExternalCount = finalDf.filter(col(externalColumnName).isNotNull).count()
    val nonNullRegularCount = finalDf.filter(col(regularColumnName).isNotNull).count()

    assertTrue(s"External groupby column $externalColumnName should have non-null values, but found 0",
               nonNullExternalCount > 0)
    assertTrue(s"Regular groupby column $regularColumnName should have non-null values, but found 0",
               nonNullRegularCount > 0)

    logger.info(s"External column $externalColumnName has $nonNullExternalCount non-null values out of ${finalDf.count()} rows")
    logger.info(s"Regular column $regularColumnName has $nonNullRegularCount non-null values out of ${finalDf.count()} rows")
  }
}
