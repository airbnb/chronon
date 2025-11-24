package ai.chronon.spark.test

import ai.chronon.aggregator.test.Column
import ai.chronon.api
import ai.chronon.api.{Builders, TimeUnit, Window}
import ai.chronon.api.Extensions._
import ai.chronon.spark.Extensions._
import ai.chronon.spark.{Comparison, Join, SparkSessionBuilder}
import ai.chronon.spark.GroupBy.logger
import ai.chronon.spark.catalog.TableUtils
import org.apache.spark.sql.SparkSession
import org.junit.Assert._
import org.junit.Test

import scala.util.Random

// Tests for joins with different partition columns on left and right sides
class JoinPartitionColumnTest {
  val dummySpark: SparkSession = SparkSessionBuilder.build("JoinPartitionColumnTest", local = true)
  private val dummyTableUtils = TableUtils(dummySpark)
  private val today = dummyTableUtils.partitionSpec.at(System.currentTimeMillis())
  private val monthAgo = dummyTableUtils.partitionSpec.minus(today, new Window(30, TimeUnit.DAYS))
  private val yearAgo = dummyTableUtils.partitionSpec.minus(today, new Window(365, TimeUnit.DAYS))
  private val dayAndMonthBefore = dummyTableUtils.partitionSpec.before(monthAgo)

  @Test
  def testJoinDifferentPartitionColumns(): Unit = {
    val spark: SparkSession =
      SparkSessionBuilder.build("JoinTest" + "_" + Random.alphanumeric.take(6).mkString, local = true)
    val tableUtils = TableUtils(spark)
    val namespace = "test_namespace_jointest" + "_" + Random.alphanumeric.take(6).mkString
    tableUtils.createDatabase(namespace)
    // Left side entities, right side entities no agg
    // Also testing specific select statement (rather than select *)
    val namesSchema = List(
      Column("user", api.StringType, 1000),
      Column("name", api.StringType, 500)
    )
    val namePartitionColumn = "name_date"
    val namesTable = s"$namespace.names"
    DataFrameGen
      .entities(spark, namesSchema, 1000, partitions = 200, partitionColOpt = Some(namePartitionColumn))
      .save(namesTable, partitionColumns = Seq(namePartitionColumn))

    val namesSource = Builders.Source.entities(
      query = Builders.Query(selects = Builders.Selects("name"),
                             startPartition = yearAgo,
                             endPartition = dayAndMonthBefore,
                             partitionColumn = namePartitionColumn),
      snapshotTable = namesTable
    )

    val namesGroupBy = Builders.GroupBy(
      sources = Seq(namesSource),
      keyColumns = Seq("user"),
      aggregations = null,
      metaData = Builders.MetaData(name = "unit_test.user_names", team = "chronon")
    )

    DataFrameGen
      .entities(spark, namesSchema, 1000, partitions = 200, partitionColOpt = Some(namePartitionColumn))
      .groupBy("user", namePartitionColumn)
      .agg(Map("name" -> "max"))
      .save(namesTable, partitionColumns = Seq(namePartitionColumn))

    // left side
    val userPartitionColumn = "user_date"
    val userSchema = List(Column("user", api.StringType, 100))
    val usersTable = s"$namespace.users"
    DataFrameGen
      .entities(spark, userSchema, 1000, partitions = 200, partitionColOpt = Some(userPartitionColumn))
      .dropDuplicates()
      .save(usersTable, partitionColumns = Seq(userPartitionColumn))

    val start = tableUtils.partitionSpec.minus(today, new Window(60, TimeUnit.DAYS))
    val end = tableUtils.partitionSpec.minus(today, new Window(15, TimeUnit.DAYS))
    val joinConf = Builders.Join(
      left = Builders.Source.entities(
        Builders.Query(selects = Map("user" -> "user"), startPartition = start, partitionColumn = userPartitionColumn),
        snapshotTable = usersTable),
      joinParts = Seq(Builders.JoinPart(groupBy = namesGroupBy)),
      metaData = Builders.MetaData(name = "test.user_features", namespace = namespace, team = "chronon")
    )

    val runner = new Join(joinConf, end, tableUtils)
    val computed = runner.computeJoin(Some(7))
    logger.debug(s"join start = $start")
    val expected = tableUtils.sql(s"""
                                     |WITH
                                     |   users AS (SELECT user, $userPartitionColumn as ds from $usersTable where $userPartitionColumn >= '$start' and $userPartitionColumn <= '$end'),
                                     |   grouped_names AS (
                                     |      SELECT user,
                                     |             name as unit_test_user_names_name,
                                     |             $namePartitionColumn as ds
                                     |      FROM $namesTable
                                     |      WHERE $namePartitionColumn >= '$yearAgo' and $namePartitionColumn <= '$dayAndMonthBefore')
                                     |   SELECT users.user,
                                     |        grouped_names.unit_test_user_names_name,
                                     |        users.ds
                                     | FROM users left outer join grouped_names
                                     | ON users.user = grouped_names.user
                                     | AND users.ds = grouped_names.ds
    """.stripMargin)

    logger.debug("showing join result")
    computed.show()
    logger.debug("showing query result")
    expected.show()
    logger.debug(
      s"Left side count: ${spark.sql(s"SELECT user, $namePartitionColumn as ds from $namesTable where $namePartitionColumn >= '$start' and $namePartitionColumn <= '$end'").count()}")
    logger.debug(s"Actual count: ${computed.count()}")
    logger.debug(s"Expected count: ${expected.count()}")
    val diff = Comparison.sideBySide(computed, expected, List("user", "ds"))
    val diffCount = diff.count()
    if (diffCount > 0) {
      logger.warn(s"Diff count: ${diffCount}")
      logger.warn(s"diff result rows")
      if (logger.isDebugEnabled) {
        diff.show()
      }
    }
    assertEquals(diffCount, 0)
  }
}
