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

package ai.chronon.spark

import ai.chronon.api
import ai.chronon.api.Extensions._
import ai.chronon.api.ParametricMacro
import ai.chronon.spark.Extensions._
import ai.chronon.spark.catalog.TableUtils
import org.slf4j.LoggerFactory
import org.apache.spark.sql.Row

import scala.collection.mutable
import scala.util.ScalaJavaConversions._

class StagingQuery(stagingQueryConf: api.StagingQuery, endPartition: String, tableUtils: TableUtils) {
  @transient lazy val logger = LoggerFactory.getLogger(getClass)
  assert(Option(stagingQueryConf.metaData.outputNamespace).nonEmpty, s"output namespace could not be empty or null")
  private val outputTable = stagingQueryConf.metaData.outputTable
  val signalPartitionsTable =
    s"${stagingQueryConf.metaData.outputNamespace}.${StagingQuery.SIGNAL_PARTITIONS_TABLE_NAME}"
  private val tableProps = Option(stagingQueryConf.metaData.tableProperties)
    .map(_.toScala.toMap)
    .orNull

  private val partitionCols: Seq[String] = Seq(tableUtils.partitionColumn) ++
    Option(stagingQueryConf.metaData.customJsonLookUp(key = "additional_partition_cols"))
      .getOrElse(new java.util.ArrayList[String]())
      .asInstanceOf[java.util.ArrayList[String]]
      .toScala

  def computeStagingQuery(stepDays: Option[Int] = None,
                          enableAutoExpand: Option[Boolean] = Some(true),
                          overrideStartPartition: Option[String] = None,
                          skipFirstHole: Boolean = true): Unit = {
    assert(!Option(stagingQueryConf.createView).getOrElse(false),
           "createView should not be enabled for computeStagingQuery mode")
    Option(stagingQueryConf.setups).foreach(_.toScala.foreach(tableUtils.sql))
    // the input table is not partitioned, usually for data testing or for kaggle demos
    if (stagingQueryConf.startPartition == null) {
      tableUtils.sql(stagingQueryConf.query).save(outputTable)
      return
    }
    val historicalBackfill = Option(stagingQueryConf.metaData.historicalBackfill).forall(_.booleanValue)
    val effectiveOverrideStartPartition = if (historicalBackfill) {
      overrideStartPartition
    } else {
      logger.info(
        s"Historical backfill is set to false for StagingQuery ${stagingQueryConf.metaData.name}. " +
          s"Backfilling latest single partition only: $endPartition")
      Some(endPartition)
    }
    val overrideStart = effectiveOverrideStartPartition.getOrElse(stagingQueryConf.startPartition)
    val unfilledRanges =
      tableUtils.unfilledRanges(outputTable,
                                PartitionRange(overrideStart, endPartition)(tableUtils),
                                skipFirstHole = skipFirstHole)

    if (unfilledRanges.isEmpty) {
      logger.info(s"""No unfilled range for $outputTable given
           |start partition of ${stagingQueryConf.startPartition}
           |override start partition of $overrideStart
           |end partition of $endPartition
           |""".stripMargin)
      return
    }
    val stagingQueryUnfilledRanges = unfilledRanges.get
    logger.info(s"Staging Query unfilled ranges: $stagingQueryUnfilledRanges")
    val exceptions = mutable.Buffer.empty[String]
    stagingQueryUnfilledRanges.foreach { stagingQueryUnfilledRange =>
      try {
        val stepRanges = stepDays.map(stagingQueryUnfilledRange.steps).getOrElse(Seq(stagingQueryUnfilledRange))
        logger.info(s"Staging query ranges to compute: ${stepRanges.map { _.toString }.pretty}")
        stepRanges.zipWithIndex.foreach {
          case (range, index) =>
            val progress = s"| [${index + 1}/${stepRanges.size}]"
            logger.info(s"Computing staging query for range: $range  $progress")
            val renderedQuery =
              StagingQuery.substitute(tableUtils, stagingQueryConf.query, range.start, range.end, endPartition)
            logger.info(s"Rendered Staging Query to run is:\n$renderedQuery")

            val df = tableUtils.sql(renderedQuery)
            tableUtils.insertPartitions(df, outputTable, tableProps, partitionCols, autoExpand = enableAutoExpand.get)
            logger.info(s"Wrote to table $outputTable, into partitions: $range $progress")

        }
        logger.info(s"Finished writing Staging Query data to $outputTable")
      } catch {
        case err: Throwable =>
          exceptions.append(s"Error handling range $stagingQueryUnfilledRange : ${err.getMessage}\n${err.traceString}")
      }
    }
    if (exceptions.nonEmpty) {
      val length = exceptions.length
      val fullMessage = exceptions.zipWithIndex
        .map {
          case (message, index) => s"[${index + 1}/${length} exceptions]\n${message}"
        }
        .mkString("\n")
      throw new Exception(fullMessage)
    }
  }

  def createStagingQueryView(): Unit = {
    assert(Option(stagingQueryConf.createView).getOrElse(false),
           "createView must be true to use createStagingQueryView")

    // Validate that query doesn't contain date templates since this method is for views without partitioning
    val query = stagingQueryConf.query
    val startDatePattern = """\{\{\s*start_date\s*\}\}""".r
    val endDatePattern = """\{\{\s*end_date\s*\}\}""".r

    assert(
      startDatePattern.findFirstIn(query).isEmpty && endDatePattern.findFirstIn(query).isEmpty,
      "createStagingQueryView cannot be used with queries containing {{ start_date }} or {{ end_date }} templates. " +
        "Use computeStagingQuery for queries with date templates."
    )

    Option(stagingQueryConf.setups).foreach(_.toScala.foreach(tableUtils.sql))

    // Process macros but not start_date/end_date since they're not in query
    val processedQuery = StagingQuery.substitute(tableUtils, stagingQueryConf.query, null, null, endPartition)

    val createViewSql = s"CREATE OR REPLACE VIEW $outputTable AS $processedQuery"
    tableUtils.sql(createViewSql)
    logger.info(s"Created staging query view: $outputTable")

    writeSignalPartitionMetadata(outputTable)
  }

  private def writeSignalPartitionMetadata(tableName: String): Unit = {
    val schema = org.apache.spark.sql.types.StructType(
      Seq(
        org.apache.spark.sql.types
          .StructField(StagingQuery.CREATED_AT_COLUMN, org.apache.spark.sql.types.StringType, false),
        org.apache.spark.sql.types
          .StructField(tableUtils.partitionColumn, org.apache.spark.sql.types.StringType, false),
        org.apache.spark.sql.types
          .StructField(StagingQuery.TABLE_NAME_COLUMN, org.apache.spark.sql.types.StringType, false)
      ))

    // Create single row for this view with both partition values and creation timestamp
    val createdAt = java.time.Instant.now().toString
    val row = Row(createdAt, endPartition, tableName)
    val signalPartitionData = tableUtils.sparkSession.createDataFrame(
      tableUtils.sparkSession.sparkContext.parallelize(Seq(row)),
      schema
    )

    // Use insertPartitions with both ds and table_name as partition columns
    // This allows partition sensors to directly check for specific table partition existence
    tableUtils.insertPartitions(
      df = signalPartitionData,
      tableName = signalPartitionsTable,
      tableProperties = tableProps,
      partitionColumns = Seq(tableUtils.partitionColumn, StagingQuery.TABLE_NAME_COLUMN)
    )

    logger.info(
      s"Updated signal partition metadata for view $tableName in partition ${tableUtils.partitionColumn}=$endPartition/${StagingQuery.TABLE_NAME_COLUMN}=$tableName")
  }
}

object StagingQuery {
  @transient lazy val logger = LoggerFactory.getLogger(getClass)

  private val SIGNAL_PARTITIONS_TABLE_NAME = "chronon_signal_partitions"
  private val TABLE_NAME_COLUMN = "table_name"
  private val CREATED_AT_COLUMN = "created_at"

  def substitute(tu: TableUtils, query: String, start: String, end: String, latest: String): String = {
    val macros: Array[ParametricMacro] = Array(
      ParametricMacro("start_date", _ => start),
      ParametricMacro("end_date", _ => end),
      ParametricMacro("latest_date", _ => latest),
      ParametricMacro(
        "max_date",
        args => {
          lazy val table = args("table")
          lazy val partitions = tu.partitions(table)
          if (table == null) {
            throw new IllegalArgumentException(s"No table in args:[$args] to macro max_date")
          } else if (partitions.isEmpty) {
            throw new IllegalStateException(s"No partitions exist for table $table to calculate max_date")
          }
          partitions.max
        }
      )
    )

    macros.foldLeft(query) { case (q, m) => m.replace(q) }
  }

  def main(args: Array[String]): Unit = {
    val parsedArgs = new Args(args)
    parsedArgs.verify()
    val stagingQueryConf = parsedArgs.parseConf[api.StagingQuery]
    val stagingQueryJob = new StagingQuery(
      stagingQueryConf,
      parsedArgs.endDate(),
      TableUtils(
        SparkSessionBuilder.build(s"staging_query_${stagingQueryConf.metaData.name}", enforceKryoSerializer = false))
    )
    stagingQueryJob.computeStagingQuery(parsedArgs.stepDays.toOption)
  }
}
