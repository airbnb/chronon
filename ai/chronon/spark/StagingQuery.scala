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

import org.slf4j.LoggerFactory
import ai.chronon.api
import ai.chronon.api.ParametricMacro
import ai.chronon.api.Extensions._
import ai.chronon.spark.Extensions._

import scala.collection.mutable
import scala.util.ScalaJavaConversions._

class StagingQuery(stagingQueryConf: api.StagingQuery, endPartition: String, tableUtils: TableUtils) {
  @transient lazy val logger = LoggerFactory.getLogger(getClass)
  assert(Option(stagingQueryConf.metaData.outputNamespace).nonEmpty, s"output namespace could not be empty or null")
  private val outputTable = stagingQueryConf.metaData.outputTable
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
    Option(stagingQueryConf.setups).foreach(_.toScala.foreach(tableUtils.sql))
    // the input table is not partitioned, usually for data testing or for kaggle demos
    if (stagingQueryConf.startPartition == null) {
      tableUtils.sql(stagingQueryConf.query).save(outputTable)
      return
    }
    val overrideStart = overrideStartPartition.getOrElse(stagingQueryConf.startPartition)
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
}

object StagingQuery {
  @transient lazy val logger = LoggerFactory.getLogger(getClass)

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
