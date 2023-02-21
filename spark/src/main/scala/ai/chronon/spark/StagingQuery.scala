package ai.chronon.spark

import ai.chronon.api
import ai.chronon.api.{Constants, ParametricMacro}
import ai.chronon.api.Extensions._
import ai.chronon.spark.Extensions._

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.util.ScalaVersionSpecificCollectionsConverter

class StagingQuery(stagingQueryConf: api.StagingQuery, endPartition: String, tableUtils: TableUtils) {
  assert(Option(stagingQueryConf.metaData.outputNamespace).nonEmpty, s"output namespace could not be empty or null")
  private val outputTable = stagingQueryConf.metaData.outputTable
  private val tableProps = Option(stagingQueryConf.metaData.tableProperties)
    .map(_.asScala.toMap)
    .orNull

  private val partitionCols: Seq[String] = Seq(tableUtils.partitionColumn) ++
    ScalaVersionSpecificCollectionsConverter.convertJavaListToScala(
      Option(stagingQueryConf.metaData.customJsonLookUp(key = "additional_partition_cols"))
        .getOrElse(new java.util.ArrayList[String]())
        .asInstanceOf[java.util.ArrayList[String]])

  def computeStagingQuery(stepDays: Option[Int] = None): Unit = {
    Option(stagingQueryConf.setups).foreach(_.asScala.foreach(tableUtils.sql))
    // the input table is not partitioned, usually for data testing or for kaggle demos
    if (stagingQueryConf.startPartition == null) {
      tableUtils.sql(stagingQueryConf.query).save(outputTable)
      return
    }
    val unfilledRanges =
      tableUtils.unfilledRanges(outputTable, PartitionRange(stagingQueryConf.startPartition, endPartition)(tableUtils))

    if (unfilledRanges.isEmpty) {
      println(s"""No unfilled range for $outputTable given
           |start partition of ${stagingQueryConf.startPartition}
           |end partition of $endPartition
           |""".stripMargin)
      return
    }
    val stagingQueryUnfilledRanges = unfilledRanges.get
    println(s"Staging Query unfilled ranges: $stagingQueryUnfilledRanges")
    val exceptions = mutable.Buffer.empty[String]
    stagingQueryUnfilledRanges.foreach { stagingQueryUnfilledRange =>
      try {
        val stepRanges = stepDays.map(stagingQueryUnfilledRange.steps).getOrElse(Seq(stagingQueryUnfilledRange))
        println(s"Staging query ranges to compute: ${stepRanges.map { _.toString }.pretty}")
        stepRanges.zipWithIndex.foreach {
          case (range, index) =>
            val progress = s"| [${index + 1}/${stepRanges.size}]"
            println(s"Computing staging query for range: $range  $progress")
            val renderedQuery = StagingQuery.substitute(tableUtils, stagingQueryConf.query, range.start, range.end, endPartition)
            println(s"Rendered Staging Query to run is:\n$renderedQuery")
            val df = tableUtils.sql(renderedQuery)
            tableUtils.insertPartitions(df, outputTable, tableProps, partitionCols)
            println(s"Wrote to table $outputTable, into partitions: $range $progress")
        }
        println(s"Finished writing Staging Query data to $outputTable")
      } catch {
        case err: Throwable =>
          exceptions.append(s"Error handling range $stagingQueryUnfilledRange : ${err.getMessage}\n${err.traceString}")
      }
    }
    if (exceptions.nonEmpty) {
      val length = exceptions.length
      val fullMessage = exceptions.zipWithIndex
        .map {
          case (message, index) => s"[${index+1}/${length} exceptions]\n${message}"
        }
        .mkString("\n")
      throw new Exception(fullMessage)
    }
  }
}

object StagingQuery {

  def substitute(tu: TableUtils, query: String, start: String, end: String, latest: String): String = {
    val macros: Array[ParametricMacro] = Array(
      ParametricMacro("start_date", _ => start),
      ParametricMacro("end_date", _ => end),
      ParametricMacro("latest_date", _ => latest),
      ParametricMacro("max_date", args => {
        lazy val table = args("table")
        lazy val partitions = tu.partitions(table)
        if(table == null) {
          throw new IllegalArgumentException(s"No table in args:[$args] to macro max_date")
        } else if (partitions.isEmpty) {
          throw new IllegalStateException(s"No partitions exist for table $table to calculate max_date")
        }
        partitions.max
      })
    )

    macros.foldLeft(query) { case(q, m) => m.replace(q)}
  }

  def main(args: Array[String]): Unit = {
    val parsedArgs = new Args(args)
    parsedArgs.verify()
    val stagingQueryConf = parsedArgs.parseConf[api.StagingQuery]
    val stagingQueryJob = new StagingQuery(
      stagingQueryConf,
      parsedArgs.endDate(),
      TableUtils(SparkSessionBuilder.build(s"staging_query_${stagingQueryConf.metaData.name}"))
    )
    stagingQueryJob.computeStagingQuery(parsedArgs.stepDays.toOption)
  }
}
