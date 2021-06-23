package ai.zipline.spark
import ai.zipline.api.Extensions._
import ai.zipline.api.{ThriftJsonCodec, StagingQuery => StagingQueryConf}
import ai.zipline.spark.Extensions._

import scala.collection.JavaConverters._

class StagingQuery(stagingQueryConf: StagingQueryConf, endPartition: String, tableUtils: TableUtils) {
  assert(Option(stagingQueryConf.metaData.outputNamespace).nonEmpty, s"output namespace could not be empty or null")
  private val outputTable = s"${stagingQueryConf.metaData.outputNamespace}.${stagingQueryConf.metaData.cleanName}"
  private val tableProps = Option(stagingQueryConf.metaData.tableProperties)
    .map(_.asScala.toMap)
    .orNull

  private final val StartDateRegex = replacementRegexFor("start_date")
  private final val EndDateRegex = replacementRegexFor("end_date")
  private def replacementRegexFor(literal: String): String = s"\\{\\{\\s*$literal\\s*\\}\\}"

  def computeStagingQuery(): Unit = {
    Option(stagingQueryConf.setups).foreach(_.asScala.foreach(tableUtils.sql))
    val stagingQueryUnfilledRange: PartitionRange =
      tableUtils.unfilledRange(outputTable, PartitionRange(stagingQueryConf.startPartition, endPartition))

    println(s"Staging Query unfilled range: $stagingQueryUnfilledRange")
    val renderedQuery = stagingQueryConf.query
      .replaceAll(StartDateRegex, stagingQueryUnfilledRange.start)
      .replaceAll(EndDateRegex, stagingQueryUnfilledRange.end)
    println(s"Rendered Staging Query to run is:\n$renderedQuery")
    tableUtils.sql(renderedQuery).save(outputTable, tableProps)
    println(s"Finished writing Staging Query data to $outputTable")
  }
}

object StagingQuery {
  def main(args: Array[String]): Unit = {
    val parsedArgs = new BatchArgs(args)
    val stagingQueryConf = parsedArgs.parseConf[StagingQueryConf]
    val stagingQueryJob = new StagingQuery(
      stagingQueryConf,
      parsedArgs.endDate(),
      TableUtils(SparkSessionBuilder.build(s"staging_query_${stagingQueryConf.metaData.name}", local = false))
    )
    stagingQueryJob.computeStagingQuery()
  }
}
