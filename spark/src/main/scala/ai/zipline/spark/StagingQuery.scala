package ai.zipline.spark
import ai.zipline.api.Extensions._
import ai.zipline.api.{ThriftJsonDecoder, StagingQuery => StagingQueryConf}
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
    Option(stagingQueryConf.setups.asScala).map(_ => tableUtils.sql(_))
    val stagingQueryUnfilledRange: PartitionRange =
      tableUtils.unfilledRange(outputTable, PartitionRange(stagingQueryConf.startPartition, endPartition))

    println(s"staging query unfilled range: $stagingQueryUnfilledRange")
    val renderedQuery = stagingQueryConf.query
      .replaceAll(StartDateRegex, stagingQueryUnfilledRange.start)
      .replaceAll(EndDateRegex, stagingQueryUnfilledRange.end)
    println(s"the rendered staging query to run is $renderedQuery")
    tableUtils.sql(renderedQuery).save(outputTable, tableProps)
    println(s"Finished writing staging query data to $outputTable")
  }
}

object StagingQuery {
  import org.rogach.scallop._
  class ParsedArgs(args: Seq[String]) extends ScallopConf(args) {
    val confPath: ScallopOption[String] = opt[String](required = true)
    val endDate: ScallopOption[String] = opt[String](required = true)
    verify()
  }

  def main(args: Array[String]): Unit = {
    // args = conf path, end date
    val parsedArgs = new ParsedArgs(args)
    println(s"Parsed Args: $parsedArgs")
    val stagingQueryConf =
      ThriftJsonDecoder
        .fromJsonFile[StagingQueryConf](parsedArgs.confPath(), check = true, clazz = classOf[StagingQueryConf])

    val stagingQueryJob = new StagingQuery(
      stagingQueryConf,
      parsedArgs.endDate(),
      TableUtils(SparkSessionBuilder.build(s"staging_query_${stagingQueryConf.metaData.name}", local = false))
    )
    stagingQueryJob.computeStagingQuery()
  }
}
