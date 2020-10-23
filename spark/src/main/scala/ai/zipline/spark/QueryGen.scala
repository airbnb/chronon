package ai.zipline.spark

import ai.zipline.api.Config.DataModel._
import ai.zipline.api.Config.{Constants, DataSource, GroupBy => GroupByConf}
import ai.zipline.api.QueryUtils
import ai.zipline.spark.Extensions.PartitionOps

// TODO: truncate queryRange for caching
case class QueryGen(groupBy: GroupByConf, queryRange: DataRange) {

  private lazy val maxWindow = groupBy.aggregations
    .flatMap(_.windows)
    // nulls will float to top
    .maxBy(window => Option(window).map(_.millis).getOrElse(Long.MaxValue))

  val query: String = groupBy.sources
    .flatMap { source => DataSourceScan(source).queries }
    .mkString("UNION\n")

  case class DataSourceScan(dataSource: DataSource) {
    private val partitionSpec = Constants.Partition

    // The "grid"
    private val scanRange: PartitionRange = (queryRange, dataSource.dataModel) match {
      // Events-Events
      case (TimeRange(queryStart, queryEnd), Events) =>
        val queryShiftedByWindow: String = Option(maxWindow)
          .map(w => partitionSpec.of(queryStart - w.millis))
          .orNull
        PartitionRange(queryShiftedByWindow, partitionSpec.of(queryEnd))
      // Entities-Events
      case (PartitionRange(queryStart, queryEnd), Events) =>
        val queryShiftedByWindow: String = Option(maxWindow)
          .map(partitionSpec.epochMillis(queryStart) - _.millis)
          .map(partitionSpec.of)
          .orNull
        PartitionRange(queryShiftedByWindow, queryEnd)

      // Events-Entities
      case (TimeRange(queryStart, queryEnd), Entities) =>
        PartitionRange(partitionSpec.before(queryStart), partitionSpec.before(queryEnd))

      //Entities-(Entities or CumulativeEvents)
      case (PartitionRange(queryStart, queryEnd), Entities) =>
        PartitionRange(queryStart, queryEnd)

    }

    private val mutationsScanRange: Option[PartitionRange] =
      (queryRange, dataSource.dataModel, Option(dataSource.mutationTable)) match {
        case (TimeRange(queryStart, queryEnd), Entities, Some(_)) =>
          Some(PartitionRange(partitionSpec.of(queryStart), partitionSpec.of(queryEnd)))
        case _ => None
      }

    private def toScanQuery(table: String,
                            range: PartitionRange,
                            fillIfAbsent: Map[String, String] = Map.empty[String, String]): Option[String] = {
      val sourceRange = PartitionRange(dataSource.startPartition, dataSource.endPartition)
      range.intersect(sourceRange).map { effectiveRange =>
        val startClause = Option(effectiveRange.start).map(start => s"${Constants.PartitionColumn} >= $start")
        val endClause =
          Option(effectiveRange.end).map(end => s"${Constants.PartitionColumn} <= $end")
        QueryUtils.build(dataSource.selects, table, dataSource.wheres ++ startClause ++ endClause, fillIfAbsent)
      }
    }

//    TODO: refactor the join keys for entities case
//    val queryEventToRightEntityPartition =
//      s"from_unixtime(${join.timeColumn} / 1000, '${partitionSpec.format}')"
//
//    // what is the effective right entity partition for a query entity partition
//    val queryEntityToRightEntityPartition =
//      s"from_unixtime(unix_timestamp(${join.partitionColumn},'${join.partitionSpec.format}'), '${partitionSpec.format}')"

    // what are the queries to be executed to scan data from this join source
    val queries: Seq[String] = {
      val epochMillisOfPartition =
        s"unix_timestamp(${Constants.PartitionColumn},'${partitionSpec.format}') * 1000"
      val commonColumnAliases =
        Map(Constants.TimeColumn -> dataSource.timeExpression, Constants.PartitionColumn -> null)
      val columnAliases = dataSource.dataModel match {
        case Entities =>
          Map(
            Constants.MutationTimeColumn -> epochMillisOfPartition,
            Constants.ReversalColumn -> "false",
            Constants.TimeColumn -> Option(dataSource.timeExpression).getOrElse(epochMillisOfPartition + " - 1")
          )
        case Events =>
          Map(
            Constants.MutationTimeColumn -> dataSource.timeExpression,
            Constants.ReversalColumn -> Option(dataSource.reversalExpression).getOrElse("false")
          )
      }
      val mainQuery = toScanQuery(dataSource.table, scanRange, commonColumnAliases ++ columnAliases)

      val mutationQuery = mutationsScanRange.flatMap(mutationRange =>
        toScanQuery(
          dataSource.mutationTable,
          mutationRange,
          commonColumnAliases ++ Map(
            Constants.MutationTimeColumn -> dataSource.mutationTimeExpression,
            Constants.ReversalColumn -> dataSource.reversalExpression
          )
        ))
      (mainQuery ++ mutationQuery).toSeq
    }
  }
}
