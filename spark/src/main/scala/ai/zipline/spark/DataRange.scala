package ai.zipline.spark
import ai.zipline.aggregator.windowing.TsUtils
import ai.zipline.api.{Constants, Query, QueryUtils}

import scala.collection.JavaConverters._

sealed trait DataRange {
  def toTimePoints: Array[Long]
}
case class TimeRange(start: Long, end: Long) extends DataRange {
  def toTimePoints: Array[Long] = {
    Stream
      .iterate(TsUtils.round(start, Constants.Partition.spanMillis))(_ + Constants.Partition.spanMillis)
      .takeWhile(_ <= end)
      .toArray
  }

  def toPartitionRange: PartitionRange = {
    PartitionRange(Constants.Partition.of(start), Constants.Partition.of(end))
  }
}
// start and end can be null - signifies unbounded-ness
case class PartitionRange(start: String, end: String) extends DataRange {

  def valid: Boolean =
    (Option(start), Option(end)) match {
      case (Some(s), Some(e)) => s <= e
      case _                  => true
    }

  def intersect(other: PartitionRange): PartitionRange = {
    // lots of null handling
    val newStart = (Option(start) ++ Option(other.start))
      .reduceLeftOption(Ordering[String].max)
      .orNull
    val newEnd = (Option(end) ++ Option(other.end))
      .reduceLeftOption(Ordering[String].min)
      .orNull
    // could be invalid
    PartitionRange(newStart, newEnd)
  }

  override def toTimePoints: Array[Long] = {
    assert(start != null && end != null, "Can't request timePoint conversion when PartitionRange is unbounded")
    Stream
      .iterate(start)(Constants.Partition.after)
      .takeWhile(_ <= end)
      .map(Constants.Partition.epochMillis)
      .toArray
  }

  def whereClauses: Seq[String] = {
    val startClause = Option(start).map(s"${Constants.PartitionColumn} >= '" + _ + "'")
    val endClause = Option(end).map(s"${Constants.PartitionColumn} <= '" + _ + "'")
    (startClause ++ endClause).toSeq
  }

  def substituteMacros(template: String): String = {
    val substitutions = Seq(Constants.StartPartitionMacro -> Option(start), Constants.EndPartitionMacro -> Option(end))
    substitutions.foldLeft(template) {
      case (q, (target, Some(replacement))) => q.replaceAllLiterally(target, s"'$replacement'")
      case (q, (_, None))                   => q
    }
  }

  def genScanQuery(query: Query, table: String, includePartition: Boolean = false): String = {
    val queryOpt = Option(query)
    val wheres =
      whereClauses ++ queryOpt.flatMap(q => Option(q.wheres).map(_.asScala)).getOrElse(Seq.empty[String])
    val selects = queryOpt.map {
      query => {
        Option(
          includePartition match {
            case true => query.selects.asScala + (Constants.PartitionColumn -> Constants.PartitionColumn)
            case false => query.selects.asScala
          }
        ).map(_.toMap).orNull
      }
    }.orNull


    QueryUtils.build(selects = selects,
                     from = table,
                     wheres = wheres)
  }

  def steps(days: Int): Seq[PartitionRange] =
    partitions
      .sliding(days, days)
      .map { step => PartitionRange(step.head, step.last) }
      .toSeq

  def partitions: Seq[String] =
    Stream
      .iterate(start)(Constants.Partition.after)
      .takeWhile(_ <= end)

}
