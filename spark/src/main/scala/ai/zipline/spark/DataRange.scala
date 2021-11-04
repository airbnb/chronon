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
    PartitionRange(Constants.Partition.at(start), Constants.Partition.at(end))
  }

  def pretty: String = s"start:[${TsUtils.toStr(start)}]-end:[${TsUtils.toStr(end)}]"
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
      // add 1 day to the end times to include data [ds 00:00:00.000, ds + 1 00:00:00.000)
      .map(Constants.Partition.epochMillis(_) + Constants.Partition.spanMillis)
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

  def genScanQuery(query: Query, table: String, fillIfAbsent: Map[String, String] = Map.empty): String = {
    val queryOpt = Option(query)
    val wheres =
      whereClauses ++ queryOpt.flatMap(q => Option(q.wheres).map(_.asScala)).getOrElse(Seq.empty[String])
    QueryUtils.build(selects = queryOpt.map { query => Option(query.selects).map(_.asScala.toMap).orNull }.orNull,
                     from = table,
                     wheres = wheres,
                     fillIfAbsent = fillIfAbsent)
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

  def shift(days: Int): PartitionRange =
    PartitionRange(Constants.Partition.shift(start, days), Constants.Partition.shift(end, days))
}
