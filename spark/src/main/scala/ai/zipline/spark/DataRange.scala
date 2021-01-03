package ai.zipline.spark
import ai.zipline.aggregator.windowing.TsUtils
import ai.zipline.api.Config.{Constants, ScanQuery, Window}
import ai.zipline.api.QueryUtils
import ai.zipline.spark.Extensions._

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

  def intersect(other: PartitionRange): Option[PartitionRange] = {
    // lots of null handling
    val newStart = (Option(start) ++ Option(other.start))
      .reduceLeftOption(Ordering[String].max)
      .orNull
    val newEnd = (Option(end) ++ Option(other.end))
      .reduceLeftOption(Ordering[String].min)
      .orNull
    val result = PartitionRange(newStart, newEnd)
    if (result.valid) Some(result) else None
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

  def genScanQuery(table: String, scanQuery: ScanQuery): String = {
    val scanQueryOpt = Option(scanQuery)
    QueryUtils.build(selects = scanQueryOpt.map(_.selects).orNull,
                     from = table,
                     wheres = whereClauses ++ scanQueryOpt.map(_.wheres).getOrElse(Seq.empty))
  }

  def length: Int =
    Stream
      .iterate(start)(Constants.Partition.after)
      .takeWhile(_ <= end)
      .length

}
