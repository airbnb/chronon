package ai.zipline.spark
import ai.zipline.aggregator.windowing.TsUtils
import ai.zipline.api.Config.Constants
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
}
