package ai.zipline.api

import java.text.SimpleDateFormat
import java.time.format.DateTimeFormatter
import java.time.{Instant, ZoneOffset}
import java.util.{Locale, TimeZone}

import ai.zipline.api.Extensions._

case class PartitionSpec(format: String, spanMillis: Long) {
  private val partitionFormatter = DateTimeFormatter
    .ofPattern(format, Locale.US)
    .withZone(ZoneOffset.UTC)
  val sdf = new SimpleDateFormat(format)
  sdf.setTimeZone(TimeZone.getTimeZone("UTC"))

  def epochMillis(partition: String): Long = {
    sdf.parse(partition).getTime
  }

  def of(millis: Long): String = {
    // [ASSUMPTION] Event partitions are start inclusive end exclusive
    // For example, for daily partitions of event data:
    //   Event timestamp at midnight of the day before of the event is included
    //   Event timestamp at midnight of the day of partition is excluded
    val roundedMillis =
      math.ceil((millis + 1).toDouble / spanMillis.toDouble).toLong * spanMillis
    partitionFormatter.format(Instant.ofEpochMilli(roundedMillis))
  }

  def at(millis: Long): String = partitionFormatter.format(Instant.ofEpochMilli(millis))

  def before(s: String): String = {
    partitionFormatter.format(Instant.ofEpochMilli(epochMillis(s) - spanMillis))
  }

  def minus(s: String, window: Window): String = of(epochMillis(s) - window.millis)

  def after(s: String): String = {
    partitionFormatter.format(Instant.ofEpochMilli(epochMillis(s) + spanMillis))
  }

  def before(millis: Long): String = of(millis - spanMillis)
}
