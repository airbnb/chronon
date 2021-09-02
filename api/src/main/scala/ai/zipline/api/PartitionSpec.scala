package ai.zipline.api

import ai.zipline.api.Extensions._

import java.text.SimpleDateFormat
import java.time.format.DateTimeFormatter
import java.time.{Instant, ZoneOffset}
import java.util.{Locale, TimeZone}

case class PartitionSpec(format: String, spanMillis: Long) {
  private def partitionFormatter =
    DateTimeFormatter
      .ofPattern(format, Locale.US)
      .withZone(ZoneOffset.UTC)
  private def sdf = {
    val formatter = new SimpleDateFormat(format)
    formatter.setTimeZone(TimeZone.getTimeZone("UTC"))
    formatter
  }

  def epochMillis(partition: String): Long = {
    sdf.parse(partition).getTime
  }

  // what partition does this timestamp belong to (when dataModel is Events)
  def of(millis: Long): String = {
    // [ASSUMPTION] Event partitions are start inclusive end exclusive
    // For example, for daily partitions of event data:
    //   Event timestamp at midnight of the day before of the event is included
    //   Event timestamp at midnight of the day of partition is excluded
    val roundedMillis =
      math.ceil((millis + 1).toDouble / spanMillis.toDouble).toLong * spanMillis
    at(roundedMillis)
  }

  // what is the date portion of this timestamp
  def at(millis: Long): String = partitionFormatter.format(Instant.ofEpochMilli(millis))

  def before(s: String): String = shift(s, -1)

  def minus(s: String, window: Window): String = of(epochMillis(s) - window.millis)

  def after(s: String): String = shift(s, 1)

  def before(millis: Long): String = of(millis - spanMillis)

  def shift(date: String, days: Int): String =
    partitionFormatter.format(Instant.ofEpochMilli(epochMillis(date) + days * spanMillis))
}
