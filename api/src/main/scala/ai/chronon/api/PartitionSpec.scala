package ai.chronon.api

import ai.chronon.api.Extensions._

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

  // what is the date portion of this timestamp
  def at(millis: Long): String = partitionFormatter.format(Instant.ofEpochMilli(millis))

  def before(s: String): String = shift(s, -1)

  def minus(s: String, window: Window): String = at(epochMillis(s) - window.millis)

  def after(s: String): String = shift(s, 1)

  def before(millis: Long): String = at(millis - spanMillis)

  def shift(date: String, days: Int): String =
    partitionFormatter.format(Instant.ofEpochMilli(epochMillis(date) + days * spanMillis))

  def now: String = at(System.currentTimeMillis())

  def shiftBackFromNow(days: Int) = shift(now, 0 - days)
}
