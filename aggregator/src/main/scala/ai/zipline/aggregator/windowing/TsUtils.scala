package ai.zipline.aggregator.windowing

import java.text.SimpleDateFormat
import java.util.{Date, TimeZone}

object TsUtils {
  val formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")
  formatter.setTimeZone(TimeZone.getTimeZone("UTC"))

  // in millisecond precision
  def toStr(epochMillis: Long): String = {
    val date = new Date(epochMillis)
    formatter.format(date)
  }

  def round(epochMillis: Long, roundMillis: Long): Long =
    (epochMillis / roundMillis) * roundMillis

  def start(epochMillis: Long, hopMillis: Long, windowMillis: Long): Long =
    round(epochMillis, hopMillis) - windowMillis
}
