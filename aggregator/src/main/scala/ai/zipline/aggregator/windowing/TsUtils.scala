package ai.zipline.aggregator.windowing

import java.text.SimpleDateFormat
import java.util.{Date, TimeZone}

object TsUtils {
  val formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
  formatter.setTimeZone(TimeZone.getTimeZone("UTC"))

  // in millisecond precision
  def toStr(epochMillis: Long): String = {
    if (epochMillis < 0) {
      "unbounded"
    } else {
      val date = new Date(epochMillis)
      formatter.format(date)
    }
  }

  def datetimeToTs(desiredTime: String): Long = {
    formatter.parse(desiredTime).getTime
  }

  def dsToTs(ds: String): Long = {
    val dsFormatter = new SimpleDateFormat("yyyy-MM-dd")
    dsFormatter.setTimeZone(TimeZone.getTimeZone("UTC"))
    dsFormatter.parse(ds).getTime
  }

  def round(epochMillis: Long, roundMillis: Long): Long =
    (epochMillis / roundMillis) * roundMillis
}
