package ai.chronon.aggregator.windowing

import java.util.{Date, TimeZone}
import org.apache.commons.lang3.time.FastDateFormat

object TsUtils {
  // changed from SimpleDateFormat to FastDateFormat for thread-safe code
  val formatter = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss", TimeZone.getTimeZone("UTC"))

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

  def round(epochMillis: Long, roundMillis: Long): Long =
    (epochMillis / roundMillis) * roundMillis
}
