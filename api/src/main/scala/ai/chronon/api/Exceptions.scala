package ai.chronon.api

import java.io.{PrintWriter, StringWriter}

case class KeyMissingException(requestName: String, missingKeys: Seq[String])
    extends IllegalArgumentException(s"Missing required keys (${missingKeys.mkString(",")}) for $requestName") {
  lazy val trace: String = {
    val stringWriter = new StringWriter()
    val printWriter = new PrintWriter(stringWriter)
    printStackTrace(printWriter)
    val trace = stringWriter.toString
    trace
  }
}
