package ai.chronon.api

case class KeyMissingException(requestName: String, missingKeys: Seq[String])
    extends IllegalArgumentException(
      s"Metadata updated to look for keys (${missingKeys.mkString(",")}) but are missing in $requestName request")
