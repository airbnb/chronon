package ai.chronon.api

case class KeyMissingException(requestName: String, missingKeys: Seq[String])
    extends IllegalArgumentException(s"Missing required keys (${missingKeys.mkString(",")}) for $requestName")