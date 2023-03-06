package ai.chronon.api

case class KeyMissingException(requestName: String, missingKeys: Seq[String], query: Map[String, Any])
    extends IllegalArgumentException(s"Missing keys $missingKeys required by $requestName in query: $query")
