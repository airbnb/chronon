package ai.chronon.api

case class KeyMissingException(requestName: String,
                               missingKeys: Seq[String],
                               query: Map[String, Any],
                               rightToLeft: Map[String, String])
    extends IllegalArgumentException(
      s"Missing keys required by $requestName, query: $query, rightOnlyContains: $rightToLeft, missing $missingKeys")
