package ai.chronon.api

// utilized by both streaming and batch
object QueryUtils {
  // when the value in fillIfAbsent for a key is null, we expect the column with the same name as the key
  // to be present in the table that the generated query runs on.
  def build(selects: Map[String, String],
            from: String,
            wheres: scala.collection.Seq[String],
            isLocalized: Boolean,
            fillIfAbsent: Map[String, String] = null): String = {

    def toProjections(m: Map[String, String]) =
      m.map {
        case (col, expr) =>
          if (expr == null) {
            s"`$col`"
          } else {
            s"$expr as `$col`"
          }
      }

    val finalSelects = (Option(selects), Option(fillIfAbsent)) match {
      case (Some(sels), Some(fills)) => toProjections(fills ++ sels)
      case (Some(sels), None)        => toProjections(sels)
      case (None, _)                 => Seq("*")
    }

    val updatedWheres = if (isLocalized) {
      wheres ++ Seq(s"(${Constants.LocalityZoneColumn} = 'DEFAULT' or ${Constants.LocalityZoneColumn} is null)")
    } else {
      wheres
    }

    val whereClause = Option(updatedWheres)
      .filter(_.nonEmpty)
      .map { ws =>
        s"""
           |WHERE
           |  ${ws.map(w => s"(${w})").mkString(" AND ")}""".stripMargin
      }
      .getOrElse("")

    s"""SELECT
       |  ${finalSelects.mkString(",\n  ")}
       |FROM $from$whereClause""".stripMargin
  }
}
