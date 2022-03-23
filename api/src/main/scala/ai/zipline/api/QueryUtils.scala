package ai.zipline.api

// utilized by both streaming and batch
object QueryUtils {
  // when the value in fillIfAbsent for a key is null, we expect the column with the same name as the key
  // to be present in the table that the generated query runs on.
  def build(selects: Map[String, String],
            from: String,
            wheres: Seq[String],
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

    val whereClause = Option(wheres)
      .filter(_.nonEmpty)
      .map { w =>
        s"""
           |WHERE
           |  ${w.mkString(" AND ")}""".stripMargin
      }
      .getOrElse("")

    s"""SELECT
       |  ${finalSelects.mkString(",\n  ")}
       |FROM $from $whereClause""".stripMargin
  }
}
