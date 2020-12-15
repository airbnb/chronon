package ai.zipline.api

// utilized by both streaming and batch
object QueryUtils {

  def columnExists(selects: Seq[String], column: String): Boolean =
    selects.exists { exprRaw =>
      val expr = exprRaw.trim
      expr == column || expr == s"`$column`" ||
      expr.endsWith(s" $column") || expr.endsWith(s" `$column`")
    }

  def addColumns(selects: Seq[String], fillIfAbsent: Map[String, String]): Seq[String] =
    fillIfAbsent.foldLeft(selects) {
      case (result, (col, expr)) =>
        if (!columnExists(selects, col)) {
          val prefix = if (expr != null && expr.nonEmpty) s"$expr as " else ""
          result :+ s"$prefix`$col`"
        } else { selects }
    }

  // when the value in fillIfAbsent for a key is null, we expect the column with the same name as the key
  // to be present in the table that the generated query runs on.
  def build(selects: Seq[String],
            from: String,
            wheres: Seq[String],
            fillIfAbsent: Map[String, String] = Map.empty[String, String]): String = {
    val finalSelects = Option(selects).filter(_.nonEmpty).map(addColumns(_, fillIfAbsent)).getOrElse(Seq("*"))
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
