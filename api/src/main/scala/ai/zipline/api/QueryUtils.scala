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

  def build(selects: Seq[String], from: String, wheres: Seq[String], fillIfAbsent: Map[String, String]): String = {
    val finalSelects = addColumns(Option(selects).getOrElse(Seq("*")), fillIfAbsent)
    val whereClause = Option(wheres)
      .filter(_.nonEmpty)
      .map { w =>
        s"""WHERE
           |  $w
           |""".stripMargin
      }
      .getOrElse("")

    s"""
       |SELECT
       |  ${finalSelects.mkString("\n  ")}
       |FROM $from
       |$whereClause
       |""".stripMargin
  }
}
