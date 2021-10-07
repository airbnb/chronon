package ai.zipline.api


// utilized by both streaming and batch
object QueryUtils {
  // when the value in fillIfAbsent for a key is null, we expect the column with the same name as the key
  // to be present in the table that the generated query runs on.
  def build(selects: Map[String, String],
            from: String,
            wheres: Seq[String],
            fillIfAbsent: Map[String, String] = null,
            nonNullColumns: Seq[String] = Seq.empty): String = {

    val finalSelects = (Option(selects), Option(fillIfAbsent)) match {
      case (Some(sels), Some(fills)) => fills ++ sels
      case (Some(sels), None)        => sels
      case (None, _) => {
        assert(fillIfAbsent == null || fillIfAbsent.values.forall(_ == null),
          s"Please specify selects, when columns are being overriden is set")
        Map("*" -> null)
      }
    }

    val nonNullWheres: Seq[String] = nonNullColumns.flatMap {
      col =>
        if (Option(selects).isEmpty) None
        else {
          assert(finalSelects.contains(col),
            s"Specified Non-Null column $col does not exist in Selects: ${finalSelects.mkString(", ")}")
          Some(finalSelects(col))
        }
    }
    val generatedWheres: Seq[String] = Option(wheres).map(_ ++ nonNullWheres).getOrElse(nonNullWheres)

    s"""SELECT
       |  ${toProjections(finalSelects).mkString(",\n  ")}
       |FROM $from ${toWhereClause(generatedWheres)}""".stripMargin
  }

  private def toProjections(m: Map[String, String]) =
    m.map {
      case (col, expr) =>
        if (col == "*") col
        else if (expr == null) {
          s"`$col`"
        } else {
          s"$expr as `$col`"
        }
    }

  private def toWhereClause(wheres:Seq[String]): String = {
   Option(wheres)
      .filter(_.nonEmpty)
      .map { w =>
        s"""
           |WHERE
           |  ${w.mkString(" AND ")}""".stripMargin
      }
      .getOrElse("")
  }
}
