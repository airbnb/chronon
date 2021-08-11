package ai.zipline.api

import scala.collection.JavaConverters._

import ai.zipline.api.Extensions.{GroupByOps, SourceOps}


// utilized by both streaming and batch
object QueryUtils {
  // when the value in fillIfAbsent for a key is null, we expect the column with the same name as the key
  // to be present in the table that the generated query runs on.
  def build(selects: Map[String, String],
            from: String,
            wheres: Seq[String],
            fillIfAbsent: Map[String, String] = null,
            nonNullColumns: Seq[String] = Seq.empty,
            additionalWheres: Seq[String] = Seq.empty
  ): String = {

    val finalSelects = (Option(selects), Option(fillIfAbsent)) match {
      case (Some(sels), Some(fills)) => (fills ++ sels)
      case (Some(sels), None)        => sels
      case (None, _) => {
        assert(fillIfAbsent == null || fillIfAbsent.values.forall(_ == null),
               s"Please specify selects, when columns are being overriden is set")
        Map("*" -> null)
      }
    }
    val nonNullWheres = nonNullColumns.map(QueryUtils.generateNonNullFilterClause(finalSelects, _))
    val generatedWheres: Seq[String] = Option(wheres).map(_ ++ nonNullWheres).getOrElse(nonNullWheres) ++ additionalWheres

    s"""SELECT
       |  ${toProjections(finalSelects).mkString(",\n  ")}
       |FROM $from ${toWhereClause(generatedWheres ++ additionalWheres)}""".stripMargin
  }

  def buildStreamingQuery(
      groupBy: GroupBy,
      additionalFilterClauses: Seq[String] = Seq.empty
  ): String = {
    val streamingSource = groupBy.streamingSource.get
    val query = streamingSource.query
    val selects = Option(query.selects).map(_.asScala.toMap).orNull
    val keys = groupBy.getKeyColumns.asScala
    val timeColumn = Option(query.timeColumn).getOrElse(Constants.TimeColumn)
    val metaColumns: Map[String, String] =
      Option(timeColumn).map { case c => Map(Constants.TimeColumn -> c) }.getOrElse(Map.empty)
    QueryUtils.build(
      selects,
      Constants.StreamingInputTable,
      Option(query.wheres).map(_.asScala).orNull,
      metaColumns,
      additionalWheres = additionalFilterClauses,
      nonNullColumns = keys ++ Seq(Constants.TimeColumn)
    )
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

  private def generateNonNullFilterClause(
      selectClauses: Map[String, String],
      nonNullColumn: String
  ): String = {
    val colName = selectClauses.get(nonNullColumn)
    require(colName.isDefined, s"column $nonNullColumn is not present in select clauses $selectClauses")
    s"${colName.get} IS NOT NULL"
  }

}
