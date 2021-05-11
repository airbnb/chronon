package ai.zipline.spark

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.desc

object Comparison {

  // Produces a "comparison" dataframe - given two dataframes that are supposed to have same data
  // The result contains the differing rows of the same key
  def sideBySide(a: DataFrame, b: DataFrame, keys: List[String]): DataFrame = {
    val prefixedExpectedDf = prefixColumnName(a, "a_")
    val prefixedOutputDf = prefixColumnName(b, "b_")
    val joinExpr = keys
      .map(key => prefixedExpectedDf(s"a_$key") <=> prefixedOutputDf(s"b_$key"))
      .reduce((col1, col2) => col1.and(col2))
    val joined = prefixedExpectedDf.join(
      prefixedOutputDf,
      joinExpr,
      joinType = "full_outer"
    )

    var finalDf = joined
    val comparisonColumns =
      a.schema.fieldNames.toSet.diff(keys.toSet).toList.sorted
    val colOrder =
      keys.map(key => { finalDf(s"a_$key").as(key) }) ++
        comparisonColumns.flatMap { col =>
          List(finalDf(s"a_$col"), finalDf(s"b_$col"))
        }
    finalDf = finalDf.select(colOrder: _*)
    finalDf = finalDf.filter(
      s"${comparisonColumns
        .flatMap { col =>
          val left = s"a_$col"
          val right = s"b_$col"
          Seq(s"(($left IS NULL AND $right IS NOT NULL) OR ($right IS NULL AND $left IS NOT NULL) OR ($left <> $right))")
        }
        .mkString(" or ")}"
    )
    finalDf
  }

  private def prefixColumnName(df: DataFrame, prefix: String): DataFrame = {
    val renamedColumns = df.columns.map(c => { df(c).as(s"$prefix$c") })
    df.select(renamedColumns: _*)
  }
}
