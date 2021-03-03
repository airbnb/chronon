package ai.zipline.spark

import org.apache.spark.sql.DataFrame

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
    finalDf = finalDf.filter(
      s"${comparisonColumns.map { col => s"a_$col <> b_$col" }.mkString(" or ")}"
    )
    val colOrder =
      keys.map(key => { finalDf(s"a_$key").as(key) }) ++
        comparisonColumns.flatMap { col =>
          List(finalDf(s"a_$col"), finalDf(s"b_$col"))
        }
    finalDf = finalDf.select(colOrder: _*)
    finalDf
  }

  private def prefixColumnName(df: DataFrame, prefix: String): DataFrame = {
    val renamedColumns = df.columns.map(c => { df(c).as(s"$prefix$c") })
    df.select(renamedColumns: _*)
  }
}
