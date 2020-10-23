package ai.zipline.spark

import org.apache.spark.sql.DataFrame

object Comparison {

  // Produces a "comparison" dataframe - given two dataframes that are supposed to have same data
  // The result contains the differing rows of the same key
  def sideBySide(a: DataFrame, b: DataFrame, keys: List[String]): DataFrame = {
    val prefixedExpectedDf = prefixColumnName(a, "a_", keys)
    val prefixedOutputDf = prefixColumnName(b, "b_", keys)
    val joined = prefixedExpectedDf.join(
      prefixedOutputDf,
      usingColumns = keys,
      joinType = "full"
    )

    var finalDf = joined
    val comparisonColumns =
      a.schema.fieldNames.toSet.diff(keys.toSet).toList.sorted
    finalDf = finalDf.filter(
      s"${comparisonColumns.map { col => s"a_$col <> b_$col" }.mkString(" or ")}"
    )
    val colOrder = keys ++ comparisonColumns.flatMap { col =>
      List(s"a_$col", s"b_$col")
    }
    finalDf = finalDf.select(colOrder.head, colOrder.tail: _*)
    finalDf
  }

  private def prefixColumnName(df: DataFrame,
                               prefix: String,
                               excludeColumns: List[String]): DataFrame = {
    val renamedColumns = df.columns.map(c => {
      if (excludeColumns.contains(c))
        df(c).as(c)
      else df(c).as(s"$prefix$c")
    })
    df.select(renamedColumns: _*)
  }
}
