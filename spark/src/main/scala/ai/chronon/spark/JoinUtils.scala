package ai.chronon.spark

import ai.chronon.api.Constants
import ai.chronon.api.DataModel.Events
import ai.chronon.api.Extensions._
import ai.chronon.spark.Extensions._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{coalesce, col, udf}

object JoinUtils {

  /***
    * Util methods for join computation
    */

  def leftDf(joinConf: ai.chronon.api.Join, range: PartitionRange, tableUtils: TableUtils): Option[DataFrame] = {
    val timeProjection = if (joinConf.left.dataModel == Events) {
      Seq(Constants.TimeColumn -> Option(joinConf.left.query).map(_.timeColumn).orNull)
    } else {
      Seq()
    }
    val scanQuery = range.genScanQuery(joinConf.left.query,
                                       joinConf.left.table,
                                       fillIfAbsent = Map(Constants.PartitionColumn -> null) ++ timeProjection)

    val df = tableUtils.sql(scanQuery)
    val skewFilter = joinConf.skewFilter()
    val result = skewFilter
      .map(sf => {
        println(s"left skew filter: $sf")
        df.filter(sf)
      })
      .getOrElse(df)
    if (result.isEmpty) {
      println(s"Left side query below produced 0 rows in range $range. Query:\n$scanQuery")
      return None
    }
    Some(result)
  }

  val set_add: UserDefinedFunction =
    udf((set: Seq[String], item: String) => {
      if (set == null && item == null) {
        null
      } else if (set == null) {
        Seq(item)
      } else if (item == null) {
        set
      } else {
        (set :+ item).distinct
      }
    })

  // if either array or query is null or empty, return false
  // if query has an item that exists in array, return true; otherwise, return false
  val contains_any: UserDefinedFunction =
    udf((array: Seq[String], query: Seq[String]) => {
      if (query == null) {
        None
      } else if (array == null) {
        Some(false)
      } else {
        Some(query.exists(q => array.contains(q)))
      }
    })

  /*
   * join left and right dataframes, merging any shared columns if exists by the coalesce rule.
   * fails if there is any data type mismatch between shared columns.
   *
   * The order of output joined dataframe is:
   *   - all keys
   *   - all columns on left (incl. both shared and non-shared) in the original order of left
   *   - all columns on right that are NOT shared by left, in the original order of right
   */
  def coalescedJoin(leftDf: DataFrame, rightDf: DataFrame, keys: Seq[String], joinType: String = "left"): DataFrame = {
    leftDf.validateJoinKeys(rightDf, keys)
    val sharedColumns = rightDf.columns.intersect(leftDf.columns)
    sharedColumns.foreach { column =>
      val leftDataType = leftDf.schema(leftDf.schema.fieldIndex(column)).dataType
      val rightDataType = rightDf.schema(rightDf.schema.fieldIndex(column)).dataType
      assert(leftDataType == rightDataType,
             s"Column '$column' has mismatched data types - left type: $leftDataType vs. right type $rightDataType")
    }

    val joinedDf = leftDf.join(rightDf, keys, joinType)
    // find columns that exist both on left and right that are not keys and coalesce them
    val selects = keys.map(col) ++
      leftDf.columns.flatMap { colName =>
        if (keys.contains(colName)) {
          None
        } else if (sharedColumns.contains(colName)) {
          Some(coalesce(leftDf(colName), rightDf(colName)).as(colName))
        } else {
          Some(leftDf(colName))
        }
      } ++
      rightDf.columns.flatMap { colName =>
        if (sharedColumns.contains(colName)) {
          None // already selected previously
        } else {
          Some(rightDf(colName))
        }
      }
    val finalDf = joinedDf.select(selects: _*)
    finalDf
  }
}
