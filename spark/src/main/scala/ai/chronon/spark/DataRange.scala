/*
 *    Copyright (C) 2023 The Chronon Authors.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package ai.chronon.spark

import ai.chronon.aggregator.windowing.TsUtils
import ai.chronon.api.Extensions.QueryOps
import ai.chronon.api.{Constants, Query, QueryUtils}
import org.apache.spark.sql.DataFrame

import scala.collection.JavaConverters._

sealed trait DataRange {
  def toTimePoints: Array[Long]
}
case class TimeRange(start: Long, end: Long)(implicit tableUtils: TableUtils) extends DataRange {
  def toTimePoints: Array[Long] = {
    Stream
      .iterate(TsUtils.round(start, tableUtils.partitionSpec.spanMillis))(_ + tableUtils.partitionSpec.spanMillis)
      .takeWhile(_ <= end)
      .toArray
  }

  def toPartitionRange: PartitionRange = {
    PartitionRange(tableUtils.partitionSpec.at(start), tableUtils.partitionSpec.at(end))
  }

  def pretty: String = s"start:[${TsUtils.toStr(start)}]-end:[${TsUtils.toStr(end)}]"
  override def toString(): String = s"[${TsUtils.toStr(start)}-${TsUtils.toStr(end)}]"
}
// start and end can be null - signifies unbounded-ness
case class PartitionRange(start: String, end: String)(implicit tableUtils: TableUtils)
    extends DataRange
    with Ordered[PartitionRange] {

  def valid: Boolean = {
    (Option(start), Option(end)) match {
      case (Some(s), Some(e)) => s <= e
      case _                  => true
    }
  }

  def isSingleDay: Boolean = {
    start == end
  }

  def intersect(other: PartitionRange): PartitionRange = {
    // lots of null handling
    val newStart = (Option(start) ++ Option(other.start))
      .reduceLeftOption(Ordering[String].max)
      .orNull
    val newEnd = (Option(end) ++ Option(other.end))
      .reduceLeftOption(Ordering[String].min)
      .orNull
    // could be invalid
    PartitionRange(newStart, newEnd)
  }

  override def toTimePoints: Array[Long] = {
    assert(start != null && end != null, "Can't request timePoint conversion when PartitionRange is unbounded")
    Stream
      .iterate(start)(tableUtils.partitionSpec.after)
      .takeWhile(_ <= end)
      .map(tableUtils.partitionSpec.epochMillis)
      .toArray
  }

  def whereClauses(partitionColumn: String = tableUtils.partitionColumn): Seq[String] = {
    val startClause = Option(start).map(s"${partitionColumn} >= '" + _ + "'")
    val endClause = Option(end).map(s"${partitionColumn} <= '" + _ + "'")
    (startClause ++ endClause).toSeq
  }

  def betweenClauses: String = {
    s"${tableUtils.partitionColumn} BETWEEN '" + start + "' AND '" + end + "'"
  }

  def substituteMacros(template: String): String = {
    val substitutions = Seq(Constants.StartPartitionMacro -> Option(start), Constants.EndPartitionMacro -> Option(end))
    substitutions.foldLeft(template) {
      case (q, (target, Some(replacement))) => q.replaceAllLiterally(target, s"'$replacement'")
      case (q, (_, None))                   => q
    }
  }

  /**
    * Executes a scan query on the specified table and returns the resulting DataFrame.
    *
    * @param query                the object containing select clauses and where conditions
    * @param table                the name of the table from which to scan data
    * @param appendRawQueryString an optional raw string to append to the generated query
    * @param fillIfAbsent         a map of column names to default values for filling in absent columns
    * @param partitionColOpt      an optional override for the partition column; if not provided, the default partition
    *                             column from [[tableUtils]] is used
    * @param renamePartitionCol   if true, the partition column in the resulting DataFrame will be renamed to
    *                             the standard column name; otherwise, the original column name is retained
    * @return Dataframe containing the result of executing the constructed scan query
    */
  def scanQueryDf(query: Query,
                  table: String,
                  appendRawQueryString: String = "",
                  fillIfAbsent: Map[String, String] = Map.empty,
                  partitionColOpt: Option[String] = None,
                  renamePartitionCol: Boolean = false): DataFrame = {
    val (_, df) =
      scanQueryStringAndDf(query, table, appendRawQueryString, fillIfAbsent, partitionColOpt, renamePartitionCol)
    df
  }

  /**
    * Identical to `scanQueryDf` except for exposing the query string for testing or logging.
    */
  def scanQueryStringAndDf(query: Query,
                           table: String,
                           appendRawQueryString: String = "",
                           fillIfAbsent: Map[String, String] = Map.empty,
                           partitionColOpt: Option[String] = None,
                           renamePartitionCol: Boolean = false): (String, DataFrame) = {
    val partitionColumn = tableUtils.getPartitionColumn(partitionColOpt)
    val genQ = genScanQuery(query, table, fillIfAbsent, partitionColumn) + appendRawQueryString
    if (renamePartitionCol) {
      (genQ, tableUtils.sqlWithDefaultPartitionColumn(genQ, partitionColumn))
    } else {
      (genQ, tableUtils.sql(genQ))
    }
  }

  private def genScanQuery(query: Query,
                           table: String,
                           fillIfAbsent: Map[String, String] = Map.empty,
                           partitionColumn: String): String = {
    // Because we allow query to specify custom partition column,
    // it is not safe to use this method independently without renaming the column
    // to the standard one. If you want to test gen query, use `scanQueryStringAndDf` instead.
    val queryOpt = Option(query)
    val partitionCol = queryOpt match {
      case Some(q) => Option(q.getPartitionColumn).getOrElse(partitionColumn)
      case None    => partitionColumn
    }
    val wheres =
      whereClauses(partitionCol) ++ queryOpt
        .flatMap(q => Option(q.wheres).map(_.asScala))
        .getOrElse(Seq.empty[String])
    QueryUtils.build(selects = queryOpt.map(_.getQuerySelects).orNull,
                     from = table,
                     wheres = wheres,
                     fillIfAbsent = fillIfAbsent)
  }

  def steps(days: Int): Seq[PartitionRange] = {
    partitions
      .sliding(days, days)
      .map { step => PartitionRange(step.head, step.last) }
      .toSeq
  }

  // no nulls in start or end and start <= end - used as a pre-check before the `partitions` function
  def wellDefined: Boolean = start != null && end != null && start <= end
  def partitions: Seq[String] = {
    assert(wellDefined, s"Invalid partition range ${this}")
    Stream
      .iterate(start)(tableUtils.partitionSpec.after)
      .takeWhile(_ <= end)
  }

  def shift(days: Int): PartitionRange = {
    if (days == 0) {
      this
    } else {
      PartitionRange(tableUtils.partitionSpec.shift(start, days), tableUtils.partitionSpec.shift(end, days))
    }
  }

  override def compare(that: PartitionRange): Int = {
    def compareDate(left: String, right: String): Int = {
      if (left == right) {
        0
      } else if (left == null) {
        -1
      } else if (right == null) {
        1
      } else {
        left compareTo right
      }
    }

    val compareStart = compareDate(this.start, that.start)
    if (compareStart != 0) {
      compareStart
    } else {
      compareDate(this.end, that.end)
    }
  }
  override def toString(): String = s"[$start...$end]"
}
