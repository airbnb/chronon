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
import ai.chronon.api.{Constants, Query, QueryUtils}

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

  def genScanQuery(query: Query,
                   table: String,
                   fillIfAbsent: Map[String, String] = Map.empty,
                   partitionColumn: String = tableUtils.partitionColumn): String = {
    val queryOpt = Option(query)
    val wheres =
      whereClauses(partitionColumn) ++ queryOpt
        .flatMap(q => Option(q.wheres).map(_.asScala))
        .getOrElse(Seq.empty[String])
    QueryUtils.build(selects = queryOpt.map { query => Option(query.selects).map(_.asScala.toMap).orNull }.orNull,
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
