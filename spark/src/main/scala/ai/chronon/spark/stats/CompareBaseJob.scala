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

package ai.chronon.spark.stats

import org.slf4j.LoggerFactory
import ai.chronon.api._
import ai.chronon.online.{SparkConversions, _}
import ai.chronon.spark.Extensions._
import ai.chronon.spark.{TableUtils, TimedKvRdd}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.DataType

import scala.collection.mutable.ListBuffer

object CompareBaseJob {
  @transient lazy val logger = LoggerFactory.getLogger(getClass)

  def checkConsistency(
      leftFields: Map[String, DataType],
      rightFields: Map[String, DataType],
      keys: Seq[String],
      tableUtils: TableUtils,
      mapping: Map[String, String] = Map.empty,
      migrationCheck: Boolean = false
  ): Unit = {
    val errors = ListBuffer[String]()
    // Make sure the number of fields are comparable on either side.
    // For migration checks or general comparisons, the left side can have more fields.
    val sizeCheck = if (migrationCheck) leftFields.size >= rightFields.size else leftFields.size == rightFields.size
    if (!sizeCheck) {
      errors += s"""Inconsistent number of fields; left side: ${leftFields.size}, right side: ${rightFields.size}
                |Left side fields:
                | - ${leftFields.toSeq.sortBy(_._1).mkString("\n - ")}
                |
                |Right side fields:
                | - ${rightFields.toSeq.sortBy(_._1).mkString("\n - ")}
                |""".stripMargin
    }

    // Verify that the mapping and the datatypes match
    val reverseMapping = mapping.map(_.swap)
    rightFields.foreach { rightField =>
      val leftFieldName = if (reverseMapping.contains(rightField._1)) {
        reverseMapping.get(rightField._1).get
      } else {
        rightField._1
      }
      if (leftFields.contains(leftFieldName)) {
        val leftFieldType = leftFields.get(leftFieldName).get
        if (rightField._2 != leftFieldType) {
          errors += s"Comparison data types do not match for column '${leftFieldName}';" +
            s" left side: ${leftFieldType}, right side: ${rightField._2}"
        }
      } else {
        errors += s"Mapping column on the left table is not present; column name: ${leftFieldName}"
      }
    }

    // Make sure the values of the mapping dict doesn't contain any duplicates
    if (mapping.size != reverseMapping.size) {
      errors += s"Mapping values contain duplicate values. Keys: ${mapping.keys}, Values: ${mapping.values}"
    }

    // Verify the mapping has unique keys and values and they are all present in the left and right data frames.
    if (!mapping.keySet.subsetOf(leftFields.keySet)) {
      errors += s"Invalid mapping provided missing fields; provided: ${mapping.keySet}," +
        s" expected to be subset of: ${leftFields.keySet}"
    }
    if (!mapping.values.toSet.subsetOf(rightFields.keySet)) {
      errors += s"Invalid mapping provided missing fields; provided: ${mapping.values.toSet}," +
        s" expected to be subset of: ${rightFields.keySet}"
    }

    // Make sure the key columns are present in both the frames.
    Seq(leftFields, rightFields).foreach { kset =>
      if (!keys.toSet.subsetOf(kset.keySet)) {
        errors += s"Some of the primary keys are missing in the source dataframe; provided: ${keys}," +
          s" expected to be subset of: ${kset.keySet}"
      }
    }

    // Make sure the passed keys has one of the time elements in it
    if (keys.intersect(Constants.ReservedColumns(tableUtils.partitionColumn)).length == 0) {
      errors += "Ensure that one of the key columns is a time column"
    }

    assert(errors.size == 0, errors.mkString("\n-----------------------------------------------------------------\n"))
  }

  /*
   * Navigate the dataframes and compare them and fetch statistics.
   */
  def compare(
      leftDf: DataFrame,
      rightDf: DataFrame,
      keys: Seq[String],
      tableUtils: TableUtils,
      mapping: Map[String, String] = Map.empty,
      migrationCheck: Boolean = false,
      name: String = "undefined"
  ): (DataFrame, TimedKvRdd, DataMetrics) = {
    // 1. Check for schema consistency issues
    val leftFields: Map[String, DataType] = leftDf.schema.fields.map(sb => (sb.name, sb.dataType)).toMap
    val rightFields: Map[String, DataType] = rightDf.schema.fields.map(sb => (sb.name, sb.dataType)).toMap
    checkConsistency(leftFields, rightFields, keys, tableUtils, mapping, migrationCheck)

    // 2. Prune the extra columns that we may have on the left side for migration use cases
    // so that the comparison becomes consistent across both sides.
    val prunedColumns = ListBuffer[String]()
    val prunedLeftDf = if (migrationCheck) {
      leftDf.schema.fieldNames.foldLeft(leftDf)((df, field) => {
        val rightFieldName = if (mapping.contains(field)) mapping.get(field).get else field
        if (!rightFields.contains(rightFieldName)) {
          prunedColumns += field
          df.drop(field)
        } else {
          df
        }
      })
    } else {
      leftDf
    }
    logger.info(s"Pruning fields from the left source for equivalent comparison - ${prunedColumns.mkString(",")}")

    // 3. Build comparison dataframe
    logger.info(s"""Join keys: ${keys.mkString(", ")}
        |Left Schema:
        |${prunedLeftDf.schema.pretty}
        |
        |Right Schema:
        |${rightDf.schema.pretty}
        |
        |""".stripMargin)

    // Rename the left data source columns with a suffix (except the keys) to reduce the ambiguity
    val renamedLeftDf = prunedLeftDf.schema.fieldNames.foldLeft(prunedLeftDf)((df, field) => {
      if (!keys.contains(field)) {
        df.withColumnRenamed(field, s"${field}${CompareMetrics.leftSuffix}")
      } else {
        df
      }
    })

    // 4.. Join both the dataframes based on the keys and the partition column
    renamedLeftDf.validateJoinKeys(rightDf, keys)
    val joinedDf = renamedLeftDf.join(rightDf, keys, "full")

    // Rename the right data source columns with a suffix (except the keys) to reduce the ambiguity
    val compareDf = rightDf.schema.fieldNames.foldLeft(joinedDf)((df, field) => {
      if (!keys.contains(field)) {
        df.withColumnRenamed(field, s"${field}${CompareMetrics.rightSuffix}")
      } else {
        df
      }
    })

    val leftChrononSchema = StructType("input",
                                       SparkConversions
                                         .toChrononSchema(prunedLeftDf.schema)
                                         .filterNot(tup => keys.contains(tup._1))
                                         .map(tup => StructField(tup._1, tup._2)))

    // 5. Run the consistency check
    val (metricsTimedKvRdd, metrics) = CompareMetrics.compute(leftChrononSchema.fields, compareDf, keys, name, mapping)

    // Return the data frame of the actual comparison table, metrics table and the metrics itself
    (compareDf, metricsTimedKvRdd, metrics)
  }
}
