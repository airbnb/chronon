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

import ai.chronon.api
import ai.chronon.api.Constants
import ai.chronon.api.DataModel.Events
import ai.chronon.api.Extensions.{JoinOps, _}
import ai.chronon.spark.Extensions._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{coalesce, col, udf}
import org.slf4j.LoggerFactory

import java.util
import scala.collection.compat._
import scala.jdk.CollectionConverters._

object JoinUtils {
  @transient lazy val logger = LoggerFactory.getLogger(getClass)

  /** *
    * Util methods for join computation
    */

  def leftDf(joinConf: ai.chronon.api.Join,
             range: PartitionRange,
             tableUtils: TableUtils,
             allowEmpty: Boolean = false,
             limit: Option[Int] = None): Option[DataFrame] = {
    val timeProjection = if (joinConf.left.dataModel == Events) {
      Seq(Constants.TimeColumn -> Option(joinConf.left.query).map(_.timeColumn).orNull)
    } else {
      Seq()
    }
    val query = joinConf.left.query
    val partitionColumn = tableUtils.getPartitionColumn(query)

    val (scanQuery, df) = range.scanQueryStringAndDf(
      query,
      joinConf.left.table,
      appendRawQueryString = limit.map(num => s" LIMIT $num").getOrElse(""),
      fillIfAbsent = Map(partitionColumn -> null) ++ timeProjection,
      partitionColOpt = Some(partitionColumn),
      renamePartitionCol = true
    )

    val skewFilter = joinConf.skewFilter()
    val result = skewFilter
      .map(sf => {
        logger.info(s"left skew filter: $sf")
        df.filter(sf)
      })
      .getOrElse(df)
    if (result.isEmpty) {
      logger.info(s"Left side query below produced 0 rows in range $range. Query:\n$scanQuery")
      if (!allowEmpty) {
        return None
      }
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

  /** *
    * Compute partition range to be filled for given join conf
    */
  def getRangesToFill(leftSource: ai.chronon.api.Source,
                      tableUtils: TableUtils,
                      endPartition: String,
                      overrideStartPartition: Option[String] = None,
                      historicalBackfill: Boolean = true): PartitionRange = {
    val overrideStart = if (historicalBackfill) {
      overrideStartPartition
    } else {
      logger.info(s"Historical backfill is set to false. Backfill latest single partition only: $endPartition")
      Some(endPartition)
    }
    lazy val defaultLeftStart = Option(leftSource.query.startPartition)
      .getOrElse(
        tableUtils
          .firstAvailablePartition(leftSource.table, leftSource.subPartitionFilters, leftSource.partitionColumnOpt)
          .get)
    val leftStart = overrideStart.getOrElse(defaultLeftStart)
    val leftEnd = Seq(Option(leftSource.query.endPartition), Some(endPartition)).flatten.min
    PartitionRange(leftStart, leftEnd)(tableUtils)
  }

  /** *
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

    val joinedDf = leftDf.join(rightDf, keys.toSeq, joinType)
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
    val finalDf = joinedDf.select(selects.toSeq: _*)
    finalDf
  }

  /** *
    * Method to create or replace a view for feature table joining with labels.
    * Label columns will be prefixed with "label" or custom prefix for easy identification
    */
  def createOrReplaceView(viewName: String,
                          leftTable: String,
                          rightTable: String,
                          joinKeys: Array[String],
                          tableUtils: TableUtils,
                          viewProperties: Map[String, String] = null,
                          labelColumnPrefix: String = Constants.LabelColumnPrefix): Unit = {
    val fieldDefinitions = joinKeys.map(field => s"l.`${field}`") ++
      tableUtils
        .getSchemaFromTable(leftTable)
        .filterNot(field => joinKeys.contains(field.name))
        .map(field => s"l.`${field.name}`") ++
      tableUtils
        .getSchemaFromTable(rightTable)
        .filterNot(field => joinKeys.contains(field.name))
        .map(field => {
          if (field.name.startsWith(labelColumnPrefix)) {
            s"r.`${field.name}`"
          } else {
            s"r.`${field.name}` AS `${labelColumnPrefix}_${field.name}`"
          }
        })
    val joinKeyDefinitions = joinKeys.map(key => s"l.`${key}` = r.`${key}`")
    val createFragment = s"""CREATE OR REPLACE VIEW $viewName"""
    val queryFragment =
      s"""
         |  AS SELECT
         |     ${fieldDefinitions.mkString(",\n    ")}
         |    FROM ${leftTable} AS l LEFT OUTER JOIN ${rightTable} AS r
         |      ON ${joinKeyDefinitions.mkString(" AND ")}""".stripMargin

    val propertiesFragment = if (viewProperties != null && viewProperties.nonEmpty) {
      s"""    TBLPROPERTIES (
         |    ${viewProperties.transform((k, v) => s"'$k'='$v'").values.mkString(",\n   ")}
         |    )""".stripMargin
    } else {
      ""
    }
    val sqlStatement = Seq(createFragment, propertiesFragment, queryFragment).mkString("\n")
    tableUtils.sql(sqlStatement)
  }

  /** *
    * Method to create a view with latest available label_ds for a given ds. This view is built
    * on top of final label view which has all label versions available.
    * This view will inherit the final label view properties as well.
    */
  def createLatestLabelView(viewName: String,
                            baseView: String,
                            tableUtils: TableUtils,
                            propertiesOverride: Map[String, String] = null): Unit = {
    val baseViewProperties = tableUtils.getTableProperties(baseView).getOrElse(Map.empty)
    val labelTableName = baseViewProperties.getOrElse(Constants.LabelViewPropertyKeyLabelTable, "")
    assert(labelTableName.nonEmpty, s"Not able to locate underlying label table for partitions")

    val labelMapping = getLatestLabelMapping(labelTableName, tableUtils)
    val caseDefinitions = labelMapping.flatMap(entry => {
      entry._2
        .map(v => s"WHEN " + v.betweenClauses + s" THEN ${Constants.LabelPartitionColumn} = '${entry._1}'")
        .toList
    })

    val createFragment = s"""CREATE OR REPLACE VIEW $viewName"""
    val queryFragment =
      s"""
         |  AS SELECT *
         |     FROM ${baseView}
         |     WHERE (
         |       CASE
         |         ${caseDefinitions.mkString("\n         ")}
         |         ELSE true
         |       END
         |     )
         | """.stripMargin

    val mergedProperties =
      if (propertiesOverride != null) baseViewProperties ++ propertiesOverride
      else baseViewProperties
    val propertiesFragment = if (mergedProperties.nonEmpty) {
      s"""TBLPROPERTIES (
         |    ${mergedProperties.transform((k, v) => s"'$k'='$v'").values.mkString(",\n   ")}
         |)""".stripMargin
    } else {
      ""
    }
    val sqlStatement = Seq(createFragment, propertiesFragment, queryFragment).mkString("\n")
    tableUtils.sql(sqlStatement)
  }

  /**
    * compute the mapping label_ds -> PartitionRange of ds which has this label_ds as latest version
    *  - Get all partitions from table
    *  - For each ds, find the latest available label_ds
    *  - Reverse the mapping and get the ds partition range for each label version(label_ds)
    *
    * @return Mapping of the label ds ->  partition ranges of ds which has this label available as latest
    */
  def getLatestLabelMapping(tableName: String, tableUtils: TableUtils): Map[String, collection.Seq[PartitionRange]] = {
    val partitions = tableUtils.allPartitions(tableName)
    assert(
      partitions.head.keys.equals(Set(tableUtils.partitionColumn, Constants.LabelPartitionColumn)),
      s""" Table must have label partition columns for latest label computation: `${tableUtils.partitionColumn}`
         | & `${Constants.LabelPartitionColumn}`
         |inputView: ${tableName}
         |""".stripMargin
    )

    val labelMap = collection.mutable.Map[String, String]()
    partitions.foreach(par => {
      val ds_value = par(tableUtils.partitionColumn)
      val label_value: String = par(Constants.LabelPartitionColumn)
      if (!labelMap.contains(ds_value)) {
        labelMap.put(ds_value, label_value)
      } else {
        labelMap.put(ds_value, Seq(labelMap(ds_value), label_value).max)
      }
    })

    labelMap.groupBy(_._2).map { case (v, kvs) => (v, tableUtils.chunk(kvs.keySet.toSet)) }
  }

  def injectKeyFilter(leftDf: DataFrame, originalJoinPart: api.JoinPart): api.JoinPart = {
    // make a copy of the original joinPart to avoid accumulating the key filters into the same object
    val joinPart = originalJoinPart.deepCopy()
    // Modifies the joinPart to inject the key filter into the where Clause of GroupBys by hardcoding the keyset
    val groupByKeyNames = joinPart.groupBy.getKeyColumns.asScala

    val collectedLeft = leftDf.collect()

    joinPart.groupBy.sources.asScala.foreach { source =>
      val selectMap = Option(source.rootQuery.getQuerySelects).getOrElse(Map.empty[String, String])
      val groupByKeyExpressions = groupByKeyNames.map { key =>
        key -> selectMap.getOrElse(key, key)
      }.toMap

      groupByKeyExpressions
        .map {
          case (keyName, groupByKeyExpression) =>
            val leftSideKeyName = joinPart.rightToLeft.get(keyName).get
            logger.info(
              s"KeyName: $keyName, leftSide KeyName: $leftSideKeyName , Join right to left: ${joinPart.rightToLeft
                .mkString(", ")}")
            val values = collectedLeft.map(row => row.getAs[Any](leftSideKeyName))
            // Check for null keys, warn if found
            val (notNullValues, nullValues) = values.partition(_ != null)
            if (notNullValues.isEmpty) {
              logger.warn(s"No not-null keys found for key: $keyName.")
            } else if (!nullValues.isEmpty) {
              logger.warn(s"Found ${nullValues.length} null keys for key: $keyName.")
            }

            // Escape single quotes in string values for spark sql
            def escapeSingleQuotes(s: String): String = s.replace("'", "\\'")

            // String manipulate to form valid SQL
            val valueSet = notNullValues.map {
              case s: String => s"'${escapeSingleQuotes(s)}'" // Add single quotes for string values
              case other     => other.toString // Keep other types (like Int) as they are
            }.toSet

            if (valueSet.isEmpty) {
              "false"
            } else {
              s"$groupByKeyExpression in (${valueSet.mkString(sep = ",")})"
            }
        }
        .foreach { whereClause =>
          // Skip adding the filter if it is a join source
          if (!source.isSetJoinSource) {
            val currentWheres = Option(source.rootQuery.getWheres).getOrElse(new util.ArrayList[String]())
            currentWheres.add(whereClause)
            source.rootQuery.setWheres(currentWheres)
          }
        }
    }
    joinPart
  }

  def filterColumns(df: DataFrame, filter: Seq[String]): DataFrame = {
    val columnsToDrop = df.columns
      .filterNot(col => filter.contains(col))
    df.drop(columnsToDrop: _*)
  }

}
