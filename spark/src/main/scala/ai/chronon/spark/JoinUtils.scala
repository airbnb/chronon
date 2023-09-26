package ai.chronon.spark

import ai.chronon.api.Constants
import ai.chronon.api.DataModel.Events
import ai.chronon.api.Extensions._
import ai.chronon.spark.Extensions._
import com.google.gson.Gson
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{coalesce, col, udf}

import scala.util.ScalaJavaConversions.MapOps

object JoinUtils {

  /***
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
    val scanQuery = range.genScanQuery(joinConf.left.query,
                                       joinConf.left.table,
                                       fillIfAbsent = Map(tableUtils.partitionColumn -> null) ++ timeProjection) +
      limit.map(num => s" LIMIT $num").getOrElse("")
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

  /***
    * Compute partition range to be filled for given join conf
    */
  def getRangesToFill(leftSource: ai.chronon.api.Source,
                      tableUtils: TableUtils,
                      endPartition: String,
                      overrideStartPartition: Option[String] = None,
                      historicalBackfill: Boolean = true
                     ): PartitionRange = {
    val overrideStart = if (historicalBackfill) {
      overrideStartPartition
    } else {
      println(s"Historical backfill is set to false. Backfill single partition only: $endPartition")
      Some(endPartition)
    }
    lazy val defaultLeftStart = Option(leftSource.query.startPartition)
      .getOrElse(tableUtils.firstAvailablePartition(leftSource.table, leftSource.subPartitionFilters).get)
    val leftStart = overrideStart.getOrElse(defaultLeftStart)
    val leftEnd = Option(leftSource.query.endPartition).getOrElse(endPartition)
    PartitionRange(leftStart, leftEnd)(tableUtils)
  }

  /***
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

  /***
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

  /***
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

  def filterColumns(df: DataFrame, filter: Seq[String]): DataFrame = {
    val columnsToDrop = df.columns
      .filterNot(col => filter.contains(col))
    df.drop(columnsToDrop: _*)
  }

  def tablesToRecompute(joinConf: ai.chronon.api.Join,
                        outputTable: String,
                        tableUtils: TableUtils): collection.Seq[String] = {
    val gson = new Gson()
    (for (
      props <- tableUtils.getTableProperties(outputTable);
      oldSemanticJson <- props.get(Constants.SemanticHashKey);
      oldSemanticHash = gson.fromJson(oldSemanticJson, classOf[java.util.HashMap[String, String]]).toScala
    ) yield {
      println(s"Comparing Hashes:\nNew: ${joinConf.semanticHash},\nOld: $oldSemanticHash")
      joinConf.tablesToDrop(oldSemanticHash)
    }).getOrElse(collection.Seq.empty)
  }
}
