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

import org.slf4j.LoggerFactory
import ai.chronon.api
import ai.chronon.api.{Accuracy, AggregationPart, Constants, DataType, TimeUnit, Window}
import ai.chronon.api.Extensions._
import ai.chronon.online.SparkConversions
import ai.chronon.spark.Driver.parseConf
import com.yahoo.memory.Memory
import com.yahoo.sketches.ArrayOfStringsSerDe
import com.yahoo.sketches.frequencies.{ErrorType, ItemsSketch}
import org.apache.spark.sql.{Column, DataFrame, Row, types}
import org.apache.spark.sql.functions.{col, from_unixtime, lit, sum, when}
import org.apache.spark.sql.types.{StringType, StructType}
import ai.chronon.api.DataModel.{DataModel, Entities, Events}

import scala.collection.{Seq, immutable, mutable}
import scala.collection.mutable.ListBuffer
import scala.util.ScalaJavaConversions.{ListOps, MapOps}

//@SerialVersionUID(3457890987L)
//class ItemSketchSerializable(var mapSize: Int) extends ItemsSketch[String](mapSize) with Serializable {}

class ItemSketchSerializable extends Serializable {
  var sketch: ItemsSketch[String] = null
  def init(mapSize: Int): ItemSketchSerializable = {
    sketch = new ItemsSketch[String](mapSize)
    this
  }

  // necessary for serialization
  private def writeObject(out: java.io.ObjectOutputStream): Unit = {
    val serDe = new ArrayOfStringsSerDe
    val bytes = sketch.toByteArray(serDe)
    out.writeInt(bytes.size)
    out.writeBytes(new String(bytes))
  }

  private def readObject(input: java.io.ObjectInputStream): Unit = {
    val size = input.readInt()
    val bytes = new Array[Byte](size)
    input.read(bytes)
    val serDe = new ArrayOfStringsSerDe
    sketch = ItemsSketch.getInstance[String](Memory.wrap(bytes), serDe)
  }
}

class Analyzer(tableUtils: TableUtils,
               conf: Any,
               startDate: String,
               endDate: String,
               count: Int = 64,
               sample: Double = 0.1,
               enableHitter: Boolean = false,
               silenceMode: Boolean = false,
               skipTimestampCheck: Boolean = false) {
  @transient lazy val logger = LoggerFactory.getLogger(getClass)
  // include ts into heavy hitter analysis - useful to surface timestamps that have wrong units
  // include total approx row count - so it is easy to understand the percentage of skewed data
  def heavyHittersWithTsAndCount(df: DataFrame,
                                 keys: Array[String],
                                 frequentItemMapSize: Int = 1024,
                                 sampleFraction: Double = 0.1): Array[(String, Array[(String, Long)])] = {
    val baseDf = df.withColumn("total_count", lit("rows"))
    val baseKeys = keys :+ "total_count"
    if (df.schema.fieldNames.contains(Constants.TimeColumn)) {
      heavyHitters(baseDf.withColumn("ts_year", from_unixtime(col("ts") / 1000, "yyyy")),
                   baseKeys :+ "ts_year",
                   frequentItemMapSize,
                   sampleFraction)
    } else {
      heavyHitters(baseDf, baseKeys, frequentItemMapSize, sampleFraction)
    }
  }

  // Uses a variant Misra-Gries heavy hitter algorithm from Data Sketches to find topK most frequent items in data
  // frame. The result is a Array of tuples of (column names, array of tuples of (heavy hitter keys, counts))
  // [(keyCol1, [(key1: count1) ...]), (keyCol2, [...]), ....]
  def heavyHitters(df: DataFrame,
                   frequentItemKeys: Array[String],
                   frequentItemMapSize: Int = 1024,
                   sampleFraction: Double = 0.1): Array[(String, Array[(String, Long)])] = {
    assert(frequentItemKeys.nonEmpty, "No column arrays specified for frequent items summary")
    // convert all keys into string
    val stringifiedCols = frequentItemKeys.map { col =>
      val stringified = df.schema.fields.find(_.name == col) match {
        case Some(types.StructField(name, StringType, _, _)) => name
        case Some(types.StructField(name, _, _, _))          => s"CAST($name AS STRING)"
        case None =>
          throw new IllegalArgumentException(s"$col is not present among: [${df.schema.fieldNames.mkString(", ")}]")
      }
      s"COALESCE($stringified, 'NULL')"
    }

    val colsLength = stringifiedCols.length
    val init = Array.fill(colsLength)((new ItemSketchSerializable).init(frequentItemMapSize))
    val freqMaps = df
      .selectExpr(stringifiedCols: _*)
      .sample(sampleFraction)
      .rdd
      .treeAggregate(init)(
        seqOp = {
          case (sketches, row) =>
            var i = 0
            while (i < colsLength) {
              sketches(i).sketch.update(row.getString(i))
              i += 1
            }
            sketches
        },
        combOp = {
          case (sketches1, sketches2) =>
            var i = 0
            while (i < colsLength) {
              sketches1(i).sketch.merge(sketches2(i).sketch)
              i += 1
            }
            sketches1
        }
      )
      .map(_.sketch.getFrequentItems(ErrorType.NO_FALSE_POSITIVES))
      .map(_.map(sketchRow => sketchRow.getItem -> (sketchRow.getEstimate.toDouble / sampleFraction).toLong).toArray)
    frequentItemKeys.zip(freqMaps)
  }

  private val range = PartitionRange(startDate, endDate)(tableUtils)
  // returns with heavy hitter analysis for the specified keys
  def analyze(df: DataFrame, keys: Array[String], sourceTable: String): String = {
    val result = heavyHittersWithTsAndCount(df, keys, count, sample)
    val header = s"Analyzing heavy-hitters from table $sourceTable over columns: [${keys.mkString(", ")}]"
    val colPrints = result.flatMap {
      case (col, heavyHitters) =>
        Seq(s"  $col") ++ heavyHitters.map { case (name, count) => s"    $name: $count" }
    }
    (header +: colPrints).mkString("\n")
  }

  // Rich version of structType which includes additional info for a groupBy feature schema
  case class AggregationMetadata(name: String,
                                 columnType: DataType,
                                 operation: String = null,
                                 window: String = null,
                                 inputColumn: String = null,
                                 groupByName: String = null) {

    def asMap: Map[String, String] = {
      Map(
        "name" -> name,
        "window" -> window,
        "columnType" -> DataType.toString(columnType),
        "inputColumn" -> inputColumn,
        "operation" -> operation,
        "groupBy" -> groupByName
      )
    }

  }

  private def toAggregationMetadata(aggPart: AggregationPart, columnType: DataType): AggregationMetadata = {
    AggregationMetadata(aggPart.outputColumnName,
                        columnType,
                        aggPart.operation.toString.toLowerCase,
                        aggPart.window.str.toLowerCase,
                        aggPart.inputColumn.toLowerCase)
  }

  private def toAggregationMetadata(columnName: String,
                                    columnType: DataType,
                                    hasDerivation: Boolean = false): AggregationMetadata = {
    val operation = if (hasDerivation) "Derivation" else "No Operation"
    AggregationMetadata(columnName, columnType, operation, "Unbounded", columnName)
  }

  def analyzeGroupBy(
      groupByConf: api.GroupBy,
      prefix: String = "",
      includeOutputTableName: Boolean = false,
      enableHitter: Boolean = false,
      skipTimestampCheck: Boolean = skipTimestampCheck,
      validateTablePermission: Boolean = false): (Array[AggregationMetadata], Map[String, DataType], Set[String]) = {
    groupByConf.setups.foreach(tableUtils.sql)
    val groupBy = GroupBy.from(groupByConf, range, tableUtils, computeDependency = enableHitter, finalize = true)
    val name = "group_by/" + prefix + groupByConf.metaData.name
    logger.info(s"""|Running GroupBy analysis for $name ...""".stripMargin)

    val noAccessTables = if (validateTablePermission) {
      groupByConf.sources.toScala.flatMap { s =>
        runTablePermissionValidation(Set(s.table), s.partitionColumnOpt)
      }.toSet
    } else Set.empty[String]

    if (!skipTimestampCheck) {
      if (noAccessTables.isEmpty) {
        val timestampChecks = runTimestampChecks(groupBy.inputDf)
        validateTimestampChecks(timestampChecks, "GroupBy", name)
      } else {
        logger.warn(s"skipping the timestamp checks due to permission errors in ${noAccessTables.size} tables")
      }
    }

    val analysis =
      if (enableHitter)
        analyze(groupBy.inputDf,
                groupByConf.keyColumns.toScala.toArray,
                groupByConf.sources.toScala.map(_.table).mkString(","))
      else ""
    val schema = if (groupByConf.isSetBackfillStartDate && groupByConf.hasDerivations) {
      // handle group by backfill mode for derivations
      // todo: add the similar logic to join derivations
      val keyAndPartitionFields =
        groupBy.keySchema.fields ++ Seq(
          org.apache.spark.sql.types
            .StructField(tableUtils.getPartitionColumn(groupByConf.getPartitionColumn), StringType))
      val sparkSchema = {
        StructType(SparkConversions.fromChrononSchema(groupBy.outputSchema).fields ++ keyAndPartitionFields)
      }
      val dummyOutputDf = tableUtils.sparkSession
        .createDataFrame(tableUtils.sparkSession.sparkContext.parallelize(immutable.Seq[Row]()), sparkSchema)
      val finalOutputColumns = groupByConf.derivationsScala.finalOutputColumn(dummyOutputDf.columns).toSeq
      val derivedDummyOutputDf = dummyOutputDf.select(finalOutputColumns: _*)
      val columns = SparkConversions.toChrononSchema(
        StructType(derivedDummyOutputDf.schema.filterNot(keyAndPartitionFields.contains)))
      api.StructType("", columns.map(tup => api.StructField(tup._1, tup._2)))
    } else {
      groupBy.outputSchema
    }
    if (silenceMode) {
      logger.info(s"""ANALYSIS completed for group_by/${name}.""".stripMargin)
    } else {
      logger.info(s"""
           |ANALYSIS for $name:
           |$analysis
               """.stripMargin)
      if (includeOutputTableName)
        logger.info(s"""
             |----- OUTPUT TABLE NAME -----
             |${groupByConf.metaData.outputTable}
               """.stripMargin)
      val keySchema = groupBy.keySchema.fields.map { field => s"  ${field.name} => ${field.dataType}" }
      schema.fields.map { field => s"  ${field.name} => ${field.fieldType}" }
      logger.info(s"""
           |----- KEY SCHEMA -----
           |${keySchema.mkString("\n")}
           |----- OUTPUT SCHEMA -----
           |${schema.mkString("\n")}
           |------ END --------------
           |""".stripMargin)
    }

    val aggMetadata = if (groupByConf.hasDerivations || groupByConf.aggregations == null) {
      schema.map { tup => toAggregationMetadata(tup.name, tup.fieldType, groupByConf.hasDerivations) }.toArray
    } else {
      groupBy.aggPartWithSchema.map { entry => toAggregationMetadata(entry._1, entry._2) }.toArray
    }

    val keySchemaMap = groupBy.keySchema.map { field =>
      field.name -> SparkConversions.toChrononType(field.name, field.dataType)
    }.toMap
    (aggMetadata, keySchemaMap, noAccessTables)

  }

  def analyzeJoin(
      joinConf: api.Join,
      enableHitter: Boolean = false,
      validateTablePermission: Boolean = true,
      validationAssert: Boolean = false,
      skipTimestampCheck: Boolean = skipTimestampCheck): (Map[String, DataType], ListBuffer[AggregationMetadata]) = {
    val name = "joins/" + joinConf.metaData.name
    logger.info(s"""|Running join analysis for $name ...""".stripMargin)
    // run SQL environment setups such as UDFs and JARs
    joinConf.setups.foreach(tableUtils.sql)

    val (analysis, leftDf) = if (enableHitter) {
      val leftDf = JoinUtils.leftDf(joinConf, range, tableUtils, allowEmpty = true).get
      val analysis = analyze(leftDf, joinConf.leftKeyCols, joinConf.left.table)
      (analysis, leftDf)
    } else {
      val analysis = ""
      val leftQuery = joinConf.left.query
      val partitionColumn = tableUtils.getPartitionColumn(leftQuery)
      val leftDf: DataFrame = range.scanQueryDf(leftQuery,
                                                joinConf.left.table,
                                                fillIfAbsent = Map(partitionColumn -> null),
                                                partitionColOpt = Some(partitionColumn),
                                                renamePartitionCol = true)
      (analysis, leftDf)
    }

    val leftNoAccessTables = if (validateTablePermission) {
      runTablePermissionValidation(Set(joinConf.left.table), joinConf.left.partitionColumnOpt)
    } else Set.empty

    if (!skipTimestampCheck) {
      if (leftNoAccessTables.isEmpty) {
        val timestampChecks = runTimestampChecks(leftDf)
        validateTimestampChecks(timestampChecks, "Join", name)
      } else {
        logger.warn(s"skipping the timestamp checks due to permission errors in ${leftNoAccessTables.size} tables")
      }
    }

    val leftSchema = leftDf.schema.fields
      .map(field => (field.name, SparkConversions.toChrononType(field.name, field.dataType)))
      .toMap
    val joinIntermediateValuesMetadata = ListBuffer[AggregationMetadata]()
    val keysWithError: ListBuffer[(String, String)] = ListBuffer.empty[(String, String)]
    val gbStartPartitions = mutable.Map[String, List[String]]()
    val noAccessTables = mutable.Set[String]() ++= leftNoAccessTables
    // Pair of (table name, group_by name, expected_start) which indicate that the table no not have data available for the required group_by
    val dataAvailabilityErrors: ListBuffer[(String, String, String)] = ListBuffer.empty[(String, String, String)]

    val rangeToFill =
      JoinUtils.getRangesToFill(joinConf.left, tableUtils, endDate, historicalBackfill = joinConf.historicalBackfill)
    logger.info(s"Join range to fill $rangeToFill")
    val unfilledRanges = tableUtils
      .unfilledRanges(joinConf.metaData.outputTable,
                      rangeToFill,
                      Some(Seq(joinConf.left.table)),
                      inputTableToPartitionColumnsMap = joinConf.left.tableToPartitionColumn)
      .getOrElse(Seq.empty)

    joinConf.joinParts.toScala.foreach { part =>
      val (aggMetadata, gbKeySchema, rightNoAccessTables) =
        analyzeGroupBy(
          part.groupBy,
          part.fullPrefix,
          includeOutputTableName = true,
          enableHitter = enableHitter,
          skipTimestampCheck = skipTimestampCheck || leftNoAccessTables.nonEmpty,
          validateTablePermission = true
        )
      joinIntermediateValuesMetadata ++= aggMetadata.map { aggMeta =>
        AggregationMetadata(part.fullPrefix + "_" + aggMeta.name,
                            aggMeta.columnType,
                            aggMeta.operation,
                            aggMeta.window,
                            aggMeta.inputColumn,
                            part.getGroupBy.getMetaData.getName)
      }
      // Run validation checks.
      keysWithError ++= runSchemaValidation(leftSchema, gbKeySchema, part.rightToLeft)
      dataAvailabilityErrors ++= runDataAvailabilityCheck(joinConf.left.dataModel, part.groupBy, unfilledRanges)
      noAccessTables ++= rightNoAccessTables
      // list any startPartition dates for conflict checks
      val gbStartPartition = part.groupBy.sources.toScala
        .map(_.query.startPartition)
        .filter(_ != null)
      if (gbStartPartition.nonEmpty)
        gbStartPartitions += (part.groupBy.metaData.name -> gbStartPartition)
    }
    if (joinConf.onlineExternalParts != null) {
      joinConf.onlineExternalParts.toScala.foreach { part =>
        joinIntermediateValuesMetadata ++= part.source.valueFields.map { field =>
          AggregationMetadata(part.fullName + "_" + field.name,
                              field.fieldType,
                              null,
                              null,
                              field.name,
                              part.source.valueSchema.name)
        }
      }
    }

    val rightSchema: Map[String, DataType] =
      joinIntermediateValuesMetadata.map(aggregation => (aggregation.name, aggregation.columnType)).toMap
    if (silenceMode) {
      logger.info(s"""ANALYSIS completed for join/${joinConf.metaData.cleanName}.""".stripMargin)
    } else {
      logger.info(s"""
           |ANALYSIS for join/${joinConf.metaData.cleanName}:
           |$analysis
           |----- OUTPUT TABLE NAME -----
           |${joinConf.metaData.outputTable}
           |------ LEFT SIDE SCHEMA -------
           |${leftSchema.mkString("\n")}
           |------ RIGHT SIDE SCHEMA ----
           |${rightSchema.mkString("\n")}
           |------ END ------------------
           |""".stripMargin)
    }

    logger.info(s"----- Validations for join/${joinConf.metaData.cleanName} -----")
    if (gbStartPartitions.nonEmpty) {
      logger.info(
        "----- Following Group_Bys contains a startPartition. Please check if any startPartition will conflict with your backfill. -----")
      gbStartPartitions.foreach {
        case (gbName, startPartitions) =>
          logger.info(s"$gbName : ${startPartitions.mkString(",")}")
      }
    }
    if (keysWithError.isEmpty && noAccessTables.isEmpty && dataAvailabilityErrors.isEmpty) {
      logger.info("----- Backfill validation completed. No errors found. -----")
    } else {
      logger.info(s"----- Schema validation completed. Found ${keysWithError.size} errors")
      val keyErrorSet: Set[(String, String)] = keysWithError.toSet
      logger.info(keyErrorSet.map { case (key, errorMsg) => s"$key => $errorMsg" }.mkString("\n"))
      logger.info(
        s"---- Table permission check completed. Found permission errors in ${noAccessTables.size} tables ----")
      logger.info(noAccessTables.mkString("\n"))
      logger.info(s"---- Data availability check completed. Found issue in ${dataAvailabilityErrors.size} tables ----")
      dataAvailabilityErrors.foreach(error =>
        logger.info(s"Group_By ${error._2} : Source Tables ${error._1} : Expected start ${error._3}"))
    }

    if (validationAssert) {
      if (joinConf.isSetBootstrapParts) {
        // For joins with bootstrap_parts, do not assert on data availability errors, as bootstrap can cover them
        // Only print out the errors as a warning
        assert(
          keysWithError.isEmpty && noAccessTables.isEmpty,
          "ERROR: Join validation failed. Please check error message for details."
        )
      } else {
        assert(
          keysWithError.isEmpty && noAccessTables.isEmpty && dataAvailabilityErrors.isEmpty,
          "ERROR: Join validation failed. Please check error message for details."
        )
      }
    }
    // Derive the join online fetching output schema with metadata
    val joinOutputValuesMetadata: ListBuffer[AggregationMetadata] = if (joinConf.hasDerivations) {
      val keyColumns: List[String] = joinConf.joinParts.toScala
        .flatMap(joinPart => {
          val keyCols: Seq[String] = joinPart.groupBy.keyColumns.toScala
          if (joinPart.keyMapping == null) keyCols
          else {
            keyCols.map(key => {
              if (joinPart.rightToLeft.contains(key)) joinPart.rightToLeft(key)
              else key
            })
          }
        })
        .distinct
      val tsDsSchema: Map[String, DataType] = {
        Map("ts" -> api.StringType, "ds" -> api.StringType)
      }
      val sparkSchema = {
        val keySchema = leftSchema.filter(tup => keyColumns.contains(tup._1))
        val schema: Seq[(String, DataType)] = keySchema.toSeq ++ rightSchema.toSeq ++ tsDsSchema
        StructType(SparkConversions.fromChrononSchema(schema))
      }
      val dummyOutputDf = tableUtils.sparkSession
        .createDataFrame(tableUtils.sparkSession.sparkContext.parallelize(immutable.Seq[Row]()), sparkSchema)
      val finalOutputColumns: Array[Column] =
        joinConf.derivationsScala.finalOutputColumn(rightSchema.toArray.map(_._1)).toArray
      val derivedDummyOutputDf = dummyOutputDf.select(finalOutputColumns: _*)
      val columns = SparkConversions.toChrononSchema(
        StructType(derivedDummyOutputDf.schema.filterNot(tup => tsDsSchema.contains(tup.name))))
      ListBuffer(columns.map { tup => toAggregationMetadata(tup._1, tup._2, joinConf.hasDerivations) }: _*)
    } else {
      joinIntermediateValuesMetadata
    }
    // (schema map showing the names and datatypes, right side feature aggregations metadata for metadata upload)
    (leftSchema ++ rightSchema, joinOutputValuesMetadata)
  }

  // validate the schema of the left and right side of the join and make sure the types match
  // return a map of keys and corresponding error message that failed validation
  def runSchemaValidation(left: Map[String, DataType],
                          right: Map[String, DataType],
                          keyMapping: Map[String, String]): Map[String, String] = {
    keyMapping.flatMap {
      case (_, leftKey) if !left.contains(leftKey) =>
        Some(leftKey ->
          s"[ERROR]: Left side of the join doesn't contain the key $leftKey. Available keys are [${left.keys.mkString(",")}]")
      case (rightKey, _) if !right.contains(rightKey) =>
        Some(
          rightKey ->
            s"[ERROR]: Right side of the join doesn't contain the key $rightKey. Available keys are [${right.keys
              .mkString(",")}]")
      case (rightKey, leftKey) if left(leftKey) != right(rightKey) =>
        Some(
          leftKey ->
            s"[ERROR]: Join key, '$leftKey', has mismatched data types - left type: ${left(
              leftKey)} vs. right type ${right(rightKey)}")
      case _ => None
    }
  }

  // validate the table permissions for given list of tables
  // return a list of tables that the user doesn't have access to
  def runTablePermissionValidation(sources: Set[String], partitionColOpt: Option[String] = None): Set[String] = {
    logger.info(s"Validating ${sources.size} tables permissions ...")
    val today = tableUtils.partitionSpec.at(System.currentTimeMillis())
    //todo: handle offset-by-1 depending on temporal vs snapshot accuracy
    val partitionFilter = tableUtils.partitionSpec.minus(today, new Window(2, TimeUnit.DAYS))
    sources.filter { sourceTable =>
      !tableUtils.checkTablePermission(sourceTable, partitionFilter, partitionColOpt)
    }
  }

  // validate that data is available for the group by
  // - For aggregation case, gb table earliest partition should go back to (first_unfilled_partition - max_window) date
  // - For none aggregation case or unbounded window, no earliest partition is required
  // return a list of (table, gb_name, expected_start) that don't have data available
  private def runDataAvailabilityCheck(leftDataModel: DataModel,
                                       groupBy: api.GroupBy,
                                       unfilledRanges: Seq[PartitionRange]): List[(String, String, String)] = {
    if (unfilledRanges.isEmpty) {
      logger.info("No unfilled ranges found.")
      List.empty
    } else {
      val firstUnfilledPartition = unfilledRanges.min.start
      lazy val groupByOps = new GroupByOps(groupBy)
      lazy val leftShiftedPartitionRangeStart = unfilledRanges.min.shift(-1).start
      lazy val rightShiftedPartitionRangeStart = unfilledRanges.min.shift(1).start
      val maxWindow = groupByOps.maxWindow
      maxWindow match {
        case Some(window) =>
          val expectedStart = (leftDataModel, groupBy.dataModel, groupBy.inferredAccuracy) match {
            // based on the end of the day snapshot
            case (Entities, Events, _)   => tableUtils.partitionSpec.minus(rightShiftedPartitionRangeStart, window)
            case (Entities, Entities, _) => firstUnfilledPartition
            case (Events, Events, Accuracy.SNAPSHOT) =>
              tableUtils.partitionSpec.minus(leftShiftedPartitionRangeStart, window)
            case (Events, Events, Accuracy.TEMPORAL) =>
              tableUtils.partitionSpec.minus(firstUnfilledPartition, window)
            case (Events, Entities, Accuracy.SNAPSHOT) => leftShiftedPartitionRangeStart
            case (Events, Entities, Accuracy.TEMPORAL) =>
              tableUtils.partitionSpec.minus(leftShiftedPartitionRangeStart, window)
          }
          logger.info(
            s"Checking data availability for group_by ${groupBy.metaData.name} ... Expected start partition: $expectedStart")
          if (groupBy.sources.toScala.exists(s => s.isCumulative)) {
            List.empty
          } else {
            val tableToPartitions = groupBy.sources.toScala.map { source =>
              val table = source.table
              logger.info(s"Checking table $table for data availability ...")
              val partitions = tableUtils.partitions(table)
              val startOpt = if (partitions.isEmpty) None else Some(partitions.min)
              val endOpt = if (partitions.isEmpty) None else Some(partitions.max)
              (table, partitions, startOpt, endOpt)
            }
            val allPartitions = tableToPartitions.flatMap(_._2)
            val minPartition = if (allPartitions.isEmpty) None else Some(allPartitions.min)

            if (minPartition.isEmpty || minPartition.get > expectedStart) {
              logger.info(s"""
                         |Join needs data older than what is available for GroupBy: ${groupBy.metaData.name}
                         |left-$leftDataModel, right-${groupBy.dataModel}, accuracy-${groupBy.inferredAccuracy}
                         |expected earliest available data partition: $expectedStart""".stripMargin)
              tableToPartitions.foreach {
                case (table, _, startOpt, endOpt) =>
                  logger.info(
                    s"Table $table startPartition ${startOpt.getOrElse("empty")} endPartition ${endOpt.getOrElse("empty")}")
              }
              val tables = tableToPartitions.map(_._1)
              List((tables.mkString(", "), groupBy.metaData.name, expectedStart))
            } else {
              List.empty
            }
          }
        case None =>
          List.empty
      }
    }
  }

  // For groupBys validate if the timestamp provided produces some values
  // if all values are null this should be flagged as an error
  def runTimestampChecks(df: DataFrame, sampleNumber: Int = 100): Map[String, String] = {

    val hasTimestamp = df.schema.fieldNames.contains(Constants.TimeColumn)
    val mapTimestampChecks = if (hasTimestamp) {
      // set max sample to 100 rows if larger input is provided
      val sampleN = if (sampleNumber > 100) { 100 }
      else { sampleNumber }
      dataFrameToMap(
        df.limit(sampleN)
          .agg(
            // will return 0 if all values are null
            sum(when(col(Constants.TimeColumn).isNull, lit(0)).otherwise(lit(1)))
              .cast(StringType)
              .as("notNullCount"),
            // assumes that we have valid unix milliseconds between the date range of
            // 1971-01-01 00:00:00 (31536000000L) to 2099-12-31 23:59:59 (4102473599999L)
            // will return 0 if all values are within the range
            sum(when(col(Constants.TimeColumn).between(31536000000L, 4102473599999L), lit(0)).otherwise(lit(1)))
              .cast(StringType)
              .as("badRangeCount")
          )
          .select(col("notNullCount"), col("badRangeCount"))
      )
    } else {
      Map(
        "noTsColumn" -> "No Timestamp Column"
      )
    }
    mapTimestampChecks
  }

  /**
    * This method can be used to trigger the assertion checks
    * or print the summary stats once the timestamp checks have been run
    * @param timestampCheckMap
    * @param configType
    * @param configName
    */
  def validateTimestampChecks(timestampCheckMap: Map[String, String], configType: String, configName: String): Unit = {

    if (!timestampCheckMap.contains("noTsColumn")) {
      // do timestamp checks
      assert(
        timestampCheckMap("notNullCount") != "0",
        s"""[ERROR]: $configType validation failed.
           | Please check that source has non-null timestamps.
           | check notNullCount: ${timestampCheckMap("notNullCount")}
           | """.stripMargin
      )
      assert(
        timestampCheckMap("badRangeCount") == "0",
        s"""[ERROR]: $configType validation failed.
           | Please check that source has valid epoch millisecond timestamps.
           | badRangeCount: ${timestampCheckMap("badRangeCount")}
           | """.stripMargin
      )

      logger.info(s"""ANALYSIS TIMESTAMP completed for ${configName}.
                     |check notNullCount: ${timestampCheckMap("notNullCount")}
                     |check badRangeCount: ${timestampCheckMap("badRangeCount")}
                     |""".stripMargin)

    } else {
      logger.info(s"""ANALYSIS TIMESTAMP completed for ${configName}.
                     |check TsColumn: ${timestampCheckMap("noTsColumn")}
                     |""".stripMargin)
    }

  }

  def dataFrameToMap(inputDf: DataFrame): Map[String, String] = {
    val row: Row = inputDf.head()
    val schema = inputDf.schema
    val columns = schema.fieldNames
    val values = row.toSeq
    columns
      .zip(values)
      .map {
        case (column, value) =>
          (column, value.toString)
      }
      .toMap
  }

  def run(): Unit =
    conf match {
      case confPath: String =>
        if (confPath.contains("/joins/")) {
          val joinConf = parseConf[api.Join](confPath)
          analyzeJoin(joinConf, enableHitter = enableHitter, skipTimestampCheck = skipTimestampCheck)
        } else if (confPath.contains("/group_bys/")) {
          val groupByConf = parseConf[api.GroupBy](confPath)
          analyzeGroupBy(groupByConf, enableHitter = enableHitter, skipTimestampCheck = skipTimestampCheck)
        }
      case groupByConf: api.GroupBy =>
        analyzeGroupBy(groupByConf, enableHitter = enableHitter, skipTimestampCheck = skipTimestampCheck)
      case joinConf: api.Join =>
        analyzeJoin(joinConf, enableHitter = enableHitter, skipTimestampCheck = skipTimestampCheck)
    }
}
