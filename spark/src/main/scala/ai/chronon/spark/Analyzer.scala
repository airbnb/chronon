package ai.chronon.spark

import ai.chronon.api
import ai.chronon.api.{AggregationPart, Constants, DataType}
import ai.chronon.api.Extensions._
import ai.chronon.online.SparkConversions
import ai.chronon.spark.Driver.parseConf
import com.yahoo.memory.Memory
import com.yahoo.sketches.ArrayOfStringsSerDe
import com.yahoo.sketches.frequencies.{ErrorType, ItemsSketch}
import org.apache.spark.sql.{DataFrame, types}
import org.apache.spark.sql.functions.{col, from_unixtime, lit}
import org.apache.spark.sql.types.StringType
import ai.chronon.aggregator.row.StatsGenerator

import scala.collection.Seq
import scala.collection.mutable.ListBuffer
import scala.util.ScalaJavaConversions.{IterableOps, ListOps}

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
               silenceMode: Boolean = false) {
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

  def toAggregationMetadata(aggPart: AggregationPart, columnType: DataType): AggregationMetadata = {
    AggregationMetadata(aggPart.outputColumnName,
                        columnType,
                        aggPart.operation.toString.toLowerCase,
                        aggPart.window.str.toLowerCase,
                        aggPart.inputColumn.toLowerCase)
  }

  def toAggregationMetadata(columnName: String, columnType: DataType): AggregationMetadata = {
    AggregationMetadata(columnName, columnType, "No operation", "Unbounded", columnName)
  }

  def analyzeGroupBy(groupByConf: api.GroupBy,
                     prefix: String = "",
                     includeOutputTableName: Boolean = false,
                     enableHitter: Boolean = false): (Array[AggregationMetadata], Map[String, DataType]) = {
    groupByConf.setups.foreach(tableUtils.sql)
    val groupBy = GroupBy.from(groupByConf, range, tableUtils, finalize = true)
    val name = "group_by/" + prefix + groupByConf.metaData.name
    println(s"""|Running GroupBy analysis for $name ...""".stripMargin)
    val analysis =
      if (enableHitter)
        analyze(groupBy.inputDf,
                groupByConf.keyColumns.toScala.toArray,
                groupByConf.sources.toScala.map(_.table).mkString(","))
      else ""
    val keySchema = groupBy.keySchema.fields.map { field => s"  ${field.name} => ${field.dataType}" }
    val schema = groupBy.outputSchema.fields.map { field => s"  ${field.name} => ${field.fieldType}" }
    if (silenceMode) {
      println(s"""ANALYSIS completed for group_by/${name}.""".stripMargin)
    } else {
      println(s"""
           |ANALYSIS for $name:
           |$analysis
               """.stripMargin)
      if (includeOutputTableName)
        println(s"""
             |----- OUTPUT TABLE NAME -----
             |${groupByConf.metaData.outputTable}
               """.stripMargin)
      println(s"""
           |----- KEY SCHEMA -----
           |${keySchema.mkString("\n")}
           |----- OUTPUT SCHEMA -----
           |${schema.mkString("\n")}
           |------ END --------------
           |""".stripMargin)
    }

    val aggMetadata = if (groupByConf.aggregations != null) {
      groupBy.aggPartWithSchema.map { entry => toAggregationMetadata(entry._1, entry._2) }.toArray
    } else {
      groupBy.outputSchema.map { tup => toAggregationMetadata(tup.name, tup.fieldType) }.toArray
    }
    val keySchemaMap = groupBy.keySchema.map {  field =>
      field.name -> SparkConversions.toChrononType(field.name, field.dataType)}.toMap
    (aggMetadata, keySchemaMap)
  }

  def analyzeJoin(joinConf: api.Join,
                  enableHitter: Boolean = false)
      : (Map[String, DataType], ListBuffer[AggregationMetadata], Map[String, DataType]) = {
    val name = "joins/" + joinConf.metaData.name
    println(s"""|Running join analysis for $name ...""".stripMargin)
    joinConf.setups.foreach(tableUtils.sql)
    val leftDf = JoinUtils.leftDf(joinConf, range, tableUtils, allowEmpty = true).get
    val analysis = if (enableHitter) analyze(leftDf, joinConf.leftKeyCols, joinConf.left.table) else ""
    val leftSchema: Map[String, DataType] =
      leftDf.schema.fields.map(field => (field.name, SparkConversions.toChrononType(field.name, field.dataType))).toMap

    val aggregationsMetadata = ListBuffer[AggregationMetadata]()
    val errorKeys = ListBuffer[String]()
    joinConf.joinParts.toScala.parallel.foreach { part =>
      val (aggMetadata, gbKeySchema) = analyzeGroupBy(part.groupBy,
        part.fullPrefix,
        true,
        enableHitter)
      synchronized {
        aggregationsMetadata ++= aggMetadata.map { aggMeta =>
          AggregationMetadata(part.fullPrefix + "_" + aggMeta.name,
            aggMeta.columnType,
            aggMeta.operation,
            aggMeta.window,
            aggMeta.inputColumn,
            part.getGroupBy.getMetaData.getName)
        }
      }
      // Run validation checks.
      // TODO: more validations on the way
      errorKeys ++= runSchemaValidation(leftSchema, gbKeySchema, joinConf.leftKeyCols)
    }

    val rightSchema: Map[String, DataType] =
      aggregationsMetadata.map(aggregation => (aggregation.name, aggregation.columnType)).toMap
    val statsSchema = StatsGenerator.statsIrSchema(api.StructType.from("Stats", rightSchema.toArray))
    if (silenceMode) {
      println(s"""ANALYSIS completed for join/${joinConf.metaData.cleanName}.""".stripMargin)
    } else {
      println(s"""
           |ANALYSIS for join/${joinConf.metaData.cleanName}:
           |$analysis
           |----- OUTPUT TABLE NAME -----
           |${joinConf.metaData.outputTable}
           |------ LEFT SIDE SCHEMA -------
           |${leftSchema.mkString("\n")}
           |------ RIGHT SIDE SCHEMA ----
           |${rightSchema.mkString("\n")}
           |------ STATS SCHEMA ---------
           |${statsSchema.unpack.toMap.mkString("\n")}
           |------ END ------------------
           |""".stripMargin)
    }

    println(s"""----- Validations for join/${joinConf.metaData.cleanName} -----""".stripMargin)
    if(errorKeys.toSet.isEmpty) {
      println(s"""----- Schema validation completed. No errors found. -----""".stripMargin)
    } else {
      println(
        s"""----- Schema validation completed. Found ${errorKeys.length} errors for following keys -
           | [${errorKeys.mkString(", ")}].
           | Please check VALIDATION ERROR message for details ----- """.stripMargin)
    }
    assert(errorKeys.toSet.isEmpty, s"ERROR: Schema validation failed. Please check error message for details.")

    // (schema map showing the names and datatypes, right side feature aggregations metadata for metadata upload)
    (leftSchema ++ rightSchema, aggregationsMetadata, statsSchema.unpack.toMap)
  }

  // validate the schema of the left and right side of the join and make sure the types match
  // return a list of keys that failed validation
  def runSchemaValidation(left: Map[String, DataType],
                          right: Map[String, DataType],
                          keys: Seq[String]): List[String] = {
    val errorKeys = ListBuffer[String]()
    keys.foreach { key =>
      val leftFields = left.keys.toSeq
      val rightFields = right.keys.toSeq
      if(!leftFields.contains(key) || !rightFields.contains(key)) {
        println(s"[VALIDATION ERROR]: Either left or right side of the join doesn't contain the key $key, " +
          s"available keys are [${leftFields.mkString(", ")}]")
        errorKeys += key
      } else {
        val leftDataType = left(key)
        val rightDataType = right(key)
        if (leftDataType != rightDataType) {
          println(s"[VALIDATION ERROR]: Join key, '$key', has mismatched data types - left type: $leftDataType vs. " +
            s"right type $rightDataType")
          errorKeys += key
        }
      }
    }
    errorKeys.toList
  }

  def run(): Unit =
    conf match {
      case confPath: String =>
        if (confPath.contains("/joins/")) {
          val joinConf = parseConf[api.Join](confPath)
          analyzeJoin(joinConf, enableHitter = enableHitter)
        } else if (confPath.contains("/group_bys/")) {
          val groupByConf = parseConf[api.GroupBy](confPath)
          analyzeGroupBy(groupByConf, enableHitter = enableHitter)
        }
      case groupByConf: api.GroupBy => analyzeGroupBy(groupByConf, enableHitter = enableHitter)
      case joinConf: api.Join       => analyzeJoin(joinConf, enableHitter = enableHitter)
    }
}
